package part1;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import part2.Bucket;

public class Request implements Runnable {
  private final int SC_CREATED = 201;
  private String port;
  private Integer resortID;
  private String seasonID;
  private String dayID;
  private Integer meanNumberOfSkiLift;
  private int skiersPerThread;
  private int startTime;
  private int endTime;
  private ThreadLocalRandom rand;
  private int numOfSkierLift;
  private int kthThread;
  private CountDownLatch countDownLatch;
  private CountDownLatch countDownLatchTotal;
  private double rate;
  private AtomicInteger numOfSuccessfulReq;
  private AtomicInteger numOfReq;
  private ConcurrentLinkedQueue<String> queue;
  private ConcurrentHashMap<Long, Bucket> bucketMap;
  public Request(int kthThread, String port, Integer resortID, String seasonID, String dayID,
      int skiersPerThread, int meanNumberOfSkiLift, int startTime, int endTime, int numOfSkierLift,
      CountDownLatch countDownLatch, CountDownLatch countDownLatchTotal, double rate,
      AtomicInteger numOfSuccessfulReq, AtomicInteger numOfReq, ConcurrentLinkedQueue<String> queue,
      ConcurrentHashMap<Long, Bucket> bucketMap) {
    this.port = port;
    this.resortID = resortID;
    this.seasonID = seasonID;
    this.dayID = dayID;
    this.meanNumberOfSkiLift = meanNumberOfSkiLift;
    this.skiersPerThread = skiersPerThread;
    this.startTime = startTime;
    this.endTime = endTime;
    this.numOfSkierLift = numOfSkierLift;
    this.kthThread = kthThread;
    this.countDownLatch = countDownLatch;
    this.rate = rate;
    this.countDownLatchTotal = countDownLatchTotal;
    this.numOfSuccessfulReq = numOfSuccessfulReq;
    this.numOfReq = numOfReq;
    this.queue = queue;
    this.bucketMap = bucketMap;
  }

  @Override
  public void run() {
    String basePath = port;
    SkiersApi apiInstance = new SkiersApi();
    ApiClient client = apiInstance.getApiClient();
    client.setReadTimeout(600000);
    client.setConnectTimeout(600000);
    client.setBasePath(basePath);
    int numOfPosts = (int)(meanNumberOfSkiLift * rate * skiersPerThread);
    this.rand = ThreadLocalRandom.current();
    for (int i = 0; i < numOfPosts; i++) {
      try {
        LiftRide liftRide = new LiftRide();
        int time = startTime + rand.nextInt(endTime - startTime);
        liftRide.time(time);
        int liftID = rand.nextInt(numOfSkierLift) + 1;
        liftRide.liftID(liftID);
        int skierID = kthThread * skiersPerThread + rand.nextInt(skiersPerThread);
        long startTimeStamp = System.nanoTime();
        String tempDayId = String.valueOf(rand.nextInt(10) + 1);
        ApiResponse<Void> response = apiInstance.writeNewLiftRideWithHttpInfo(liftRide, resortID, seasonID, tempDayId, skierID);
        if (endTime == 420) {
          // phase 3
          int total = apiInstance.getSkierDayVertical(resortID, seasonID, tempDayId, skierID);
//          System.out.println(total);
        }
        long endTimeStamp = System.nanoTime();
        long latency = endTimeStamp - startTimeStamp;
        String result = startTimeStamp + "," + "POST," + latency + "," + response.getStatusCode() + "\n";
        queue.add(result);
        addToBucket(startTimeStamp, latency);
        StartClient.maxLatency = Math.max(latency, StartClient.maxLatency);
        if (response.getStatusCode() == SC_CREATED) {
          numOfSuccessfulReq.getAndIncrement();
        }
        numOfReq.getAndIncrement();
      } catch (Exception e) {
        System.err.println("Exception when calling SkiersApi#writeNewLiftRide");
        e.printStackTrace();
      }
    }
    queue.add("end");
    countDownLatch.countDown();
    countDownLatchTotal.countDown();
  }
  public synchronized void addToBucket(long startTimeStamp, long latency) {
    long sec = startTimeStamp / Utils.MS_PER_SEC;
    bucketMap.putIfAbsent(sec, new Bucket());
    bucketMap.get(sec).getTotalLatency().getAndAdd(latency);
    bucketMap.get(sec).getSize().getAndIncrement();
  }
}
