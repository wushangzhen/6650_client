package part1;

import java.io.FileOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import part2.Bucket;

public class Phase {
  private String port;
  private Integer resortID;
  private String seasonID;
  private String dayID;
  private Integer meanNumberOfSkiLift;
  private int skiersPerThread;
  private int startTime;
  private int endTime;
  private int numOfSkierLift;
  private CountDownLatch countDownLatch;
  private CountDownLatch countDownLatchTotal;
  private double rate;
  private AtomicInteger numOfSuccessfulReq;
  private AtomicInteger numOfReq;
  private ConcurrentLinkedQueue<String> queue;
  private ConcurrentHashMap<Long, Bucket> bucketMap;
  private int numOfThreads;
  private FileOutputStream out;

  public Phase(int numOfThreads, String port, Integer resortID, String seasonID, String dayID,
      Integer meanNumberOfSkiLift, int skiersPerThread, int startTime, int endTime,
      int numOfSkierLift, CountDownLatch countDownLatch, CountDownLatch countDownLatchTotal,
      double rate, AtomicInteger numOfSuccessfulReq, AtomicInteger numOfReq,
      ConcurrentLinkedQueue<String> queue,
      ConcurrentHashMap<Long, Bucket> bucketMap,
      FileOutputStream out) {
    this.port = port;
    this.resortID = resortID;
    this.seasonID = seasonID;
    this.dayID = dayID;
    this.meanNumberOfSkiLift = meanNumberOfSkiLift;
    this.skiersPerThread = skiersPerThread;
    this.startTime = startTime;
    this.endTime = endTime;
    this.numOfSkierLift = numOfSkierLift;
    this.countDownLatch = countDownLatch;
    this.countDownLatchTotal = countDownLatchTotal;
    this.rate = rate;
    this.numOfSuccessfulReq = numOfSuccessfulReq;
    this.numOfReq = numOfReq;
    this.queue = queue;
    this.bucketMap = bucketMap;
    this.numOfThreads = numOfThreads;
    this.out = out;
  }
  public void startPhase() {
    for (int i = 0; i < numOfThreads; i++) {
      Request request = new Request(i, port, resortID, seasonID, dayID,
          skiersPerThread, meanNumberOfSkiLift, startTime, endTime, numOfSkierLift,
          countDownLatch, countDownLatchTotal, rate, numOfSuccessfulReq, numOfReq, queue, bucketMap);
      new Thread(request).start();
      new Thread(new WriteFile(out, queue)).start();
    }
  }
}
