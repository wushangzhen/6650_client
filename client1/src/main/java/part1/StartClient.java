package part1;

import java.io.FileOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import java.util.concurrent.atomic.AtomicInteger;
import part2.Bucket;

public class StartClient {
  public static int maxThreads = 0;
  public static int numOfSkiers = 0;
  public static int numOfSkierLift = 0;
  public static int meanNumberOfSkiLift = 0;
  public static String port = "http://localhost:8080/cs6650lab";
  // string s = http://34.219.46.253:8080/server;
//   http://lb2-170840443.us-west-2.elb.amazonaws.com:8080/server
  public static ConcurrentHashMap<Long, Bucket> bucketMap;
  public static long maxLatency = 0;
  public static AtomicInteger numOfReq;
  public static void main(String[] args) {
    try {
      if (args.length == 5) {
        maxThreads = Integer.parseInt(args[0]);
        numOfSkiers = Integer.parseInt(args[1]);
        numOfSkierLift = Integer.parseInt(args[2]);
        meanNumberOfSkiLift = Integer.parseInt(args[3]);
        port = args[4];
        System.out.println("maxThread: " + maxThreads);
        System.out.println("numOfSkiers: " + numOfSkiers);
        System.out.println("numOfSkierLift: " + numOfSkierLift);
        System.out.println("meanNumberOfSkiLift: " + meanNumberOfSkiLift);
        System.out.println("port: " + port);
      } else {
        throw new IllegalArgumentException("Wrong arguments number");
      }
      bucketMap = new ConcurrentHashMap<>();
      long startTime = System.nanoTime();
      AtomicInteger numOfSuccessfulReq = new AtomicInteger(0);
      numOfReq = new AtomicInteger(0);
      String filePath = "./data-" + maxThreads + ".csv";
      Utils utils = new Utils();
      FileOutputStream out = utils.createFileIfNotExisted(filePath);
      // part1.Phase 1
      System.out.println("Start part1.Phase 1");
      int numOfThread_P1 = maxThreads / 4;
      int skiersPerThread_P1 = numOfSkiers / numOfThread_P1;
      int count_P1 = (int) (numOfThread_P1 * 0.1);
      CountDownLatch latch_P1 = new CountDownLatch(count_P1);
      CountDownLatch latchTotal_P1 = new CountDownLatch(numOfThread_P1);
      int startTime_P1 = 1;
      int endTime_P1 = 90;
      double rate_P1 = 0.1;
      ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
      new Phase(numOfThread_P1, port, 1, "1", "1",
            skiersPerThread_P1, meanNumberOfSkiLift, startTime_P1, endTime_P1, numOfSkierLift,
            latch_P1, latchTotal_P1, rate_P1, numOfSuccessfulReq, numOfReq, queue, bucketMap, out).startPhase();
      latch_P1.await();
      // part1.Phase 2
      System.out.println("Start part1.Phase 2");
      int numOfThread_P2 = maxThreads;
      int skiersPerThread_P2 = numOfSkiers / numOfThread_P2;
      int startTime_P2 = 91;
      int endTime_P2 = 360;
      int count_P2 = (int) (numOfThread_P2 * 0.1);
      CountDownLatch latch_P2 = new CountDownLatch(count_P2);
      CountDownLatch latchTotal_P2 = new CountDownLatch(numOfThread_P2);
      double rate_P2 = 0.8;
      new Phase(numOfThread_P2, port, 1, "2019", "1",
          skiersPerThread_P2, meanNumberOfSkiLift, startTime_P2, endTime_P2, numOfSkierLift,
          latch_P2, latchTotal_P2, rate_P2, numOfSuccessfulReq, numOfReq, queue, bucketMap, out).startPhase();
      latch_P2.await();
      System.out.println("Start part1.Phase 3");
      int numOfThread_P3 = maxThreads / 4;
      int skiersPerThread_P3 = numOfSkiers / numOfThread_P3;
      int startTime_P3 = 361;
      int endTime_P3 = 420;
      double rate_P3 = 0.1;
      CountDownLatch latch_P3 = new CountDownLatch(numOfThread_P3);
      CountDownLatch latchTotal_P3 = new CountDownLatch(numOfThread_P3);
      new Phase(numOfThread_P3, port, 1, "2019", "1",
          skiersPerThread_P3, meanNumberOfSkiLift, startTime_P3, endTime_P3, numOfSkierLift,
          latch_P3, latchTotal_P3, rate_P3, numOfSuccessfulReq, numOfReq, queue, bucketMap, out).startPhase();
      latchTotal_P1.await();
      latchTotal_P2.await();
      latchTotal_P3.await();
      utils.analyseResults(startTime, numOfReq, numOfSuccessfulReq);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
