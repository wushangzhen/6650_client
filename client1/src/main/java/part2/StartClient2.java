package part2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import part1.StartClient;
import part1.Utils;

public class StartClient2 {
  public static void main(String[] args) {
    long startTime = System.nanoTime();
    StartClient.main(args);
    ConcurrentHashMap<Long, Bucket> bucketMap = StartClient.bucketMap;
    List<Long> latencies = new ArrayList<>();
    long totalSizes = 0;
    long totalLatencies = 0;
    for (Map.Entry<Long, Bucket> entry : bucketMap.entrySet()) {
      long totalLatency = entry.getValue().getTotalLatency().get();
      long totalSize = entry.getValue().getSize().get();
      latencies.add(totalLatency / totalSize);
      totalSizes += totalSize;
      totalLatencies += totalLatency;
    }
    Collections.sort(latencies);
    int index = (int) Math.floor(latencies.size() * 0.99);
    System.out.println("The Mean Latency: " + totalLatencies / (totalSizes * 1_000_000) + "ms");
    System.out.println("The Medium Latency: " + latencies.get(latencies.size() / 2) / 1_000_000 + "ms");
    System.out.println("Throughput: " + totalSizes / ((System.nanoTime() - startTime) / Utils.MS_PER_SEC));
    System.out.println("The 99th Percentile Latency: " + latencies.get(index) / 1_000_000 + "ms");
    System.out.println("The Max Latency: " + latencies.get(latencies.size() - 1) / 1_000_000 + "ms");
    System.out.println("--------------------------------------------------------------");
  }
}
