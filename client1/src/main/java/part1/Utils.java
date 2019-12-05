package part1;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
  public static int MS_PER_SEC = 1_000_000_000;
  public FileOutputStream createFileIfNotExisted(String path) throws Exception {
    File file = new File(path);
    if (file.delete()) {
      System.out.println("File deleted successfully");
    } else {
      System.out.println("Failed to delete the file");
    }
    if (file.createNewFile()) {
      System.out.println("data.csv created");
    } else {
      System.out.println("File data.csv already exists");
    }
    return new FileOutputStream(file, true);
  }
  public void analyseResults(long startTime, AtomicInteger numOfReq, AtomicInteger numOfSuccessfulReq) {
    long endTime = System.nanoTime();
    long duration = (endTime - startTime);
    int totalNumberOfReq = numOfReq.get();
    int successfulReq = numOfSuccessfulReq.get();
    System.out.println("--------------------------------------------------------------");
    System.out.println("Number of Successful Requests: " + successfulReq);
    System.out.println("Number of Unsuccessful Requests: " + (totalNumberOfReq - successfulReq));
    System.out.println("Number of Requests: " + totalNumberOfReq);
    System.out.println("Execution Time: " + duration / MS_PER_SEC + "s");
    System.out.println("--------------------------------------------------------------");
  }
}
