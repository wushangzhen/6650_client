package part2;

import java.util.concurrent.atomic.AtomicLong;

public class Bucket {
  private AtomicLong totalLatency;
  private AtomicLong size;


  public Bucket() {
    this.totalLatency = new AtomicLong();
    this.size = new AtomicLong();
    this.totalLatency.set(0);
    this.size.set(0);
  }

  public AtomicLong getTotalLatency() {
    return totalLatency;
  }

  public AtomicLong getSize() {
    return size;
  }

}
