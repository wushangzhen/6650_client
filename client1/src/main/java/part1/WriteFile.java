package part1;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WriteFile implements Runnable {
  private FileOutputStream out;
  private ConcurrentLinkedQueue<String> queue;

	public WriteFile(FileOutputStream out, ConcurrentLinkedQueue<String> queue) {
	  this.out = out;
	  this.queue = queue;
	}

  @Override
  public void run() {
    synchronized (queue) {
      while (true) {
        if (!queue.isEmpty()) {
          try {
            String str = queue.poll();
            if (str.equals("end")) {
              break;
            }
            out.write(str.getBytes("UTF-8"));
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    }
	}
}
