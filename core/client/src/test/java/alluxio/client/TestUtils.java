package alluxio.client;

import com.google.common.base.Stopwatch;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class created on 2017/12/28.
 *
 * @author Connery
 */
public class TestUtils {

  public static long benchmark(int loopCount, Callable callable) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    try {
      for (int i = 0; i < loopCount; i++) {
        callable.call();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    watch.stop();
    long time = watch.elapsed(TimeUnit.MILLISECONDS);
    System.out.println("avg time" + (time / (float) loopCount));
    return time;
  }
}
