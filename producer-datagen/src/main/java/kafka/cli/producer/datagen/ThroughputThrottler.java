package kafka.cli.producer.datagen;

public class ThroughputThrottler {

  private static final long NS_PER_MS = 1000000L;
  private static final long NS_PER_SEC = 1000 * NS_PER_MS;
  private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

  final long startMs, sleepTimeNs, targetThroughput;

  private long sleepDeficitNs = 0;
  private boolean wakeup = false;

  public ThroughputThrottler(long startMs, long targetThroughput) {
    this.startMs = startMs;
    this.targetThroughput = targetThroughput;
    this.sleepTimeNs =
      targetThroughput > 0 ? NS_PER_SEC / targetThroughput : Long.MAX_VALUE;
  }

  public boolean shouldThrottle(long amountSoFar, long sendStartMs) {
    if (this.targetThroughput < 0) {
      // No throttling in this case
      return false;
    }

    float elapsedSec = (sendStartMs - startMs) / 1000.f;
    return elapsedSec > 0 && (amountSoFar / elapsedSec) > this.targetThroughput;
  }

  public void throttle() {
    if (targetThroughput == 0) {
      try {
        synchronized (this) {
          while (!wakeup) {
            this.wait();
          }
        }
      } catch (InterruptedException e) {
        // do nothing
      }
      return;
    }

    // throttle throughput by sleeping, on average,
    // (1 / this.throughput) seconds between "things sent"
    sleepDeficitNs += sleepTimeNs;

    // If enough sleep deficit has accumulated, sleep a little
    if (sleepDeficitNs >= MIN_SLEEP_NS) {
      long sleepStartNs = System.nanoTime();
      try {
        synchronized (this) {
          long remaining = sleepDeficitNs;
          while (!wakeup && remaining > 0) {
            long sleepMs = remaining / 1000000;
            long sleepNs = remaining - sleepMs * 1000000;
            this.wait(sleepMs, (int) sleepNs);
            long elapsed = System.nanoTime() - sleepStartNs;
            remaining = sleepDeficitNs - elapsed;
          }
          wakeup = false;
        }
        sleepDeficitNs = 0;
      } catch (InterruptedException e) {
        // If sleep is cut short, reduce deficit by the amount of
        // time we actually spent sleeping
        long sleepElapsedNs = System.nanoTime() - sleepStartNs;
        if (sleepElapsedNs <= sleepDeficitNs) {
          sleepDeficitNs -= sleepElapsedNs;
        }
      }
    }
  }

  /** Wakeup the throttler if its sleeping. */
  public void wakeup() {
    synchronized (this) {
      wakeup = true;
      this.notifyAll();
    }
  }
}
