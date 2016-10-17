package com.github.ambry.store;

import com.github.ambry.utils.Throttler;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Helps schedule I/O operations.
 * This is meant to be used by
 * 1. Application reads/writes from/to the log.
 * 2. Hard delete
 * 3. Compaction
 * The initial implementation simply returns MAX_VALUE for the application and use the throttler for the other 2.
 * In the future this will have functions to submit feedback so that more intelligent decisions can be made.
 */
class DiskIOScheduler implements Closeable {
  private final Map<String, Throttler> throttlers = new ConcurrentHashMap<>();

  /**
   * Create a {@link DiskIOScheduler}.
   * @param throttlers the {@link Throttler}s to use for each job type.
   */
  DiskIOScheduler(Map<String, ? extends Throttler> throttlers) {
    if (throttlers != null) {
      this.throttlers.putAll(throttlers);
    }
  }

  /**
   * Return the size of I/O permissible based on the parameters provided.
   * @param jobType the type of the job requesting an I/O slice.
   * @param jobId the ID of the job requesting an I/O slice.
   * @param usedSinceLastCall the amount of capacity used since the last call to this function.
   * @return the I/O slice available for use.
   * @throws InterruptedException
   */
  long getSlice(String jobType, String jobId, long usedSinceLastCall)
      throws InterruptedException {
    Throttler throttler = throttlers.get(jobType);
    if (throttler != null) {
      throttler.maybeThrottle(usedSinceLastCall);
    }
    return Long.MAX_VALUE;
  }

  /**
   * Release resources and close throttlers
   */
  @Override
  public void close() {
    for (Throttler throttler : throttlers.values()) {
      throttler.close();
    }
  }
}

