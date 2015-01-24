package com.github.ambry.clustermap;

import com.github.ambry.utils.SystemTime;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FixedBackoffResourceStatePolicy marks a resource as unavailable for retryBackoff milliseconds if the number of
 * consecutive errors the resource encountered is greater than failureCountThreshold.
 */
class FixedBackoffResourceStatePolicy implements ResourceStatePolicy {
  private final Object resource;
  private final boolean hardDown;
  private final AtomicInteger failureCount;
  private final int failureCountThreshold;
  private final long retryBackoffMs;
  private long downUntil;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public FixedBackoffResourceStatePolicy(Object resource, boolean hardDown,  int failureCountThreshold,
      long retryBackoffMs) {
    this.resource = resource;
    this.hardDown = hardDown;
    this.failureCountThreshold = failureCountThreshold;
    this.retryBackoffMs = retryBackoffMs;
    this.downUntil = 0;
    this.failureCount = new AtomicInteger(0);
  }

  /*
   * On an error, if the failureCount is greater than the threshold, mark the node as down.
   */
  @Override
  public void onError() {
    synchronized(this) {
      if (failureCount.incrementAndGet() >= failureCountThreshold) {
        downUntil = SystemTime.getInstance().milliseconds() + retryBackoffMs;
        logger.error("Resource " + resource + " has gone down");
      }
    }
  }

  /*
   * Only take the lock if failureCount is greater than 0, so that the lock is not taken during normal conditions.
   * Note that a single response resets the count.
   */
  @Override
  public void onSuccess() {
    if (failureCount.get() > 0) {
      synchronized (this) {
        failureCount.set(0);
      }
    }
  }

  /*
   * If the number of failures are above the threshold, the resource will be counted as down unless downUntil is in
   * the past.
   * Note how failureCount is not reset to 0 here. This is so that the node is marked as down if the first request after
   * marking a node back up, also times out. We only reset failureCount on an actual response, so down nodes get a
   * 'chance' to prove they are back up every retryBackoffMs - they do not get 'fully up' status until they are actually
   * responsive.
   */
  @Override
  public boolean isDown() {
    boolean ret = false;
    if (hardDown) {
      ret = true;
    } else if (failureCount.get() >= failureCountThreshold) {
      synchronized (this) {
        if (SystemTime.getInstance().milliseconds() < downUntil) {
          ret = true;
        }
      }
    }
    return ret;
  }

  @Override
  public boolean isHardDown() {
    return hardDown;
  }
}
