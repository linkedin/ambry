package com.github.ambry.clustermap;

import com.github.ambry.utils.SystemTime;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/** ResourceStatePolicy is used to determine if the state of a resource is "up" or "down". For resources like data nodes
 *  and disks, up and down mean available and unavailable, respectively.
 */
public interface ResourceStatePolicy {
  /**
   * Checks to see if the state is permanently down.
   *
   * @return true if the state is down, false otherwise.
   */
  public boolean isHardDown();

  /**
   * Checks to see if the state is temporarily down (soft failed).
   *
   * @return true if the state is down, false otherwise.
   */
  public boolean isSoftDown();

  /**
   * Should be called by the caller every time an error is encountered for the corresponding resource.
   */
  public void onError();
}

abstract class FixedBackoffResourceStatePolicy implements ResourceStatePolicy {
  private final boolean hardDown;
  private final int failureCountThreshold;
  private final long failureWindowSizeMs;
  private final long retryBackoffMs;
  private AtomicLong downUntil;
  private ArrayDeque<Long> failureQueue;

  public FixedBackoffResourceStatePolicy(boolean hardDown, long failureWindowSizeMs, int failureCountThreshold,
      long retryBackoffMs) {
    this.hardDown = hardDown;
    this.failureWindowSizeMs = failureWindowSizeMs;
    this.failureCountThreshold = failureCountThreshold;
    this.retryBackoffMs = retryBackoffMs;
    this.downUntil = new AtomicLong(0);
    failureQueue = new ArrayDeque<Long>();
  }

  /* If down (which is checked locklessly), check to see if it is time to be up.
   */
  private boolean isUp() {
    if (downUntil.get() != 0) {
      synchronized (this) {
        if (SystemTime.getInstance().milliseconds() > downUntil.get()) {
          downUntil.set(0);
        } else {
          return false;
        }
      }
    }
    return true;
  }

  /** On error, check if there have been threshold number of errors in the last failure window milliseconds.
   *  If so, make this resource down until now + retryBackoffMs. The size of the queue is the threshold.
   */
  @Override
  public void onError() {
    synchronized (this) {
      while (failureQueue.size() > 0
          && failureQueue.getFirst() < SystemTime.getInstance().milliseconds() - failureWindowSizeMs) {
        failureQueue.remove();
      }
      if (failureQueue.size() < failureCountThreshold) {
        failureQueue.add(SystemTime.getInstance().milliseconds());
      } else {
        failureQueue.clear();
        downUntil.set(SystemTime.getInstance().milliseconds() + retryBackoffMs);
      }
    }
  }

  @Override
  public boolean isHardDown() {
    return hardDown;
  }

  @Override
  public boolean isSoftDown() {
    return !hardDown && !isUp();
  }
}

class DataNodeStatePolicy extends FixedBackoffResourceStatePolicy {
  public DataNodeStatePolicy(HardwareState initialState, long failureWindowInitialSizeMs, int failureCountThreshold,
      long retryBackoffMs) {
    super(initialState == HardwareState.UNAVAILABLE, failureWindowInitialSizeMs, failureCountThreshold, retryBackoffMs);
  }

  public HardwareState getState() {
    return isHardDown() || isSoftDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }
}

class DiskStatePolicy extends FixedBackoffResourceStatePolicy {

  public DiskStatePolicy(HardwareState initialState, long failureWindowInitialSizeMs, int failureCountThreshold,
      long retryBackoffMs) {
    super(initialState == HardwareState.UNAVAILABLE, failureWindowInitialSizeMs, failureCountThreshold, retryBackoffMs);
  }

  public HardwareState getState() {
    return isHardDown() || isSoftDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }
}
