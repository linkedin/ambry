package com.github.ambry.clustermap;

import com.github.ambry.utils.SystemTime;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;


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
  private AtomicBoolean up;
  private long downUntil;
  private ArrayDeque<Long> failureQueue;

  public FixedBackoffResourceStatePolicy(boolean hardDown, long failureWindowSizeMs, int failureCountThreshold,
      long retryBackoffMs) {
    this.hardDown = hardDown;
    this.failureWindowSizeMs = failureWindowSizeMs;
    this.failureCountThreshold = failureCountThreshold;
    this.retryBackoffMs = retryBackoffMs;
    this.up = new AtomicBoolean(true);
    this.downUntil = 0;
    failureQueue = new ArrayDeque<Long>();
  }

  private boolean isUp() {
    if (!up.get()) {
      synchronized (this) {
        if (SystemTime.getInstance().milliseconds() > downUntil) {
          up.set(true);
          downUntil = 0;
        } else {
          return false;
        }
      }
    }
    return true;
  }

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
        up.set(false);
        failureQueue.clear();
        downUntil = SystemTime.getInstance().milliseconds() + retryBackoffMs;
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
