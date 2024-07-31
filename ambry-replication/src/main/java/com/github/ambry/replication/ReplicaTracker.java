package com.github.ambry.replication;

import com.github.ambry.utils.Time;


class ReplicaTracker {
  private final RemoteReplicaInfo replica;
  private final long throttleDurationInMs;
  private final Time time;
  private long throttledTill;
  private boolean isStandByTimedOut;
  private boolean isIterating;

  ReplicaTracker(RemoteReplicaInfo replica, Time time, long throttleDurationInMs) {
    this.replica = replica;
    this.throttleDurationInMs = throttleDurationInMs;
    this.time = time;
    throttledTill = 0;
    isStandByTimedOut = false;
    isIterating = false;
  }

  RemoteReplicaInfo getReplica() {
    return replica;
  }

  private void throttle() {
    throttledTill = time.milliseconds() + throttleDurationInMs;
  }

  void setSandByTimedOut(boolean value) {
    isStandByTimedOut = value;
  }

  boolean isStandByTimedOut() {
    return isStandByTimedOut;
  }

  void iterationFinished() {
    isIterating = false;
    isStandByTimedOut = true;
    throttle();
  }

  boolean isFree() {
    return throttledTill <= time.milliseconds();
  }
}
