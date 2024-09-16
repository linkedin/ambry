/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replication.continuous;

import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.utils.Time;


/**
 * This class tracks for a current status of Replica for a continuous replication cycle.
 * This also tracks whether a replicas is throttled or not in a continuous replication cycle.
 */
public class ReplicaTracker {
  private final RemoteReplicaInfo remoteReplicaInfo;
  private ReplicaStatus replicaStatus;
  private final Time time;
  private final long throttleDurationMs;
  private long throttledTill;

  public ReplicaTracker(RemoteReplicaInfo remoteReplicaInfo, Time time, long throttleDurationMs) {
    this.remoteReplicaInfo = remoteReplicaInfo;
    this.replicaStatus = ReplicaStatus.UNKNOWN;
    this.time = time;
    this.throttleDurationMs = throttleDurationMs;
    this.throttledTill = 0;
  }

  public RemoteReplicaInfo getRemoteReplicaInfo() {
    return remoteReplicaInfo;
  }

  public void setReplicaStatus(ReplicaStatus replicaStatus) {
    this.replicaStatus = replicaStatus;
  }

  public ReplicaStatus getReplicaStatus() {
    return replicaStatus;
  }

  public boolean isThrottled() {
    return time.milliseconds() <= throttledTill;
  }

  /**
   * whenever a group is finished, this method gets called for corresponding replica trackers,
   * replica status is moved to unknown and replica is throttled
   */
  public void finishIteration() {
    this.replicaStatus = ReplicaStatus.UNKNOWN;
    this.throttledTill = time.milliseconds() + throttleDurationMs;
  }
}
