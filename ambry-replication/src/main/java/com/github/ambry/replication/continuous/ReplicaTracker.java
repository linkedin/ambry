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


public class ReplicaTracker {
  private final RemoteReplicaInfo remoteReplicaInfo;
  private ReplicaState replicaState;
  private long throttledTill;

  public ReplicaTracker(RemoteReplicaInfo remoteReplicaInfo) {
    this.remoteReplicaInfo = remoteReplicaInfo;
    this.replicaState = ReplicaState.UNKNOWN;
    this.throttledTill = 0;
  }

  public RemoteReplicaInfo getRemoteReplicaInfo() {
    return remoteReplicaInfo;
  }

  public void setReplicaState(ReplicaState replicaState) {
    this.replicaState = replicaState;
  }

  public ReplicaState getReplicaState() {
    return replicaState;
  }

  public boolean isThrottled(Time time) {
    return time.milliseconds() <= throttledTill;
  }

  public void setThrottledTill(long throttledTill) {
    this.throttledTill = throttledTill;
  }
}
