package com.github.ambry.replication;

public class ReplicaGroupTracker {
  ReplicaThread.RemoteReplicaGroup replicaGroup;
  int iterCount = 0;
  long resumeAt;
  public ReplicaGroupTracker ReplicaGroupTracker() {
    return this;
  }
  public ReplicaThread.RemoteReplicaGroup getRemoteReplicaGroup() {
    return replicaGroup;
  }

  public ReplicaGroupTracker setReplicaGroup(ReplicaThread.RemoteReplicaGroup replicaGroup) {
    this.replicaGroup = replicaGroup;
    return this;
  }

  public boolean isDone() {
    return replicaGroup.isDone();
  }
}
