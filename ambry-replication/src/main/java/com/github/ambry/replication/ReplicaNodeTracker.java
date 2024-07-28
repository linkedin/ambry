package com.github.ambry.replication;

import com.github.ambry.clustermap.DataNodeId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class ReplicaNodeTracker {
  DataNodeId nodeId;

  HashSet<RemoteReplicaInfo> activeWaiting;
  HashSet<RemoteReplicaInfo> timedOutWaiting;

  ArrayList<ReplicaGroupTracker> inFlight;

  public ReplicaNodeTracker(DataNodeId nodeId) {}

  public DataNodeId getNodeId() {
    return nodeId;
  }

  public void addInFlightGroup(ReplicaGroupTracker replicaGroupTracker) {
  }

  public ArrayList<ReplicaGroupTracker> getInFlightGroups() {
    return inFlight;
  }

  public void addActiveWaiting(List<RemoteReplicaInfo> active) {
  }

  public void addTimedOutWaiting(List<RemoteReplicaInfo> timedOutWaiting) {

  }
  public HashSet<RemoteReplicaInfo> getActiveWaiting() {
    return activeWaiting;
  }
  public HashSet<RemoteReplicaInfo> getTimedOutWaiting() {
    return timedOutWaiting;
  }
}
