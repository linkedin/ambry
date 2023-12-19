package com.github.ambry.clustermap;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class ResourceInfo {
  private final List<String> liveInstances;
  private final List<String> unavailableInstances;
  private final int totalInstanceCount;
  private final long liveCapacity;
  private final long unavailableCapacity;
  private final int numPartitions;
  private final int numExpectedReplicas;
  private final int numCurrentReplicas;
  private final int expectedTotalReplicaWeight;
  private final int currentTotalReplicaWeight;
  private final Map<String, Set<String>> failedDisks;

  public ResourceInfo(List<String> liveInstances, List<String> unavailableInstances, long liveCapacity,
      long unavailableCapacity, int numPartitions, int numExpectedReplicas, int numCurrentReplicas,
      int expectedTotalReplicaWeight, int currentTotalReplicaWeight, Map<String, Set<String>> failedDisks) {
    this.liveInstances = liveInstances;
    this.unavailableInstances = unavailableInstances;
    this.totalInstanceCount = liveInstances.size() + unavailableInstances.size();
    this.liveCapacity = liveCapacity;
    this.unavailableCapacity = unavailableCapacity;
    this.numPartitions = numPartitions;
    this.numExpectedReplicas = numExpectedReplicas;
    this.numCurrentReplicas = numCurrentReplicas;
    this.expectedTotalReplicaWeight = expectedTotalReplicaWeight;
    this.currentTotalReplicaWeight = currentTotalReplicaWeight;
    this.failedDisks = failedDisks;
  }
}