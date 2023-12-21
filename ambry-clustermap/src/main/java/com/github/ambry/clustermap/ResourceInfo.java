/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Data class to represent the information of a resource. All fields are public so json serializer can serialize
 * objects of this class.
 */
public class ResourceInfo {
  public final String resourceName;
  public final List<String> liveInstances;
  public final List<String> unavailableInstances;
  public final int totalInstanceCount;
  public final long liveCapacity;
  public final long unavailableCapacity;
  public final int numPartitions;
  public final int numExpectedReplicas;
  public final int numCurrentReplicas;
  public final int expectedTotalReplicaWeight;
  public final int currentTotalReplicaWeight;
  public final Map<String, Set<String>> failedDisks;

  /**
   * Constructor to create a {@link ResourceInfo}.
   * @param resourceName
   * @param liveInstances
   * @param unavailableInstances
   * @param liveCapacity
   * @param unavailableCapacity
   * @param numPartitions
   * @param numExpectedReplicas
   * @param numCurrentReplicas
   * @param expectedTotalReplicaWeight
   * @param currentTotalReplicaWeight
   * @param failedDisks
   */
  public ResourceInfo(String resourceName, List<String> liveInstances, List<String> unavailableInstances,
      long liveCapacity, long unavailableCapacity, int numPartitions, int numExpectedReplicas, int numCurrentReplicas,
      int expectedTotalReplicaWeight, int currentTotalReplicaWeight, Map<String, Set<String>> failedDisks) {
    this.resourceName = resourceName;
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