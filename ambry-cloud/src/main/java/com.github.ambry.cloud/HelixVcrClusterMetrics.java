/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.PartitionId;
import java.util.Set;


public class HelixVcrClusterMetrics {

  public final Counter partitionIdNotInClusterMapOnRemove;
  public final Counter partitionIdNotInClusterMapOnAdd;
  public Gauge<Integer> numberOfAssignedPartitions;

  public HelixVcrClusterMetrics(MetricRegistry registry, Set<PartitionId> assignedPartitionIds) {
    partitionIdNotInClusterMapOnRemove =
        registry.counter(MetricRegistry.name(HelixVcrCluster.class, "PartitionIdNotInClusterMapOnRemove"));
    partitionIdNotInClusterMapOnAdd =
        registry.counter(MetricRegistry.name(HelixVcrCluster.class, "PartitionIdNotInClusterMapOnAdd"));
    numberOfAssignedPartitions = () -> assignedPartitionIds.size();
    registry.register(MetricRegistry.name(HelixVcrCluster.class, "NumberOfAssignedPartitions"),
        numberOfAssignedPartitions);
  }
}
