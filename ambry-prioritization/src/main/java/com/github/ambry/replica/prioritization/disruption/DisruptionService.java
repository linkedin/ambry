/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.replica.prioritization.disruption;

import com.github.ambry.clustermap.PartitionId;
import java.util.List;
import java.util.Map;

/**
 * The DisruptionService interface defines the contract for services that handle disruptions.
 * Implementing classes should provide functionality to batch operations by partition.
 */
public interface DisruptionService {
  /**
   * Batches operations by partition by merging operations from all replicas in the local datacenter.
   *
   * @param partitionIds List of partitions to process
   * @return A map of PartitionId to a list of merged DisruptionOperations
   */
  Map<PartitionId, List<Operation>> batchDisruptionsByPartition(List<PartitionId> partitionIds);

  default List<PartitionId> sortByDisruptions(List<PartitionId> partitionIds) {
    return partitionIds;
  }
}
