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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultDisruptionService implements DisruptionService {
  // This class is a placeholder for the actual implementation of the DisruptionService.
  // It should contain methods to handle disruptions and batch operations by partition.

  @Override
  public Map<PartitionId, List<Operation>> batchDisruptionsByPartition(List<PartitionId> partitionIds) {
    Map<PartitionId, List<Operation>> partitionOperationsMap = new HashMap<>();
    for (PartitionId partitionId : partitionIds) {
      // Process each partitionId
      partitionOperationsMap.put(partitionId, Arrays.asList());
    }

    return partitionOperationsMap;
  }

  @Override
  public List<PartitionId> sortByDisruptions(List<PartitionId> partitionIds) {
    return partitionIds;
  }
}
