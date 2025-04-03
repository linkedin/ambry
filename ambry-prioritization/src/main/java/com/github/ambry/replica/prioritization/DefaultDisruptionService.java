package com.github.ambry.replica.prioritization;

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
}
