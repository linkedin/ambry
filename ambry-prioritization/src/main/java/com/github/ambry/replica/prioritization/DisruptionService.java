package com.github.ambry.replica.prioritization;

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
}
