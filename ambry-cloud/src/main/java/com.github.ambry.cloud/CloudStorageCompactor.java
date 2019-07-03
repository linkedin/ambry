package com.github.ambry.cloud;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.replication.PartitionInfo;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudStorageCompactor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  private final CloudDestination cloudDestination;
  private final Map<PartitionId, PartitionInfo> partitionMap;
  private final VcrMetrics vcrMetrics;
  private final boolean testMode;

  /**
   * Public constructor.
   * @param cloudDestination the cloud destination to use.
   * @param partitionMap the map whose keys are the partitions to compact.
   * @param vcrMetrics the metrics to update.
   * @param testMode if true, the compaction methods only perform the query but do not actually remove the blobs.
   */
  public CloudStorageCompactor(CloudDestination cloudDestination, Map<PartitionId, PartitionInfo> partitionMap,
      VcrMetrics vcrMetrics, boolean testMode) {
    this.cloudDestination = cloudDestination;
    this.partitionMap = partitionMap;
    this.vcrMetrics = vcrMetrics;
    this.testMode = testMode;
  }

  @Override
  public void run() {
    // TODO: time the whole task
    compactPartitions();
  }

  /**
   * Purge the inactive blobs in all managed partitions.
   * @return the total number of blobs purged.
   */
  public int compactPartitions() {
    Set<PartitionId> partitions = new HashSet<>(partitionMap.keySet());
    logger.info("Beginning dead blob compaction for {} partitions", partitions.size());
    long startTime = System.currentTimeMillis();
    int totalBlobsPurged = 0;
    // TODO: what if reassignment happens in the middle and another VCR starts compacting the same partition?
    // The schedule delay won't help if other node is already up
    for (PartitionId partitionId : partitions) {
      String partitionPath = partitionId.toPathString();
      logger.info("Running dead blob compaction for partition {}", partitionPath);
      try {
        totalBlobsPurged += compactPartition(partitionPath);
      } catch (CloudStorageException ex) {
        logger.error("Compaction failed for partition {}", partitionPath, ex);
      }
    }
    long compactionTime = (System.currentTimeMillis() - startTime) / 1000;
    logger.info("Purged {} blobs in {} partitions taking {} seconds", totalBlobsPurged, partitions.size(),
        compactionTime);
    return totalBlobsPurged;
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * @param partitionPath the partition to compact.
   * @return the number of blobs purged.
   */
  public int compactPartition(String partitionPath) throws CloudStorageException {
    List<CloudBlobMetadata> deadBlobs = cloudDestination.getDeadBlobs(partitionPath);
    if (!testMode) {
      return cloudDestination.purgeBlobs(deadBlobs);
    } else {
      return deadBlobs.size();
    }
  }
}
