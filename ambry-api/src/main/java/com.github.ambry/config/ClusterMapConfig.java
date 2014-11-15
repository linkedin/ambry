package com.github.ambry.config;

/**
 * The configs for resource state.
 */
public class ClusterMapConfig {

  /**
   * The window size for datanode state determination.
   */
  @Config("clustermap.datanode.window.ms")
  @Default("2000")
  public final int clusterMapDatanodeWindowMs;

  /**
   * The threshold for the number of errors to tolerate for a datanode in the datanode window
   */
  @Config("clustermap.datanode.error.threshold")
  @Default("3")
  public final int clusterMapDatanodeErrorThreshold;

  /**
   * The time to wait before a datanode is retried after it has been determined to be down.
   */
  @Config("clustermap.datanode.retry.backoff.ms")
  @Default("10000")
  public final int clusterMapDataNodeRetryBackoffMs;

  /**
   * The window size for disk state determination.
   */
  @Config("clustermap.disk.window.ms")
  @Default("2000")
  public final int clusterMapDiskWindowMs;

  /**
   * The threshold for the number of errors to tolerate for a disk in the disk window
   */
  @Config("clustermap.disk.error.threshold")
  @Default("1")
  public final int clusterMapDiskErrorThreshold;

  /**
   * The time to wait before a disk is retried after it has been determined to be down.
   */
  @Config("clustermap.disk.retry.backoff.ms")
  @Default("10000")
  public final int clusterMapDiskRetryBackoffMs;

  public ClusterMapConfig(VerifiableProperties verifiableProperties) {
    clusterMapDatanodeWindowMs = verifiableProperties.getIntInRange("clustermap.datanode.window.ms", 2000, 1, 100000);
    clusterMapDatanodeErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.datanode.error.threshold", 3, 1, 100);
    clusterMapDataNodeRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.datanode.retry.backoff.ms", 10000, 1, 100000);
    clusterMapDiskWindowMs = verifiableProperties.getIntInRange("clustermap.disk.window.ms", 2000, 1, 100000);
    clusterMapDiskErrorThreshold = verifiableProperties.getIntInRange("clustermap.disk.error.threshold", 1, 1, 100);
    clusterMapDiskRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.disk.retry.backoff.ms", 10000, 1, 100000);
  }
}
