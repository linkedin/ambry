package com.github.ambry.config;

/**
 * The configs for resource state.
 */
public class ClusterMapConfig {

  /**
   * The factory class used to get the resource state policies.
   */
  @Config("clustermap.resourcestatepolicy.factory")
  @Default("com.github.ambry.clustermap.FixedBackoffResourceStatePolicyFactory")
  public final String clusterMapResourceStatePolicyFactory;

  /**
   * The fixed timeout based resource state handling checks if we have had a 'threshold' number of errors in the last
   * time window of size 'window' milliseconds, and if so, considers the resource as down for 'retry backoff'
   * milliseconds.
   */

  /**
   * The window size for datanode state determination.
   */
  @Config("clustermap.fixedtimeout.datanode.window.ms")
  @Default("2000")
  public final int clusterMapFixedTimeoutDatanodeWindowMs;

  /**
   * The threshold for the number of errors to tolerate for a datanode in the datanode window
   */
  @Config("clustermap.fixedtimeout.datanode.error.threshold")
  @Default("3")
  public final int clusterMapFixedTimeoutDatanodeErrorThreshold;

  /**
   * The time to wait before a datanode is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.datanode.retry.backoff.ms")
  @Default("5 * 60 * 1000")
  public final int clusterMapFixedTimeoutDataNodeRetryBackoffMs;

  /**
   * The window size for disk state determination.
   */
  @Config("clustermap.fixedtimeout.disk.window.ms")
  @Default("2000")
  public final int clusterMapFixedTimeoutDiskWindowMs;

  /**
   * The threshold for the number of errors to tolerate for a disk in the disk window
   */
  @Config("clustermap.fixedtimeout.disk.error.threshold")
  @Default("1")
  public final int clusterMapFixedTimeoutDiskErrorThreshold;

  /**
   * The time to wait before a disk is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.disk.retry.backoff.ms")
  @Default("10 * 60 * 1000")
  public final int clusterMapFixedTimeoutDiskRetryBackoffMs;

  public ClusterMapConfig(VerifiableProperties verifiableProperties) {
    clusterMapResourceStatePolicyFactory = verifiableProperties.getString("clustermap.resourcestatepolicy.factory",
        "com.github.ambry.clustermap.FixedBackoffResourceStatePolicyFactory");
    clusterMapFixedTimeoutDatanodeWindowMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.window.ms", 2000, 1, 100000);
    clusterMapFixedTimeoutDatanodeErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.error.threshold", 3, 1, 100);
    clusterMapFixedTimeoutDataNodeRetryBackoffMs = verifiableProperties
        .getIntInRange("clustermap.fixedtimeout.datanode.retry.backoff.ms", 10000, 1, 5 * 60 * 1000);
    clusterMapFixedTimeoutDiskWindowMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.window.ms", 2000, 1, 100000);
    clusterMapFixedTimeoutDiskErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.error.threshold", 1, 1, 100);
    clusterMapFixedTimeoutDiskRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.retry.backoff.ms", 10000, 1, 10 * 60 * 1000);
  }
}
