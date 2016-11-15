/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
   * The fixed timeout based resource state handling checks if we have had a 'threshold'
   * number of consecutive errors, and if so, considers the resource as down for
   * 'retry backoff' milliseconds.
   */

  /**
   * The threshold for the number of consecutive errors to tolerate for a datanode.
   */
  @Config("clustermap.fixedtimeout.datanode.error.threshold")
  @Default("6")
  public final int clusterMapFixedTimeoutDatanodeErrorThreshold;

  /**
   * The time to wait before a datanode is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.datanode.retry.backoff.ms")
  @Default("5 * 60 * 1000")
  public final int clusterMapFixedTimeoutDataNodeRetryBackoffMs;

  /**
   * The threshold for the number of errors to tolerate for a disk.
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

  /**
   * List of Datacenters to which local node needs SSL encryption to communicate
   */
  @Config("clustermap.ssl.enabled.datacenters")
  @Default("")
  public final String clusterMapSslEnabledDatacenters;

  public ClusterMapConfig(VerifiableProperties verifiableProperties) {
    clusterMapResourceStatePolicyFactory = verifiableProperties.getString("clustermap.resourcestatepolicy.factory",
        "com.github.ambry.clustermap.FixedBackoffResourceStatePolicyFactory");
    clusterMapFixedTimeoutDatanodeErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.error.threshold", 3, 1, 100);
    clusterMapFixedTimeoutDataNodeRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.retry.backoff.ms", 5 * 60 * 1000, 1,
            20 * 60 * 1000);
    clusterMapFixedTimeoutDiskErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.error.threshold", 1, 1, 100);
    clusterMapFixedTimeoutDiskRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.retry.backoff.ms", 10 * 60 * 1000, 1,
            30 * 60 * 1000);
    clusterMapSslEnabledDatacenters = verifiableProperties.getString("clustermap.ssl.enabled.datacenters", "");
  }
}
