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
 * Configuration parameters required by a {@link com.github.ambry.router.Router}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for a Router and presents them for retrieval through defined APIs.
 */
public class RouterConfig {

  /**
   * Number of independent scaling units for the router.
   */
  @Config("router.scaling.unit.count")
  @Default("1")
  public final int routerScalingUnitCount;

  /**
   * The hostname of the node upon which the router runs.
   */
  @Config("router.hostname")
  public final String routerHostname;

  /**
   * The name of the datacenter in which the router is located.
   */
  @Config("router.datacenter.name")
  public final String routerDatacenterName;

  /**
   * The max connections allowed per (datanode, port) for plain text
   */
  @Config("router.scaling.unit.max.connections.per.port.plain.text")
  @Default("5")
  public final int routerScalingUnitMaxConnectionsPerPortPlainText;

  /**
   * The max connections allowed per (datanode, port) for ssl
   */
  @Config("router.scaling.unit.max.connections.per.port.ssl")
  @Default("2")
  public final int routerScalingUnitMaxConnectionsPerPortSsl;

  /**
   * The percentage of {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortSsl} or
   * {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortPlainText} to warm up in the startup.
   * {@link RouterConfig#routerConnectionsWarmUpTimeoutMs} may need to be adjusted.
   */
  @Config("router.connections.warm.up.percentage.per.port")
  @Default("25")
  public final int routerConnectionsWarmUpPercentagePerPort;

  /**
   * The max time allowed to establish connections to local DC in the startup
   */
  @Config("router.connections.warm.up.timeout.ms")
  @Default("5000")
  public final int routerConnectionsWarmUpTimeoutMs;

  /**
   * Timeout for checking out an available connection to a (datanode, port).
   */
  @Config("router.connection.checkout.timeout.ms")
  @Default("1000")
  public final int routerConnectionCheckoutTimeoutMs;

  /**
   * Timeout for requests issued by the router to the network layer.
   */
  @Config("router.request.timeout.ms")
  @Default("2000")
  public final int routerRequestTimeoutMs;

  /**
   * The max chunk size to be used for put operations.
   */
  @Config("router.max.put.chunk.size.bytes")
  @Default("4*1024*1024")
  public final int routerMaxPutChunkSizeBytes;

  /**
   * The maximum number of parallel requests issued at a time by the put manager for a chunk.
   */
  @Config("router.put.request.parallelism")
  @Default("3")
  public final int routerPutRequestParallelism;

  /**
   * The minimum number of successful responses required for a put operation.
   */
  @Config("router.put.success.target")
  @Default("2")
  public final int routerPutSuccessTarget;

  /**
   * The maximum number of times to retry putting any chunk of a put operation
   */
  @Config("router.max.slipped.put.attempts")
  @Default("1")
  public final int routerMaxSlippedPutAttempts;

  /**
   * The maximum number of parallel requests allowed for a delete operation.
   */
  @Config("router.delete.request.parallelism")
  @Default("3")
  public final int routerDeleteRequestParallelism;

  /**
   * The minimum number of successful responses required for a delete operation.
   */
  @Config("router.delete.success.target")
  @Default("2")
  public final int routerDeleteSuccessTarget;

  /**
   * The maximum number of parallel requests issued at a time by the get manager for a get operation on a chunk.
   */
  @Config("router.get.request.parallelism")
  @Default("2")
  public final int routerGetRequestParallelism;

  /**
   * The minimum number of successful responses required for a get operation on a chunk.
   */
  @Config("router.get.success.target")
  @Default("1")
  public final int routerGetSuccessTarget;

  /**
   * Indicates whether get operations are allowed to make requests to nodes in remote data centers.
   */
  @Config("router.get.cross.dc.enabled")
  @Default("true")
  public final boolean routerGetCrossDcEnabled;

  /**
   * Indicates whether get operations are allowed to make requests to nodes in non-originating remote data centers.
   */
  @Config("router.get.include.non.originating.dc.replicas")
  @Default("true")
  public final boolean routerGetIncludeNonOriginatingDcReplicas;

  /**
   * Number of replicas required for GET OperationTracker when routerGetIncludeNonOriginatingDcReplicas is False.
   * Please note routerGetReplicasRequired is 6 because total number of local and originating replicas is always <= 6.
   * This may no longer be true with partition classes and flexible replication.
   */
  @Config("router.get.replicas.required")
  @Default("6")
  public final int routerGetReplicasRequired;
  /**
   * The OperationTracker to use for GET operations.
   */
  @Config("router.get.operation.tracker.type")
  @Default("SimpleOperationTracker")
  public final String routerGetOperationTrackerType;

  /**
   * If an adaptive operation tracker is being used, a request is discounted from the parallelism count if it has been
   * outstanding for more than the quantile defined here (compared to latencies of other requests of the same class).
   */
  @Config("router.latency.tolerance.quantile")
  @Default("0.9")
  public final double routerLatencyToleranceQuantile;

  /**
   * The version to use for new BlobIds.
   */
  @Config("router.blobid.current.version")
  @Default("5")
  public final short routerBlobidCurrentVersion;

  /**
   * The KeyManagementServiceFactory that will be used to fetch {@link com.github.ambry.router.KeyManagementService}
   */
  @Config("router.key.management.service.factory")
  @Default("com.github.ambry.router.SingleKeyManagementServiceFactory")
  public final String routerKeyManagementServiceFactory;

  /**
   * The CryptoServiceFactory that will be used to fetch {@link com.github.ambry.router.CryptoService}
   */
  @Config("router.crypto.service.factory")
  @Default("com.github.ambry.router.GCMCryptoServiceFactory")
  public final String routerCryptoServiceFactory;

  /**
   * Number of crypto jobs worker count
   */
  @Config("router.crypto.jobs.worker.count")
  @Default("1")
  public final int routerCryptoJobsWorkerCount;

  /**
   * The maximum number of parallel requests issued at a time by the TTL update manager for a chunk.
   */
  @Config("router.ttl.update.request.parallelism")
  @Default("3")
  public final int routerTtlUpdateRequestParallelism;

  /**
   * The minimum number of successful responses required for a TTL update operation.
   */
  @Config("router.ttl.update.success.target")
  @Default("2")
  public final int routerTtlUpdateSuccessTarget;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerScalingUnitCount = verifiableProperties.getIntInRange("router.scaling.unit.count", 1, 1, Integer.MAX_VALUE);
    routerHostname = verifiableProperties.getString("router.hostname");
    routerDatacenterName = verifiableProperties.getString("router.datacenter.name");
    routerScalingUnitMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.plain.text", 5, 1, 20);
    routerScalingUnitMaxConnectionsPerPortSsl =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.ssl", 2, 1, 20);
    routerConnectionsWarmUpPercentagePerPort =
        verifiableProperties.getIntInRange("router.connections.warm.up.percentage.per.port", 25, 0, 100);
    routerConnectionsWarmUpTimeoutMs =
        verifiableProperties.getIntInRange("router.connections.warm.up.timeout.ms", 5000, 0, Integer.MAX_VALUE);
    routerConnectionCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("router.connection.checkout.timeout.ms", 1000, 1, 5000);
    routerRequestTimeoutMs = verifiableProperties.getIntInRange("router.request.timeout.ms", 2000, 1, 10000);
    routerMaxPutChunkSizeBytes =
        verifiableProperties.getIntInRange("router.max.put.chunk.size.bytes", 4 * 1024 * 1024, 1, Integer.MAX_VALUE);
    routerPutRequestParallelism =
        verifiableProperties.getIntInRange("router.put.request.parallelism", 3, 1, Integer.MAX_VALUE);
    routerPutSuccessTarget = verifiableProperties.getIntInRange("router.put.success.target", 2, 1, Integer.MAX_VALUE);
    routerMaxSlippedPutAttempts =
        verifiableProperties.getIntInRange("router.max.slipped.put.attempts", 1, 0, Integer.MAX_VALUE);
    routerDeleteRequestParallelism =
        verifiableProperties.getIntInRange("router.delete.request.parallelism", 3, 1, Integer.MAX_VALUE);
    routerDeleteSuccessTarget =
        verifiableProperties.getIntInRange("router.delete.success.target", 2, 1, Integer.MAX_VALUE);
    routerGetRequestParallelism =
        verifiableProperties.getIntInRange("router.get.request.parallelism", 2, 1, Integer.MAX_VALUE);
    routerGetSuccessTarget = verifiableProperties.getIntInRange("router.get.success.target", 1, 1, Integer.MAX_VALUE);
    routerGetCrossDcEnabled = verifiableProperties.getBoolean("router.get.cross.dc.enabled", true);
    routerGetIncludeNonOriginatingDcReplicas =
        verifiableProperties.getBoolean("router.get.include.non.originating.dc.replicas", true);
    routerGetReplicasRequired =
        verifiableProperties.getIntInRange("router.get.replicas.required", 6, 1, Integer.MAX_VALUE);
    routerGetOperationTrackerType =
        verifiableProperties.getString("router.get.operation.tracker.type", "SimpleOperationTracker");
    routerLatencyToleranceQuantile =
        verifiableProperties.getDoubleInRange("router.latency.tolerance.quantile", 0.9, 0.0, 1.0);
    routerBlobidCurrentVersion =
        verifiableProperties.getShortFromAllowedValues("router.blobid.current.version", (short) 6,
            new Short[]{1, 2, 3, 4, 5, 6});
    routerKeyManagementServiceFactory = verifiableProperties.getString("router.key.management.service.factory",
        "com.github.ambry.router.SingleKeyManagementServiceFactory");
    routerCryptoServiceFactory = verifiableProperties.getString("router.crypto.service.factory",
        "com.github.ambry.router.GCMCryptoServiceFactory");
    routerCryptoJobsWorkerCount =
        verifiableProperties.getIntInRange("router.crypto.jobs.worker.count", 1, 1, Integer.MAX_VALUE);
    routerTtlUpdateRequestParallelism =
        verifiableProperties.getIntInRange("router.ttl.update.request.parallelism", 3, 1, Integer.MAX_VALUE);
    routerTtlUpdateSuccessTarget =
        verifiableProperties.getIntInRange("router.ttl.update.success.target", 2, 1, Integer.MAX_VALUE);
  }
}
