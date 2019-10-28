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

import com.github.ambry.router.OperationTrackerScope;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Configuration parameters required by a {@link com.github.ambry.router.Router}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for a Router and presents them for retrieval through defined APIs.
 */
public class RouterConfig {

  public static final String DEFAULT_KMS_FACTORY = "com.github.ambry.router.SingleKeyManagementServiceFactory";
  public static final String DEFAULT_CRYPTO_SERVICE_FACTORY = "com.github.ambry.router.GCMCryptoServiceFactory";

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
   * {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortPlainText} to warm up for data nodes in the local
   * datacenter during startup.
   * {@link RouterConfig#routerConnectionsWarmUpTimeoutMs} may need to be adjusted.
   */
  @Config("router.connections.local.dc.warm.up.percentage")
  @Default("25")
  public final int routerConnectionsLocalDcWarmUpPercentage;

  /**
   * The percentage of {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortSsl} or
   * {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortPlainText} to warm up for data nodes in remote
   * datacenters during startup.
   * {@link RouterConfig#routerConnectionsWarmUpTimeoutMs} may need to be adjusted.
   */
  @Config("router.connections.remote.dc.warm.up.percentage")
  @Default("0")
  public final int routerConnectionsRemoteDcWarmUpPercentage;
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
   * {@code true} if the router should tell the network layer about requests that have timed out. The network client
   * can choose how to drop these requests.
   */
  @Config("router.drop.request.on.timeout")
  @Default("false")
  public final boolean routerDropRequestOnTimeout;

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
   * The version to use for new metadata blobs.
   */
  @Config("router.metadata.content.version")
  @Default("2")
  public final short routerMetadataContentVersion;

  /**
   * The KeyManagementServiceFactory that will be used to fetch {@link com.github.ambry.router.KeyManagementService}
   */
  @Config("router.key.management.service.factory")
  @Default(DEFAULT_KMS_FACTORY)
  public final String routerKeyManagementServiceFactory;

  /**
   * The CryptoServiceFactory that will be used to fetch {@link com.github.ambry.router.CryptoService}
   */
  @Config("router.crypto.service.factory")
  @Default(DEFAULT_CRYPTO_SERVICE_FACTORY)
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
   * If this config is set to {@code true} the router will use {@code GetBlobOperation} instead of
   * {@code GetBlobInfoOperation} for {@code getBlobInfo} calls. This allows the router to correct some blob size
   * corruptions that may have arisen from using older versions of {@code BlobIdTransformer} with the downside of
   * requiring more data to be fetched from storage. For most ambry deployments this is not necessary.
   */
  @Config("router.use.get.blob.operation.for.blob.info")
  @Default("false")
  public final boolean routerUseGetBlobOperationForBlobInfo;

  /**
   * The custom percentiles of Histogram in operation tracker to be reported. This allows router to emit metrics of
   * arbitrary percentiles (i.e. 97th, 93th etc). An example of this config is "0.91,0.93,0.97"(comma separated), each
   * value should fall in {@code [0..1]}.
   */
  @Config("router.operation.tracker.custom.percentiles")
  @Default("")
  public final List<Double> routerOperationTrackerCustomPercentiles;

  /**
   * The metric scope that is applied to operation tracker. This config specifies at which granularity router should
   * track the latency distribution. For example, Datacenter or Partition. The valid scope is defined in
   * {@link OperationTrackerScope}
   */
  @Config("router.operation.tracker.metric.scope")
  @Default("Datacenter")
  public final OperationTrackerScope routerOperationTrackerMetricScope;

  /**
   * The maximum size of histogram reservoir in operation tracker. This configs specifies the max number of data points
   * that can be kept by histogram reservoir.
   */
  @Config("router.operation.tracker.reservoir.size")
  @Default("1028")
  public final int routerOperationTrackerReservoirSize;

  /**
   * The decay factor of histogram reservoir in operation tracker. This config specifies how biased histogram should be
   * on new data.
   */
  @Config("router.operation.tracker.reservoir.decay.factor")
  @Default("0.015")
  public final double routerOperationTrackerReservoirDecayFactor;

  /**
   * The minimum required data points to populate histogram in operation tracker. If number of data points is less than
   * this threshold, the tracker ignores statistics from histogram.
   */
  @Config("router.operation.tracker.min.data.points.required")
  @Default("1000")
  public final long routerOperationTrackerMinDataPointsRequired;

  /**
   * If this config is set to {@code true} the operation tracker would terminate operations when there are more than 2
   * NOT_FOUND responses returned from originating dc. Notice that some of the blob ids don't have the datacenter id, it
   * will have no effect on those blobs.
   */
  @Config("router.operation.tracker.terminate.on.not.found.enabled")
  @Default("false")
  public final boolean routerOperationTrackerTerminateOnNotFoundEnabled;

  /**
   * The maximum number of inflight requests that allowed for adaptive tracker. If current number of inflight requests
   * is larger than or equal to this threshold, tracker shouldn't send out any request even though the oldest is past due.
   * {@link RouterConfig#routerGetRequestParallelism} is a suggestive number that operation tracker uses to determine how
   * many requests can be outstanding in parallel (assuming request gets response in time). Adaptive tracker is allowed
   * to issue more requests (total inflight requests may exceed #routerGetRequestParallelism) if old request is past due.
   * {@link RouterConfig#routerOperationTrackerMaxInflightRequests} is the strict upper bound that at any point of time,
   * number of inflight requests issued by adaptive tracker should not exceed this number. Hence, for adaptive tracker,
   * inflight requests number should always be within [0, #routerOperationTrackerMaxInflightRequests]
   */
  @Config("router.operation.tracker.max.inflight.requests")
  @Default("2")
  public final int routerOperationTrackerMaxInflightRequests;

  /**
   * Indicates whether to enable excluding timed out requests in Histogram reservoir.
   */
  @Config("router.operation.tracker.exclude.timeout.enabled")
  @Default("false")
  public final boolean routerOperationTrackerExcludeTimeoutEnabled;

  /**
   * Indicates whether to dump resource-level histogram to log file.
   */
  @Config("router.operation.tracker.histogram.dump.enabled")
  @Default("false")
  public final boolean routerOperationTrackerHistogramDumpEnabled;

  /**
   * The period of dumping resource-level histogram (if enabled).
   */
  @Config("router.operation.tracker.histogram.dump.period")
  @Default("600")
  public final long routerOperationTrackerHistogramDumpPeriod;

  /**
   * The max number of chunks per PutOperation that may be buffered in memory.
   */
  @Config("router.max.in.mem.put.chunks")
  @Default("4")
  public final int routerMaxInMemPutChunks;

  /**
   * The max number of chunks per GetBlobOperation that may be buffered in memory.
   */
  @Config("router.max.in.mem.get.chunks")
  @Default("4")
  public final int routerMaxInMemGetChunks;

  @Config("router.get.blob.operation.share.memory")
  @Default("false")
  public final boolean routerGetBlobOperationShareMemory;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerScalingUnitCount = verifiableProperties.getIntInRange("router.scaling.unit.count", 1, 1, Integer.MAX_VALUE);
    routerHostname = verifiableProperties.getString("router.hostname");
    routerDatacenterName = verifiableProperties.getString("router.datacenter.name");
    routerScalingUnitMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.plain.text", 5, 1, 100);
    routerScalingUnitMaxConnectionsPerPortSsl =
        verifiableProperties.getIntInRange("router.scaling.unit.max.connections.per.port.ssl", 2, 1, 100);
    routerConnectionsLocalDcWarmUpPercentage =
        verifiableProperties.getIntInRange("router.connections.local.dc.warm.up.percentage", 25, 0, 100);
    routerConnectionsRemoteDcWarmUpPercentage =
        verifiableProperties.getIntInRange("router.connections.remote.dc.warm.up.percentage", 0, 0, 100);
    routerConnectionsWarmUpTimeoutMs =
        verifiableProperties.getIntInRange("router.connections.warm.up.timeout.ms", 5000, 0, Integer.MAX_VALUE);
    routerConnectionCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("router.connection.checkout.timeout.ms", 1000, 1, 5000);
    routerRequestTimeoutMs = verifiableProperties.getIntInRange("router.request.timeout.ms", 2000, 1, 10000);
    routerDropRequestOnTimeout = verifiableProperties.getBoolean("router.drop.request.on.timeout", false);
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
    routerMetadataContentVersion =
        verifiableProperties.getShortFromAllowedValues("router.metadata.content.version", (short) 2, new Short[]{2, 3});
    routerKeyManagementServiceFactory =
        verifiableProperties.getString("router.key.management.service.factory", DEFAULT_KMS_FACTORY);
    routerCryptoServiceFactory =
        verifiableProperties.getString("router.crypto.service.factory", DEFAULT_CRYPTO_SERVICE_FACTORY);
    routerCryptoJobsWorkerCount =
        verifiableProperties.getIntInRange("router.crypto.jobs.worker.count", 1, 1, Integer.MAX_VALUE);
    routerTtlUpdateRequestParallelism =
        verifiableProperties.getIntInRange("router.ttl.update.request.parallelism", 3, 1, Integer.MAX_VALUE);
    routerTtlUpdateSuccessTarget =
        verifiableProperties.getIntInRange("router.ttl.update.success.target", 2, 1, Integer.MAX_VALUE);
    routerUseGetBlobOperationForBlobInfo =
        verifiableProperties.getBoolean("router.use.get.blob.operation.for.blob.info", false);
    List<String> customPercentiles =
        Utils.splitString(verifiableProperties.getString("router.operation.tracker.custom.percentiles", ""), ",");
    routerOperationTrackerCustomPercentiles =
        Collections.unmodifiableList(customPercentiles.stream().map(Double::valueOf).collect(Collectors.toList()));
    String scopeStr = verifiableProperties.getString("router.operation.tracker.metric.scope", "Datacenter");
    routerOperationTrackerMetricScope = OperationTrackerScope.valueOf(scopeStr);
    routerOperationTrackerReservoirSize =
        verifiableProperties.getIntInRange("router.operation.tracker.reservoir.size", 1028, 0, Integer.MAX_VALUE);
    routerOperationTrackerReservoirDecayFactor =
        verifiableProperties.getDouble("router.operation.tracker.reservoir.decay.factor", 0.015);
    routerOperationTrackerMinDataPointsRequired =
        verifiableProperties.getLong("router.operation.tracker.min.data.points.required", 1000L);
    routerOperationTrackerMaxInflightRequests =
        verifiableProperties.getIntInRange("router.operation.tracker.max.inflight.requests", 2, 1, Integer.MAX_VALUE);
    routerOperationTrackerExcludeTimeoutEnabled =
        verifiableProperties.getBoolean("router.operation.tracker.exclude.timeout.enabled", false);
    routerOperationTrackerHistogramDumpEnabled =
        verifiableProperties.getBoolean("router.operation.tracker.histogram.dump.enabled", false);
    routerOperationTrackerHistogramDumpPeriod =
        verifiableProperties.getLongInRange("router.operation.tracker.histogram.dump.period", 600L, 1L, Long.MAX_VALUE);
    if (routerGetRequestParallelism > routerOperationTrackerMaxInflightRequests) {
      throw new IllegalArgumentException(
          "Operation tracker parallelism is larger than operation tracker max inflight number");
    }
    routerOperationTrackerTerminateOnNotFoundEnabled =
        verifiableProperties.getBoolean("router.operation.tracker.terminate.on.not.found.enabled", false);
    routerMaxInMemPutChunks = verifiableProperties.getIntInRange("router.max.in.mem.put.chunks", 4, 1,
        Integer.MAX_VALUE / routerMaxPutChunkSizeBytes);
    routerMaxInMemGetChunks = verifiableProperties.getIntInRange("router.max.in.mem.get.chunks", 4, 1,
        Integer.MAX_VALUE / routerMaxPutChunkSizeBytes);
    routerGetBlobOperationShareMemory =
        verifiableProperties.getBoolean("router.get.blob.operation.share.memory", false);
  }
}
