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
  public static final double DEFAULT_LATENCY_TOLERANCE_QUANTILE = 0.9;
  public static final long DEFAULT_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS = 1000L;
  public static final long ROUTER_NOT_FOUND_CACHE_MAX_TTL_IN_MS = 24 * 60 * 1000L;
  public static final int MAX_NETWORK_TIMEOUT_VALUE_FOR_A_REQUEST_IN_MS = 60 * 1000;
  public static final long DEFAULT_ROUTER_UPDATE_OP_METADATA_RELIANCE_TIMESTAMP_IN_MS = Long.MAX_VALUE;
  // This is a theoretical maximum value. Configured value may be much smaller since we might need to respond back to
  // client with either success or failure much sooner.
  public static final int MAX_OVERALL_TIMEOUT_VALUE_FOR_A_REQUEST_IN_MS = 60 * 60 * 1000;
  // By default a get request should prioritize remote replicas only if there are no local replicas available.
  public static final int DEFAULT_ROUTER_GET_OPERATION_MIN_LOCAL_REPLICA_COUNT_TO_PRIORITIZE_LOCAL = 0;

  // config keys
  public static final String ROUTER_SCALING_UNIT_COUNT = "router.scaling.unit.count";
  public static final String ROUTER_HOSTNAME = "router.hostname";
  public static final String ROUTER_DATACENTER_NAME = "router.datacenter.name";
  public static final String ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_PLAIN_TEXT =
      "router.scaling.unit.max.connections.per.port.plain.text";
  public static final String ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_SSL =
      "router.scaling.unit.max.connections.per.port.ssl";
  public static final String ROUTER_CONNECTIONS_LOCAL_DC_WARM_UP_PERCENTAGE =
      "router.connections.local.dc.warm.up.percentage";
  public static final String ROUTER_CONNECTIONS_REMOTE_DC_WARM_UP_PERCENTAGE =
      "router.connections.remote.dc.warm.up.percentage";
  public static final String ROUTER_CONNECTIONS_WARM_UP_TIMEOUT_MS = "router.connections.warm.up.timeout.ms";
  public static final String ROUTER_CONNECTION_CHECKOUT_TIMEOUT_MS = "router.connection.checkout.timeout.ms";
  public static final String ROUTER_REQUEST_TIMEOUT_MS = "router.request.timeout.ms";
  public static final String ROUTER_REQUEST_NETWORK_TIMEOUT_MS = "router.request.network.timeout.ms";
  public static final String ROUTER_DROP_REQUEST_ON_TIMEOUT = "router.drop.request.on.timeout";
  public static final String ROUTER_MAX_PUT_CHUNK_SIZE_BYTES = "router.max.put.chunk.size.bytes";
  public static final String ROUTER_PUT_REQUEST_PARALLELISM = "router.put.request.parallelism";
  public static final String ROUTER_PUT_LOCAL_REQUEST_PARALLELISM = "router.put.local.request.parallelism";
  public static final String ROUTER_PUT_REMOTE_REQUEST_PARALLELISM = "router.put.remote.request.parallelism";
  public static final String ROUTER_PUT_SUCCESS_TARGET = "router.put.success.target";
  public static final String ROUTER_PUT_REMOTE_SUCCESS_TARGET = "router.put.remote.success.target";
  public static final String ROUTER_REPLICATE_BLOB_REQUEST_PARALLELISM = "router.replicate.blob.request.parallelism";
  public static final String ROUTER_REPLICATE_BLOB_SUCCESS_TARGET = "router.replicate.blob.success.target";
  public static final String ROUTER_MAX_SLIPPED_PUT_ATTEMPTS = "router.max.slipped.put.attempts";
  public static final String ROUTER_DELETE_REQUEST_PARALLELISM = "router.delete.request.parallelism";
  public static final String ROUTER_DELETE_SUCCESS_TARGET = "router.delete.success.target";
  public static final String ROUTER_GET_REQUEST_PARALLELISM = "router.get.request.parallelism";
  public static final String ROUTER_GET_SUCCESS_TARGET = "router.get.success.target";
  public static final String ROUTER_GET_CROSS_DC_ENABLED = "router.get.cross.dc.enabled";
  public static final String ROUTER_OPERATION_TRACKER_INCLUDE_DOWN_REPLICAS =
      "router.operation.tracker.include.down.replicas";
  public static final String ROUTER_GET_OPERATION_TRACKER_TYPE = "router.get.operation.tracker.type";
  public static final String ROUTER_PUT_OPERATION_TRACKER_TYPE = "router.put.operation.tracker.type";
  public static final String ROUTER_LATENCY_TOLERANCE_QUANTILE = "router.latency.tolerance.quantile";
  public static final String ROUTER_BLOBID_CURRENT_VERSION = "router.blobid.current.version";
  public static final String ROUTER_METADATA_CONTENT_VERSION = "router.metadata.content.version";
  public static final String ROUTER_KEY_MANAGEMENT_SERVICE_FACTORY = "router.key.management.service.factory";
  public static final String ROUTER_CRYPTO_SERVICE_FACTORY = "router.crypto.service.factory";
  public static final String ROUTER_CRYPTO_JOBS_WORKER_COUNT = "router.crypto.jobs.worker.count";
  public static final String ROUTER_TTL_UPDATE_REQUEST_PARALLELISM = "router.ttl.update.request.parallelism";
  public static final String ROUTER_TTL_UPDATE_SUCCESS_TARGET = "router.ttl.update.success.target";
  public static final String ROUTER_UNDELETE_REQUEST_PARALLELISM = "router.undelete.request.parallelism";
  public static final String ROUTER_UNDELETE_OPERATION_TRACKER_TYPE = "router.undelete.operation.tracker.type";
  public static final String ROUTER_UNDELETE_SUCCESS_TARGET = "router.undelete.success.target";
  public static final String ROUTER_USE_GET_BLOB_OPERATION_FOR_BLOB_INFO =
      "router.use.get.blob.operation.for.blob.info";
  public static final String ROUTER_OPERATION_TRACKER_CUSTOM_PERCENTILES =
      "router.operation.tracker.custom.percentiles";
  public static final String ROUTER_OPERATION_TRACKER_METRIC_SCOPE = "router.operation.tracker.metric.scope";
  public static final String ROUTER_OPERATION_TRACKER_RESERVOIR_SIZE = "router.operation.tracker.reservoir.size";
  public static final String ROUTER_OPERATION_TRACKER_RESERVOIR_DECAY_FACTOR =
      "router.operation.tracker.reservoir.decay.factor";
  public static final String ROUTER_OPERATION_TRACKER_MIN_DATA_POINTS_REQUIRED =
      "router.operation.tracker.min.data.points.required";
  public static final String ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED =
      "router.operation.tracker.terminate.on.not.found.enabled";
  public static final String ROUTER_OPERATION_TRACKER_MAX_INFLIGHT_REQUESTS =
      "router.operation.tracker.max.inflight.requests";
  public static final String ROUTER_ADAPTIVE_OPERATION_TRACKER_WAITING_FOR_RESPONSE =
      "router.adaptive.operation.tracker.waiting.for.response";
  public static final String ROUTER_OPERATION_TRACKER_EXCLUDE_TIMEOUT_ENABLED =
      "router.operation.tracker.exclude.timeout.enabled";
  public static final String ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_ENABLED =
      "router.operation.tracker.histogram.dump.enabled";
  public static final String ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_PERIOD =
      "router.operation.tracker.histogram.dump.period";
  public static final String ROUTER_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS =
      "router.operation.tracker.histogram.cache.timeout.ms";
  public static final String ROUTER_MAX_IN_MEM_PUT_CHUNKS = "router.max.in.mem.put.chunks";
  public static final String ROUTER_MAX_IN_MEM_GET_CHUNKS = "router.max.in.mem.get.chunks";
  public static final String ROUTER_GET_ELIGIBLE_REPLICAS_BY_STATE_ENABLED =
      "router.get.eligible.replicas.by.state.enabled";
  public static final String ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET = "router.put.use.dynamic.success.target";
  public static final String ROUTER_CLOUD_SUCCESS_TARGET = "router.cloud.success.target";
  public static final String ROUTER_CLOUD_REQUEST_PARALLELISM = "router.cloud.request.parallelism";
  public static final String ROUTER_ENABLE_HTTP2_NETWORK_CLIENT = "router.enable.http2.network.client";
  public static final String ROUTER_CROSS_COLO_REQUEST_TO_DC_WITH_MOST_REPLICAS =
      "router.cross.colo.request.to.dc.with.most.replicas";
  public static final String ROUTER_BACKGROUND_DELETER_MAX_CONCURRENT_OPERATIONS =
      "router.background.deleter.max.concurrent.operations";
  public static final String OPERATION_CONTROLLER = "router.operation.controller";
  public static final String ROUTER_REQUEST_HANDLER_NUM_OF_THREADS = "router.request.handler.num.of.threads";
  public static final String ROUTER_STORE_KEY_CONVERTER_FACTORY = "router.store.key.converter.factory";
  public static final String ROUTER_UNAVAILABLE_DUE_TO_OFFLINE_REPLICAS = "router.unavailable.due.to.offline.replicas";
  public static final String ROUTER_NOT_FOUND_CACHE_TTL_IN_MS = "router.not.found.cache.ttl.in.ms";
  public static final String ROUTER_UPDATE_OP_METADATA_RELIANCE_TIMESTAMP_IN_MS =
      "router.update.op.metadata.reliance.timestamp.in.ms";
  public static final String ROUTER_UNAVAILABLE_DUE_TO_SUCCESS_COUNT_IS_NON_ZERO_FOR_DELETE =
      "router.unavailable.due.to.success.count.is.non.zero.for.delete";
  // repair the blob with the on-demand replication on ttlupdate
  public static final String ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_TTLUPDATE_ENABLED =
      "router.repair.with.replicate.blob.enabled";
  // repair the blob with the on-demand replication on deletion
  public static final String ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_DELETE_ENABLED =
      "router.repair.with.replicate.blob.on.delete.enabled";
  // offline repair partially failed ttl update
  public static final String ROUTER_TTLUPDATE_OFFLINE_REPAIR_ENABLED = "router.ttlupdate.offline.repair.enabled";
  // offline repair partially failed delete request
  public static final String ROUTER_DELETE_OFFLINE_REPAIR_ENABLED = "router.delete.offline.repair.enabled";
  // offline repair db factory
  public static final String ROUTER_REPAIR_REQUESTS_DB_FACTORY = "router.repair.requests.db.factory";
  public static final String ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND =
      "router.operation.tracker.check.all.originating.replicas.for.not.found";
  public static final String RESERVED_METADATA_ENABLED = "router.reserved.metadata.enabled";
  public static final String ROUTER_GET_OPERATION_DEPRIORITIZE_BOOTSTRAP_REPLICAS =
      "router.get.operation.deprioritize.bootstrap.replicas";

  // minimum number of local replicas that should be live for a get request so that local replicas are prioritized.
  public static final String ROUTER_GET_OPERATION_MIN_LOCAL_REPLICA_COUNT_TO_PRIORITIZE_LOCAL =
      "router.get.operation.min.local.replica.count.to.prioritize.local";

  /**
   * Number of independent scaling units for the router.
   */
  @Config(ROUTER_SCALING_UNIT_COUNT)
  @Default("1")
  public final int routerScalingUnitCount;

  /**
   * The hostname of the node upon which the router runs.
   */
  @Config(ROUTER_HOSTNAME)
  public final String routerHostname;

  /**
   * The name of the datacenter in which the router is located.
   */
  @Config(ROUTER_DATACENTER_NAME)
  public final String routerDatacenterName;

  /**
   * The max connections allowed per (datanode, port) for plain text
   */
  @Config(ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_PLAIN_TEXT)
  @Default("5")
  public final int routerScalingUnitMaxConnectionsPerPortPlainText;

  /**
   * The max connections allowed per (datanode, port) for ssl
   */
  @Config(ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_SSL)
  @Default("2")
  public final int routerScalingUnitMaxConnectionsPerPortSsl;

  /**
   * The percentage of {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortSsl} or
   * {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortPlainText} to warm up for data nodes in the local
   * datacenter during startup.
   * {@link RouterConfig#routerConnectionsWarmUpTimeoutMs} may need to be adjusted.
   */
  @Config(ROUTER_CONNECTIONS_LOCAL_DC_WARM_UP_PERCENTAGE)
  @Default("25")
  public final int routerConnectionsLocalDcWarmUpPercentage;

  /**
   * The percentage of {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortSsl} or
   * {@link RouterConfig#routerScalingUnitMaxConnectionsPerPortPlainText} to warm up for data nodes in remote
   * datacenters during startup.
   * {@link RouterConfig#routerConnectionsWarmUpTimeoutMs} may need to be adjusted.
   */
  @Config(ROUTER_CONNECTIONS_REMOTE_DC_WARM_UP_PERCENTAGE)
  @Default("0")
  public final int routerConnectionsRemoteDcWarmUpPercentage;
  /**
   * The max time allowed to establish connections to local DC in the startup
   */
  @Config(ROUTER_CONNECTIONS_WARM_UP_TIMEOUT_MS)
  @Default("5000")
  public final int routerConnectionsWarmUpTimeoutMs;

  /**
   * Timeout for checking out an available connection to a (datanode, port).
   */
  @Config(ROUTER_CONNECTION_CHECKOUT_TIMEOUT_MS)
  @Default("1000")
  public final int routerConnectionCheckoutTimeoutMs;

  /**
   * Timeout for requests waiting at the router layer.
   */
  @Config(ROUTER_REQUEST_TIMEOUT_MS)
  @Default("20000")
  public final int routerRequestTimeoutMs;

  /**
   * Timeout for requests waiting at the network layer.
   */
  @Config(ROUTER_REQUEST_NETWORK_TIMEOUT_MS)
  @Default("10000")
  public final int routerRequestNetworkTimeoutMs;

  /**
   * {@code true} if the router should tell the network layer about requests that have timed out. The network client
   * can choose how to drop these requests.
   */
  @Config(ROUTER_DROP_REQUEST_ON_TIMEOUT)
  @Default("false")
  public final boolean routerDropRequestOnTimeout;

  /**
   * The max chunk size to be used for put operations.
   */
  @Config(ROUTER_MAX_PUT_CHUNK_SIZE_BYTES)
  @Default("4*1024*1024")
  public final int routerMaxPutChunkSizeBytes;

  /**
   * The maximum number of parallel requests issued at a time by the put manager for a chunk.
   */
  @Config(ROUTER_PUT_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerPutRequestParallelism;

  /**
   * The maximum number of parallel requests issued at a time by the put manager for a chunk.
   */
  @Config(ROUTER_PUT_LOCAL_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerPutLocalRequestParallelism;

  /**
   * The maximum number of parallel requests issued at a time by the put manager for a chunk.
   */
  @Config(ROUTER_PUT_REMOTE_REQUEST_PARALLELISM)
  @Default("0")
  public final int routerPutRemoteRequestParallelism;

  /**
   * The minimum number of successful responses required for a put operation.
   */
  @Config(ROUTER_PUT_SUCCESS_TARGET)
  @Default("2")
  public final int routerPutSuccessTarget;

  @Config(ROUTER_PUT_REMOTE_SUCCESS_TARGET)
  @Default("2")
  public final int routerPutRemoteSuccessTarget;

  /**
   * The maximum number of parallel requests issued at a time by the ReplicateBlob manager.
   */
  @Config(ROUTER_REPLICATE_BLOB_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerReplicateBlobRequestParallelism;

  /**
   * The minimum number of successful responses required for a ReplicateBlob operation.
   */
  @Config(ROUTER_REPLICATE_BLOB_SUCCESS_TARGET)
  @Default("2")
  public final int routerReplicateBlobSuccessTarget;

  /**
   * The maximum number of times to retry putting any chunk of a put operation
   */
  @Config(ROUTER_MAX_SLIPPED_PUT_ATTEMPTS)
  @Default("1")
  public final int routerMaxSlippedPutAttempts;

  /**
   * The maximum number of parallel requests allowed for a delete operation.
   */
  @Config(ROUTER_DELETE_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerDeleteRequestParallelism;

  /**
   * The minimum number of successful responses required for a delete operation.
   */
  @Config(ROUTER_DELETE_SUCCESS_TARGET)
  @Default("2")
  public final int routerDeleteSuccessTarget;

  /**
   * The maximum number of parallel requests issued at a time by the get manager for a get operation on a chunk.
   */
  @Config(ROUTER_GET_REQUEST_PARALLELISM)
  @Default("2")
  public final int routerGetRequestParallelism;

  /**
   * The minimum number of successful responses required for a get operation on a chunk.
   */
  @Config(ROUTER_GET_SUCCESS_TARGET)
  @Default("1")
  public final int routerGetSuccessTarget;

  /**
   * Indicates whether get operations are allowed to make requests to nodes in remote data centers.
   */
  @Config(ROUTER_GET_CROSS_DC_ENABLED)
  @Default("true")
  public final boolean routerGetCrossDcEnabled;

  /**
   * Whether to include down(offline) replicas in replicas pool within operation tracker.
   */
  @Config(ROUTER_OPERATION_TRACKER_INCLUDE_DOWN_REPLICAS)
  @Default("true")
  public final boolean routerOperationTrackerIncludeDownReplicas;

  /**
   * The OperationTracker to use for GET operations.
   */
  @Config(ROUTER_GET_OPERATION_TRACKER_TYPE)
  @Default("SimpleOperationTracker")
  public final String routerGetOperationTrackerType;

  /**
   * The OperationTracker to use for PUT operations.
   */
  @Config(ROUTER_PUT_OPERATION_TRACKER_TYPE)
  @Default("SimpleOperationTracker")
  public final String routerPutOperationTrackerType;

  /**
   * If an adaptive operation tracker is being used, a request is discounted from the parallelism count if it has been
   * outstanding for more than the quantile defined here (compared to latencies of other requests of the same class).
   */
  @Config(ROUTER_LATENCY_TOLERANCE_QUANTILE)
  @Default("0.9")
  public final double routerLatencyToleranceQuantile;

  /**
   * The version to use for new BlobIds.
   */
  @Config(ROUTER_BLOBID_CURRENT_VERSION)
  @Default("5")
  public final short routerBlobidCurrentVersion;

  /**
   * The version to use for new metadata blobs.
   */
  @Config(ROUTER_METADATA_CONTENT_VERSION)
  @Default("2")
  public final short routerMetadataContentVersion;

  /**
   * The KeyManagementServiceFactory that will be used to fetch {@link com.github.ambry.router.KeyManagementService}
   */
  @Config(ROUTER_KEY_MANAGEMENT_SERVICE_FACTORY)
  @Default(DEFAULT_KMS_FACTORY)
  public final String routerKeyManagementServiceFactory;

  /**
   * The CryptoServiceFactory that will be used to fetch {@link com.github.ambry.router.CryptoService}
   */
  @Config(ROUTER_CRYPTO_SERVICE_FACTORY)
  @Default(DEFAULT_CRYPTO_SERVICE_FACTORY)
  public final String routerCryptoServiceFactory;

  /**
   * Number of crypto jobs worker count
   */
  @Config(ROUTER_CRYPTO_JOBS_WORKER_COUNT)
  @Default("1")
  public final int routerCryptoJobsWorkerCount;

  /**
   * The maximum number of parallel requests issued at a time by the TTL update manager for a chunk.
   */
  @Config(ROUTER_TTL_UPDATE_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerTtlUpdateRequestParallelism;

  /**
   * The minimum number of successful responses required for a TTL update operation.
   */
  @Config(ROUTER_TTL_UPDATE_SUCCESS_TARGET)
  @Default("2")
  public final int routerTtlUpdateSuccessTarget;

  /**
   * The maximum number of parallel requests issued at a time by the undelete manager for a blob.
   */
  @Config(ROUTER_UNDELETE_REQUEST_PARALLELISM)
  @Default("3")
  public final int routerUndeleteRequestParallelism;

  /**
   * The OperationTracker to use for UNDELETE operations.
   */
  @Config(ROUTER_UNDELETE_OPERATION_TRACKER_TYPE)
  @Default("UndeleteOperationTracker")
  public final String routerUndeleteOperationTrackerType;

  /**
   * The minimum number of successful responses required for a undelete operation.
   */
  @Config(ROUTER_UNDELETE_SUCCESS_TARGET)
  @Default("2")
  public final int routerUndeleteSuccessTarget;

  /**
   * If this config is set to {@code true} the router will use {@code GetBlobOperation} instead of
   * {@code GetBlobInfoOperation} for {@code getBlobInfo} calls. This allows the router to correct some blob size
   * corruptions that may have arisen from using older versions of {@code BlobIdTransformer} with the downside of
   * requiring more data to be fetched from storage. For most ambry deployments this is not necessary.
   */
  @Config(ROUTER_USE_GET_BLOB_OPERATION_FOR_BLOB_INFO)
  @Default("false")
  public final boolean routerUseGetBlobOperationForBlobInfo;

  /**
   * The custom percentiles of Histogram in operation tracker to be reported. This allows router to emit metrics of
   * arbitrary percentiles (i.e. 97th, 93th etc). An example of this config is "0.91,0.93,0.97"(comma separated), each
   * value should fall in {@code [0..1]}.
   */
  @Config(ROUTER_OPERATION_TRACKER_CUSTOM_PERCENTILES)
  @Default("")
  public final List<Double> routerOperationTrackerCustomPercentiles;

  /**
   * The metric scope that is applied to operation tracker. This config specifies at which granularity router should
   * track the latency distribution. For example, Datacenter or Partition. The valid scope is defined in
   * {@link OperationTrackerScope}
   */
  @Config(ROUTER_OPERATION_TRACKER_METRIC_SCOPE)
  @Default("Datacenter")
  public final OperationTrackerScope routerOperationTrackerMetricScope;

  /**
   * The maximum size of histogram reservoir in operation tracker. This configs specifies the max number of data points
   * that can be kept by histogram reservoir.
   */
  @Config(ROUTER_OPERATION_TRACKER_RESERVOIR_SIZE)
  @Default("1028")
  public final int routerOperationTrackerReservoirSize;

  /**
   * The decay factor of histogram reservoir in operation tracker. This config specifies how biased histogram should be
   * on new data.
   */
  @Config(ROUTER_OPERATION_TRACKER_RESERVOIR_DECAY_FACTOR)
  @Default("0.015")
  public final double routerOperationTrackerReservoirDecayFactor;

  /**
   * The minimum required data points to populate histogram in operation tracker. If number of data points is less than
   * this threshold, the tracker ignores statistics from histogram.
   */
  @Config(ROUTER_OPERATION_TRACKER_MIN_DATA_POINTS_REQUIRED)
  @Default("1000")
  public final long routerOperationTrackerMinDataPointsRequired;

  /**
   * If this config is set to {@code true} the operation tracker would terminate operations when there are more than 2
   * NOT_FOUND responses returned from originating dc. Notice that some of the blob ids don't have the datacenter id, it
   * will have no effect on those blobs.
   */
  @Config(ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED)
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
  @Config(ROUTER_OPERATION_TRACKER_MAX_INFLIGHT_REQUESTS)
  @Default("2")
  public final int routerOperationTrackerMaxInflightRequests;

  /**
   * True when the adaptive operation tracker would wait for all the responses coming back before sending out new requests
   * when there is no request exceeding the given percentile.
   */
  @Config(ROUTER_ADAPTIVE_OPERATION_TRACKER_WAITING_FOR_RESPONSE)
  @Default("false")
  public final boolean routerAdaptiveOperationTrackerWaitingForResponse;

  /**
   * Indicates whether to enable excluding timed out requests in Histogram reservoir.
   */
  @Config(ROUTER_OPERATION_TRACKER_EXCLUDE_TIMEOUT_ENABLED)
  @Default("false")
  public final boolean routerOperationTrackerExcludeTimeoutEnabled;

  /**
   * Indicates whether to dump resource-level histogram to log file.
   */
  @Config(ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_ENABLED)
  @Default("false")
  public final boolean routerOperationTrackerHistogramDumpEnabled;

  /**
   * The period of dumping resource-level histogram in seconds(if enabled).
   */
  @Config(ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_PERIOD)
  @Default("600")
  public final long routerOperationTrackerHistogramDumpPeriod;

  /**
   * The timeout in ms for a cached histogram value used by operation tracker. After this timeout expires, a new
   * histogram snapshot will be generated.
   */
  @Config(ROUTER_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS)
  @Default("1000")
  public final long routerOperationTrackerHistogramCacheTimeoutMs;

  /**
   * The max number of chunks per PutOperation that may be buffered in memory.
   */
  @Config(ROUTER_MAX_IN_MEM_PUT_CHUNKS)
  @Default("4")
  public final int routerMaxInMemPutChunks;

  /**
   * The max number of chunks per GetBlobOperation that may be buffered in memory.
   */
  @Config(ROUTER_MAX_IN_MEM_GET_CHUNKS)
  @Default("4")
  public final int routerMaxInMemGetChunks;

  /**
   * if {@code true}, operation tracker will get replicas in required states based on the type of operation. This helps
   * dynamically manage replicas in cluster (i.e. add/remove/move replicas) without restarting frontends.
   */
  @Config(ROUTER_GET_ELIGIBLE_REPLICAS_BY_STATE_ENABLED)
  @Default("false")
  public final boolean routerGetEligibleReplicasByStateEnabled;

  /**
   * Whether to use dynamic success target for put operation in router.
   */
  @Config(ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET)
  @Default("false")
  public final boolean routerPutUseDynamicSuccessTarget;

  /**
   * Whether or not to use HTTP/2 network client
   */
  @Config(ROUTER_ENABLE_HTTP2_NETWORK_CLIENT)
  @Default("false")
  public final boolean routerEnableHttp2NetworkClient;

  /**
   * When request didn't succeed in local dc, whether to let router send request to remote dc with most replicas first.
   * Currently, our logic is to try originating dc first. In some cases where originating dc is no longer available (i.e.
   * the dc has been decommissioned), this config allows router to choose a dc with most replicas in which it's more
   * likely to succeed.
   */
  @Config(ROUTER_CROSS_COLO_REQUEST_TO_DC_WITH_MOST_REPLICAS)
  @Default("false")
  public final boolean routerCrossColoRequestToDcWithMostReplicas;

  /**
   * The maximum number of outgoing delete operations in background deleter. 0 means no limit.
   */
  @Config(ROUTER_BACKGROUND_DELETER_MAX_CONCURRENT_OPERATIONS)
  @Default("0")
  public final int routerBackgroundDeleterMaxConcurrentOperations;

  /**
   * Name of the operation controller class to use.
   */
  @Config(OPERATION_CONTROLLER)
  @Default("com.github.ambry.router.OperationController")
  public final String operationController;

  /**
   * Expiration time for Blob IDs stored in not-found cache. Default value is 15 seconds.
   * Setting it to 0 would disable the cache and avoid storing any blob IDs.
   * TODO: With PR https://github.com/linkedin/ambry/pull/2072, when operation tracker fails due to blob-not-found and
   *  some of eligible replicas are offline during the time of operation, we differentiate it with unavailable error and
   *  and return 503 to client instead of 404. But we seem to do it only for 'ttl_update' & 'delete' but not for 'Get'.
   *  When this cache is introduced, it is possible that blobs are cached for not-found on 'Get' and that could interfere
   *  with above logic for 'TTL_Update' and 'Delete' as we would return 404 instead of 503. We might need to keep this
   *  cache disabled (by setting it to 0 in configs) until we fix to return 503 for 'Get' calls as well when replicas are
   *  offline.
   */
  @Config(ROUTER_NOT_FOUND_CACHE_TTL_IN_MS)
  @Default("15*1000")
  public final long routerNotFoundCacheTtlInMs;

  public static final String ROUTER_BLOB_METADATA_CACHE_ID = "router.blob.metadata.cache.id";
  @Config(ROUTER_BLOB_METADATA_CACHE_ID)
  public final String routerBlobMetadataCacheId;

  public static final String ROUTER_BLOB_METADATA_CACHE_ENABLED = "router.blob.metadata.cache.enabled";
  @Config(ROUTER_BLOB_METADATA_CACHE_ENABLED)
  public final boolean routerBlobMetadataCacheEnabled;

  public static final String ROUTER_SMALLEST_BLOB_FOR_METADATA_CACHE = "router.smallest.blob.for.metadata.cache";
  @Config(ROUTER_SMALLEST_BLOB_FOR_METADATA_CACHE)
  public final long routerSmallestBlobForMetadataCache;
  public static final long NUM_BYTES_IN_ONE_TB = (long) Math.pow(1024, 4);

  public static final String ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES = "router.max.num.metadata.cache.entries";
  @Config(ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES)
  public final int routerMaxNumMetadataCacheEntries;
  public static final int MAX_NUM_METADATA_CACHE_ENTRIES_DEFAULT = 10;

  /**
   * Blobs created after this timestamp can rely on metadata chunk to get the overall status of update operations
   * (ttl update, undelete) on the blob.
   * This config is expected to be a temporary stop gap to improve the resiliency of update operations on a blob, until
   * the server version changes are deployed to identify update operations that can rely on metadata chunk for the
   * update status of the entire blob.
   * The value of this config will be an upper bound of the timestamp when the frontend code version that updates
   * metadata chunk last has been deployed to all frontend hosts on a cluster.
   * A value of LONG.MAX_VALUE for this config would mean that this config is effectively disabled.
   */
  @Config(ROUTER_UPDATE_OP_METADATA_RELIANCE_TIMESTAMP_IN_MS)
  public final long routerUpdateOpMetadataRelianceTimestampInMs;

  /**
   * If this config is set to {@code true}, when ttlupdate fails because of not enough replicas having replicated the Blob,
   * the operation will trigger the on-demand replication to replicate the Blob to more replicas and then retry the request.
   */
  @Config(ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_TTLUPDATE_ENABLED)
  public final boolean routerRepairWithReplicateBlobOnTtlUpdateEnabled;

  /**
   * If this config is set to {@code true}, when deletion fails because of not enough replicas having replicated the Blob,
   * the operation will trigger the on-demand replication to replicate the Blob to more replicas and then retry the request.
   */
  @Config(ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_DELETE_ENABLED)
  public final boolean routerRepairWithReplicateBlobOnDeleteEnabled;

  /**
   * If this config is set to {@code true}, when ttlupdate is partially failed,
   * the operation will save it to the secondary storage and fix it in the background.
   */
  @Config(ROUTER_TTLUPDATE_OFFLINE_REPAIR_ENABLED)
  public final boolean routerTtlUpdateOfflineRepairEnabled;

  /**
   * If this config is set to {@code true}, when delete is partially failed,
   * the operation will save it to the secondary storage and fix it in the background.
   */
  @Config(ROUTER_DELETE_OFFLINE_REPAIR_ENABLED)
  public final boolean routerDeleteOfflineRepairEnabled;

  /**
   * Specify the RepairRequestsDBFactory we use for the background repair.
   */
  @Config(ROUTER_REPAIR_REQUESTS_DB_FACTORY)
  public final String routerRepairRequestsDbFactory;

  /**
   * The maximum duration in seconds to retry. If the get blob operation takes more than this duration, we would not retry.
   */
  @Config(ROUTER_GET_BLOB_RETRY_LIMIT_IN_SEC)
  public final int routerGetBlobRetryLimitInSec;
  public static final String ROUTER_GET_BLOB_RETRY_LIMIT_IN_SEC = "router.get.blob.retry.limit.in.sec";
  public static final int ROUTER_GET_BLOB_RETRY_LIMIT_IN_SEC_MAX = 300;

  /**
   * The maximum number of retries for get blob. The default value is 0, which means no retry for get blob.
   */
  @Config(ROUTER_GET_BLOB_RETRY_LIMIT_COUNT)
  public final int routerGetBlobRetryLimitCount;
  public static final String ROUTER_GET_BLOB_RETRY_LIMIT_COUNT = "router.get.blob.retry.limit.count";
  public static final int ROUTER_GET_BLOB_RETRY_LIMIT_COUNT_MAX = 100;

  /**
   * If this config is set to {@code true} the operation tracker would make sure all replicas in originating data center
   * are up and respond with BLOB_NOT_FOUND before concluding that blob is not present. Else, it would check in
   * total_originating_dc_replicas - put_success_target + 1 replicas.
   */
  @Config(ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND)
  @Default("true")
  public final boolean routerOperationTrackerCheckAllOriginatingReplicasForNotFound;

  @Config(ROUTER_OPERATION_TRACKER_REQUIRE_TWO_NOT_FOUND)
  @Default("false")
  public final boolean routerOperationTrackerRequireTwoNotFound;
  public static final String ROUTER_OPERATION_TRACKER_REQUIRE_TWO_NOT_FOUND =
      "router.operation.tracker.require.two.not.found";

  // Group compression-related configs in the CompressConfig class.
  private final CompressionConfig compressionConfig;

  /**
   * Feature flag to indicate if reserved metadata is enabled.
   */
  // TODO Efficient_Metadata_Operations_TODO: Remove this flag and its usages once the efficient metadata operations feature is complete.
  @Config(RESERVED_METADATA_ENABLED)
  @Default("false")
  public final boolean routerReservedMetadataEnabled;

  @Config(ROUTER_GET_OPERATION_DEPRIORITIZE_BOOTSTRAP_REPLICAS)
  public final boolean routerGetOperationDeprioritizeBootstrapReplicas;

  @Config(ROUTER_GET_OPERATION_MIN_LOCAL_REPLICA_COUNT_TO_PRIORITIZE_LOCAL)
  @Default("0")
  public final int routerGetOperationMinLocalReplicaCountToPrioritizeLocal;

  /**
   * Create a RouterConfig instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public RouterConfig(VerifiableProperties verifiableProperties) {
    routerBlobMetadataCacheId =
        verifiableProperties.getString(ROUTER_BLOB_METADATA_CACHE_ID, "routerBlobMetadataCache");
    routerMaxNumMetadataCacheEntries =
        verifiableProperties.getInt(ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES, MAX_NUM_METADATA_CACHE_ENTRIES_DEFAULT);
    routerBlobMetadataCacheEnabled = verifiableProperties.getBoolean(ROUTER_BLOB_METADATA_CACHE_ENABLED, false);
    routerSmallestBlobForMetadataCache =
        verifiableProperties.getLong(ROUTER_SMALLEST_BLOB_FOR_METADATA_CACHE, NUM_BYTES_IN_ONE_TB);
    routerScalingUnitCount = verifiableProperties.getIntInRange(ROUTER_SCALING_UNIT_COUNT, 1, 1, Integer.MAX_VALUE);
    routerHostname = verifiableProperties.getString(ROUTER_HOSTNAME);
    routerDatacenterName = verifiableProperties.getString(ROUTER_DATACENTER_NAME);
    routerScalingUnitMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange(ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_PLAIN_TEXT, 5, 1, 100);
    routerScalingUnitMaxConnectionsPerPortSsl =
        verifiableProperties.getIntInRange(ROUTER_SCALING_UNIT_MAX_CONNECTIONS_PER_PORT_SSL, 2, 1, 100);
    routerConnectionsLocalDcWarmUpPercentage =
        verifiableProperties.getIntInRange(ROUTER_CONNECTIONS_LOCAL_DC_WARM_UP_PERCENTAGE, 25, 0, 100);
    routerConnectionsRemoteDcWarmUpPercentage =
        verifiableProperties.getIntInRange(ROUTER_CONNECTIONS_REMOTE_DC_WARM_UP_PERCENTAGE, 0, 0, 100);
    routerConnectionsWarmUpTimeoutMs =
        verifiableProperties.getIntInRange(ROUTER_CONNECTIONS_WARM_UP_TIMEOUT_MS, 5000, 0, Integer.MAX_VALUE);
    routerConnectionCheckoutTimeoutMs =
        verifiableProperties.getIntInRange(ROUTER_CONNECTION_CHECKOUT_TIMEOUT_MS, 1000, 1, 5000);
    routerRequestTimeoutMs = verifiableProperties.getIntInRange(ROUTER_REQUEST_TIMEOUT_MS, 4000, 1,
        MAX_OVERALL_TIMEOUT_VALUE_FOR_A_REQUEST_IN_MS);
    routerRequestNetworkTimeoutMs = verifiableProperties.getIntInRange(ROUTER_REQUEST_NETWORK_TIMEOUT_MS, 2000, 1,
        MAX_NETWORK_TIMEOUT_VALUE_FOR_A_REQUEST_IN_MS);
    routerDropRequestOnTimeout = verifiableProperties.getBoolean(ROUTER_DROP_REQUEST_ON_TIMEOUT, false);
    routerMaxPutChunkSizeBytes =
        verifiableProperties.getIntInRange(ROUTER_MAX_PUT_CHUNK_SIZE_BYTES, 4 * 1024 * 1024, 1, Integer.MAX_VALUE);
    routerPutRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_PUT_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerPutLocalRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_PUT_LOCAL_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerPutRemoteRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_PUT_REMOTE_REQUEST_PARALLELISM, 0, 0, Integer.MAX_VALUE);
    routerPutSuccessTarget = verifiableProperties.getIntInRange(ROUTER_PUT_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerPutRemoteSuccessTarget = verifiableProperties.getIntInRange(ROUTER_PUT_REMOTE_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerReplicateBlobRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_REPLICATE_BLOB_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerReplicateBlobSuccessTarget =
        verifiableProperties.getIntInRange(ROUTER_REPLICATE_BLOB_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerMaxSlippedPutAttempts =
        verifiableProperties.getIntInRange(ROUTER_MAX_SLIPPED_PUT_ATTEMPTS, 1, 0, Integer.MAX_VALUE);
    routerDeleteRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_DELETE_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerDeleteSuccessTarget =
        verifiableProperties.getIntInRange(ROUTER_DELETE_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerGetRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_GET_REQUEST_PARALLELISM, 2, 1, Integer.MAX_VALUE);
    routerGetSuccessTarget = verifiableProperties.getIntInRange(ROUTER_GET_SUCCESS_TARGET, 1, 1, Integer.MAX_VALUE);
    routerGetCrossDcEnabled = verifiableProperties.getBoolean(ROUTER_GET_CROSS_DC_ENABLED, true);
    routerOperationTrackerIncludeDownReplicas =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_INCLUDE_DOWN_REPLICAS, true);
    routerGetOperationTrackerType =
        verifiableProperties.getString(ROUTER_GET_OPERATION_TRACKER_TYPE, "SimpleOperationTracker");
    routerPutOperationTrackerType =
        verifiableProperties.getString(ROUTER_PUT_OPERATION_TRACKER_TYPE, "SimpleOperationTracker");
    routerLatencyToleranceQuantile =
        verifiableProperties.getDoubleInRange(ROUTER_LATENCY_TOLERANCE_QUANTILE, DEFAULT_LATENCY_TOLERANCE_QUANTILE,
            0.0, 1.0);
    routerBlobidCurrentVersion =
        verifiableProperties.getShortFromAllowedValues(ROUTER_BLOBID_CURRENT_VERSION, (short) 6,
            new Short[]{1, 2, 3, 4, 5, 6});
    routerMetadataContentVersion =
        verifiableProperties.getShortFromAllowedValues(ROUTER_METADATA_CONTENT_VERSION, (short) 2, new Short[]{2, 3});
    routerKeyManagementServiceFactory =
        verifiableProperties.getString(ROUTER_KEY_MANAGEMENT_SERVICE_FACTORY, DEFAULT_KMS_FACTORY);
    routerCryptoServiceFactory =
        verifiableProperties.getString(ROUTER_CRYPTO_SERVICE_FACTORY, DEFAULT_CRYPTO_SERVICE_FACTORY);
    routerCryptoJobsWorkerCount =
        verifiableProperties.getIntInRange(ROUTER_CRYPTO_JOBS_WORKER_COUNT, 1, 1, Integer.MAX_VALUE);
    routerTtlUpdateRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_TTL_UPDATE_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerTtlUpdateSuccessTarget =
        verifiableProperties.getIntInRange(ROUTER_TTL_UPDATE_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerUndeleteRequestParallelism =
        verifiableProperties.getIntInRange(ROUTER_UNDELETE_REQUEST_PARALLELISM, 3, 1, Integer.MAX_VALUE);
    routerUndeleteSuccessTarget =
        verifiableProperties.getIntInRange(ROUTER_UNDELETE_SUCCESS_TARGET, 2, 1, Integer.MAX_VALUE);
    routerUndeleteOperationTrackerType =
        verifiableProperties.getString(ROUTER_UNDELETE_OPERATION_TRACKER_TYPE, "UndeleteOperationTracker");
    routerUseGetBlobOperationForBlobInfo =
        verifiableProperties.getBoolean(ROUTER_USE_GET_BLOB_OPERATION_FOR_BLOB_INFO, false);
    List<String> customPercentiles =
        Utils.splitString(verifiableProperties.getString(ROUTER_OPERATION_TRACKER_CUSTOM_PERCENTILES, ""), ",");
    routerOperationTrackerCustomPercentiles =
        Collections.unmodifiableList(customPercentiles.stream().map(Double::valueOf).collect(Collectors.toList()));
    String scopeStr = verifiableProperties.getString(ROUTER_OPERATION_TRACKER_METRIC_SCOPE, "Datacenter");
    routerOperationTrackerMetricScope = OperationTrackerScope.valueOf(scopeStr);
    routerOperationTrackerReservoirSize =
        verifiableProperties.getIntInRange(ROUTER_OPERATION_TRACKER_RESERVOIR_SIZE, 1028, 0, Integer.MAX_VALUE);
    routerOperationTrackerReservoirDecayFactor =
        verifiableProperties.getDouble(ROUTER_OPERATION_TRACKER_RESERVOIR_DECAY_FACTOR, 0.015);
    routerOperationTrackerMinDataPointsRequired =
        verifiableProperties.getLong(ROUTER_OPERATION_TRACKER_MIN_DATA_POINTS_REQUIRED, 1000L);
    routerOperationTrackerMaxInflightRequests =
        verifiableProperties.getIntInRange(ROUTER_OPERATION_TRACKER_MAX_INFLIGHT_REQUESTS, 2, 1, Integer.MAX_VALUE);
    routerAdaptiveOperationTrackerWaitingForResponse =
        verifiableProperties.getBoolean(ROUTER_ADAPTIVE_OPERATION_TRACKER_WAITING_FOR_RESPONSE, false);
    routerOperationTrackerExcludeTimeoutEnabled =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_EXCLUDE_TIMEOUT_ENABLED, false);
    routerOperationTrackerHistogramDumpEnabled =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_ENABLED, false);
    routerOperationTrackerHistogramDumpPeriod =
        verifiableProperties.getLongInRange(ROUTER_OPERATION_TRACKER_HISTOGRAM_DUMP_PERIOD, 600L, 1L, Long.MAX_VALUE);
    routerOperationTrackerHistogramCacheTimeoutMs =
        verifiableProperties.getLong(ROUTER_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS,
            DEFAULT_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS);
    if (routerGetRequestParallelism > routerOperationTrackerMaxInflightRequests) {
      throw new IllegalArgumentException(
          "Operation tracker parallelism is larger than operation tracker max inflight number");
    }
    routerOperationTrackerTerminateOnNotFoundEnabled =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED, false);
    routerMaxInMemPutChunks = verifiableProperties.getIntInRange(ROUTER_MAX_IN_MEM_PUT_CHUNKS, 4, 1,
        Integer.MAX_VALUE / routerMaxPutChunkSizeBytes);
    routerMaxInMemGetChunks = verifiableProperties.getIntInRange(ROUTER_MAX_IN_MEM_GET_CHUNKS, 4, 1,
        Integer.MAX_VALUE / routerMaxPutChunkSizeBytes);
    routerGetEligibleReplicasByStateEnabled =
        verifiableProperties.getBoolean(ROUTER_GET_ELIGIBLE_REPLICAS_BY_STATE_ENABLED, false);
    routerPutUseDynamicSuccessTarget = verifiableProperties.getBoolean(ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET, false);
    routerEnableHttp2NetworkClient = verifiableProperties.getBoolean(ROUTER_ENABLE_HTTP2_NETWORK_CLIENT, false);
    routerCrossColoRequestToDcWithMostReplicas =
        verifiableProperties.getBoolean(ROUTER_CROSS_COLO_REQUEST_TO_DC_WITH_MOST_REPLICAS, false);
    routerBackgroundDeleterMaxConcurrentOperations =
        verifiableProperties.getIntInRange(ROUTER_BACKGROUND_DELETER_MAX_CONCURRENT_OPERATIONS, 0, 0,
            Integer.MAX_VALUE);
    operationController =
        verifiableProperties.getString(OPERATION_CONTROLLER, "com.github.ambry.router.OperationController");
    routerNotFoundCacheTtlInMs = verifiableProperties.getLongInRange(ROUTER_NOT_FOUND_CACHE_TTL_IN_MS, 15 * 1000L, 0,
        ROUTER_NOT_FOUND_CACHE_MAX_TTL_IN_MS);
    routerUpdateOpMetadataRelianceTimestampInMs =
        verifiableProperties.getLong(ROUTER_UPDATE_OP_METADATA_RELIANCE_TIMESTAMP_IN_MS,
            DEFAULT_ROUTER_UPDATE_OP_METADATA_RELIANCE_TIMESTAMP_IN_MS);
    routerRepairWithReplicateBlobOnTtlUpdateEnabled =
        verifiableProperties.getBoolean(ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_TTLUPDATE_ENABLED, false);
    routerRepairWithReplicateBlobOnDeleteEnabled =
        verifiableProperties.getBoolean(ROUTER_REPAIR_WITH_REPLICATE_BLOB_ON_DELETE_ENABLED, false);
    routerRepairRequestsDbFactory = verifiableProperties.getString(ROUTER_REPAIR_REQUESTS_DB_FACTORY, null);
    routerTtlUpdateOfflineRepairEnabled = routerRepairRequestsDbFactory == null ? false
        : verifiableProperties.getBoolean(ROUTER_TTLUPDATE_OFFLINE_REPAIR_ENABLED, false);
    routerDeleteOfflineRepairEnabled = routerRepairRequestsDbFactory == null ? false
        : verifiableProperties.getBoolean(ROUTER_DELETE_OFFLINE_REPAIR_ENABLED, false);

    routerGetBlobRetryLimitInSec = verifiableProperties.getIntInRange(ROUTER_GET_BLOB_RETRY_LIMIT_IN_SEC, 0, 0,
        ROUTER_GET_BLOB_RETRY_LIMIT_IN_SEC_MAX);
    routerGetBlobRetryLimitCount = verifiableProperties.getIntInRange(ROUTER_GET_BLOB_RETRY_LIMIT_COUNT, 0, 0,
        ROUTER_GET_BLOB_RETRY_LIMIT_COUNT_MAX);

    compressionConfig = new CompressionConfig(verifiableProperties);
    routerOperationTrackerCheckAllOriginatingReplicasForNotFound =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND, true);
    routerOperationTrackerRequireTwoNotFound =
        verifiableProperties.getBoolean(ROUTER_OPERATION_TRACKER_REQUIRE_TWO_NOT_FOUND, false);
    routerReservedMetadataEnabled = verifiableProperties.getBoolean(RESERVED_METADATA_ENABLED, false);
    routerGetOperationDeprioritizeBootstrapReplicas =
        verifiableProperties.getBoolean(ROUTER_GET_OPERATION_DEPRIORITIZE_BOOTSTRAP_REPLICAS, false);
    routerGetOperationMinLocalReplicaCountToPrioritizeLocal =
        verifiableProperties.getInt(ROUTER_GET_OPERATION_MIN_LOCAL_REPLICA_COUNT_TO_PRIORITIZE_LOCAL,
            DEFAULT_ROUTER_GET_OPERATION_MIN_LOCAL_REPLICA_COUNT_TO_PRIORITIZE_LOCAL);
  }

  /**
   * Get the compression-related configs.
   * @return The compression-related configs.
   */
  public CompressionConfig getCompressionConfig() {
    return compressionConfig;
  }
}
