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
package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.Resource;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.utils.CachedHistogram;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * {@link NonBlockingRouter}-specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link NonBlockingRouter} to the provided {@link MetricRegistry}
 */
public class NonBlockingRouterMetrics {
  private final MetricRegistry metricRegistry;
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterMetrics.class);

  // Operation rate.
  public final Meter putBlobOperationRate;
  public final Meter stitchBlobOperationRate;
  public final Meter getBlobInfoOperationRate;
  public final Meter getBlobOperationRate;
  public final Meter getBlobWithRangeOperationRate;
  public final Meter getBlobWithSegmentOperationRate;
  public final Meter deleteBlobOperationRate;
  public final Meter undeleteBlobOperationRate;
  public final Meter updateBlobTtlOperationRate;
  public final Meter putEncryptedBlobOperationRate;
  public final Meter stitchEncryptedBlobOperationRate;
  public final Meter getEncryptedBlobInfoOperationRate;
  public final Meter getEncryptedBlobOperationRate;
  public final Meter getEncryptedBlobWithRangeOperationRate;
  public final Meter getEncryptedBlobWithSegmentOperationRate;
  public final Meter operationQueuingRate;
  public final Meter operationDequeuingRate;
  public final Meter getBlobNotOriginateLocalOperationRate;
  public final Meter deleteBlobNotOriginateLocalOperationRate;
  public final Meter undeleteBlobNotOriginateLocalOperationRate;
  public final Meter ttlUpdateBlobNotOriginateLocalOperationRate;

  // Latency.
  public final Histogram putBlobOperationLatencyMs;
  public final Histogram stitchBlobOperationLatencyMs;
  public final Timer putChunkOperationLatencyMs;
  public final Histogram getBlobInfoOperationLatencyMs;
  public final Histogram getBlobOperationLatencyMs;
  public final Histogram getBlobOperationTotalTimeMs;
  public final Histogram putEncryptedBlobOperationLatencyMs;
  public final Histogram stitchEncryptedBlobOperationLatencyMs;
  public final Histogram putEncryptedChunkOperationLatencyMs;
  public final Histogram getEncryptedBlobInfoOperationLatencyMs;
  public final Histogram getEncryptedBlobOperationLatencyMs;
  public final Histogram getEncryptedBlobOperationTotalTimeMs;
  public final Histogram deleteBlobOperationLatencyMs;
  public final Histogram undeleteBlobOperationLatencyMs;
  public final Histogram updateBlobTtlOperationLatencyMs;
  public final Histogram routerRequestLatencyMs;
  public final Histogram responseReceiveToHandleLatencyMs;

  // Operation error count.
  public final Counter putBlobErrorCount;
  public final Counter stitchBlobErrorCount;
  public final Counter getBlobInfoErrorCount;
  public final Counter getBlobErrorCount;
  public final Counter getBlobWithRangeErrorCount;
  public final Counter getBlobWithSegmentErrorCount;
  public final Counter putEncryptedBlobErrorCount;
  public final Counter stitchEncryptedBlobErrorCount;
  public final Counter getEncryptedBlobInfoErrorCount;
  public final Counter getEncryptedBlobErrorCount;
  public final Counter getEncryptedBlobWithRangeErrorCount;
  public final Counter getEncryptedBlobWithSegmentErrorCount;
  public final Counter deleteBlobErrorCount;
  public final Counter undeleteBlobErrorCount;
  public final Counter updateBlobTtlErrorCount;
  public final Counter operationAbortCount;
  public final Counter routerRequestErrorCount;

  // Count for various errors.
  public final Counter ambryUnavailableErrorCount;
  public final Counter invalidBlobIdErrorCount;
  public final Counter invalidPutArgumentErrorCount;
  public final Counter operationTimedOutErrorCount;
  public final Counter routerClosedErrorCount;
  public final Counter unexpectedInternalErrorCount;
  public final Counter blobTooLargeErrorCount;
  public final Counter badInputChannelErrorCount;
  public final Counter insufficientCapacityErrorCount;
  public final Counter blobDeletedErrorCount;
  public final Counter blobDoesNotExistErrorCount;
  public final Counter blobExpiredErrorCount;
  public final Counter rangeNotSatisfiableErrorCount;
  public final Counter channelClosedErrorCount;
  public final Counter unknownReplicaResponseError;
  public final Counter unknownErrorCountForOperation;
  public final Counter responseDeserializationErrorCount;
  public final Counter operationManagerPollErrorCount;
  public final Counter operationManagerHandleResponseErrorCount;
  public final Counter requestResponseHandlerUnexpectedErrorCount;
  public final Counter chunkFillerUnexpectedErrorCount;
  public final Counter operationFailureWithUnsetExceptionCount;

  // Performance metrics for operation managers.
  public final Histogram putManagerPollTimeMs;
  public final Histogram getManagerPollTimeMs;
  public final Histogram deleteManagerPollTimeMs;
  public final Histogram undeleteManagerPollTimeMs;
  public final Histogram ttlUpdateManagerPollTimeMs;
  public final Histogram putManagerHandleResponseTimeMs;
  public final Histogram getManagerHandleResponseTimeMs;
  public final Histogram deleteManagerHandleResponseTimeMs;
  public final Histogram undeleteManagerHandleResponseTimeMs;
  public final Histogram ttlUpdateManagerHandleResponseTimeMs;
  // time spent in getting a chunk filled once it is available.
  public final Histogram chunkFillTimeMs;
  // time spent in encrypting a chunk once filling is complete
  public final Histogram encryptTimeMs;
  // time spent in decrypting content for a single getChunk or UserMetadata in case of GetBlobInfo
  public final Histogram decryptTimeMs;
  // time spent waiting for a chunk to become available for filling once data is available.
  public final Histogram waitTimeForFreeChunkAvailabilityMs;
  // time spent by a chunk waiting for data to become available in the channel.
  public final Histogram waitTimeForChannelDataAvailabilityMs;

  // Misc metrics.
  public final Meter operationErrorRate;
  public final Meter encryptedOperationErrorRate;
  public final Counter slippedPutAttemptCount;
  public final Counter slippedPutSuccessCount;
  public final Counter ignoredResponseCount;
  public final Counter crossColoRequestCount;
  public final Counter crossColoSuccessCount;
  public final Counter simpleEncryptedBlobSizeMismatchCount;
  public final Counter simpleUnencryptedBlobSizeMismatchCount;
  public final Counter compositeBlobSizeMismatchCount;
  // Number of unnecessary blob gets avoided via use of BlobDataType
  public final Counter skippedGetBlobCount;
  public Gauge<Long> chunkFillerThreadRunning;
  public Gauge<Long> requestResponseHandlerThreadRunning;

  // metrics for tracking blob sizes and chunking.
  public final Histogram putBlobSizeBytes;
  public final Histogram putBlobChunkCount;
  public final Histogram getBlobSizeBytes;
  public final Histogram getBlobChunkCount;
  public final Histogram getBlobWithRangeSizeBytes;
  public final Histogram getBlobWithSegmentSizeBytes;
  public final Histogram getBlobWithRangeTotalBlobSizeBytes;
  public final Histogram getBlobWithSegmentTotalBlobSizeBytes;
  public final Counter simpleBlobPutCount;
  public final Counter simpleBlobGetCount;
  public final Counter compositeBlobPutCount;
  public final Counter compositeBlobGetCount;
  public final Counter rawBlobGetCount;

  // AdaptiveOperationTracker metrics
  public final CachedHistogram getBlobLocalDcLatencyMs;
  public final CachedHistogram getBlobCrossDcLatencyMs;
  public final Counter getBlobPastDueCount;

  public final CachedHistogram getBlobInfoLocalDcLatencyMs;
  public final CachedHistogram getBlobInfoCrossDcLatencyMs;
  public final Counter getBlobInfoPastDueCount;

  // Workload characteristics
  public final AgeAtAccessMetrics ageAtGet;
  public final AgeAtAccessMetrics ageAtDelete;
  public final AgeAtAccessMetrics ageAtUndelete;
  public final AgeAtAccessMetrics ageAtTtlUpdate;

  // Crypto job metrics
  public final CryptoJobMetrics encryptJobMetrics;
  public final CryptoJobMetrics decryptJobMetrics;

  // Resource to latency histogram map. Here resource can be DataNode, Partition, Disk, Replica etc.
  Map<Resource, CachedHistogram> getBlobLocalDcResourceToLatency = new HashMap<>();
  Map<Resource, CachedHistogram> getBlobCrossDcResourceToLatency = new HashMap<>();

  Map<Resource, CachedHistogram> getBlobInfoLocalDcResourceToLatency = new HashMap<>();
  Map<Resource, CachedHistogram> getBlobInfoCrossDcResourceToLatency = new HashMap<>();

  // Map that stores dataNode-level metrics.
  private final Map<DataNodeId, NodeLevelMetrics> dataNodeToMetrics;
  private final RouterConfig routerConfig;
  private ScheduledExecutorService scheduler = null;

  public NonBlockingRouterMetrics(ClusterMap clusterMap, RouterConfig routerConfig) {
    metricRegistry = clusterMap.getMetricRegistry();
    this.routerConfig = routerConfig;

    // Operation Rate.
    putBlobOperationRate = metricRegistry.meter(MetricRegistry.name(PutOperation.class, "PutBlobOperationRate"));
    stitchBlobOperationRate = metricRegistry.meter(MetricRegistry.name(PutOperation.class, "StitchBlobOperationRate"));
    getBlobInfoOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoOperationRate"));
    getBlobOperationRate = metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationRate"));
    getBlobWithRangeOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeOperationRate"));
    getBlobWithSegmentOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithSegmentOperationRate"));
    putEncryptedBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobOperationRate"));
    stitchEncryptedBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(PutOperation.class, "StitchEncryptedBlobOperationRate"));
    getEncryptedBlobInfoOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobInfoOperation.class, "GetEncryptedBlobInfoOperationRate"));
    getEncryptedBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobOperationRate"));
    getEncryptedBlobWithRangeOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithRangeOperationRate"));
    getEncryptedBlobWithSegmentOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithSegmentOperationRate"));
    deleteBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobOperationRate"));
    undeleteBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(UndeleteOperation.class, "UndeleteBlobOperationRate"));
    updateBlobTtlOperationRate =
        metricRegistry.meter(MetricRegistry.name(TtlUpdateOperation.class, "UpdateBlobTtlOperationRate"));
    operationQueuingRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationQueuingRate"));
    operationDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationDequeuingRate"));
    getBlobNotOriginateLocalOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobNotOriginateLocalOperationRate"));
    deleteBlobNotOriginateLocalOperationRate =
        metricRegistry.meter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobNotOriginateLocalOperationRate"));
    undeleteBlobNotOriginateLocalOperationRate = metricRegistry.meter(
        MetricRegistry.name(UndeleteOperation.class, "UndeleteBlobNotOriginateLocalOperationRate"));
    ttlUpdateBlobNotOriginateLocalOperationRate = metricRegistry.meter(
        MetricRegistry.name(TtlUpdateOperation.class, "TtlUpdateBlobNotOriginateLocalOperationRate"));

    // Latency.
    putBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutBlobOperationLatencyMs"));
    stitchBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "StitchBlobOperationLatencyMs"));
    putChunkOperationLatencyMs =
        metricRegistry.timer(MetricRegistry.name(PutOperation.class, "PutChunkOperationLatencyMs"));
    getBlobInfoOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoOperationLatencyMs"));
    getBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationLatencyMs"));
    getBlobOperationTotalTimeMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationTotalTimeMs"));
    putEncryptedBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobOperationLatencyMs"));
    stitchEncryptedBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "StitchEncryptedBlobOperationLatencyMs"));
    putEncryptedChunkOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutEncryptedChunkOperationLatencyMs"));
    getEncryptedBlobInfoOperationLatencyMs = metricRegistry.histogram(
        MetricRegistry.name(GetBlobInfoOperation.class, "GetEncryptedBlobInfoOperationLatencyMs"));
    getEncryptedBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobOperationLatencyMs"));
    getEncryptedBlobOperationTotalTimeMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobOperationTotalTimeMs"));
    deleteBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteOperation.class, "DeleteBlobOperationLatencyMs"));
    undeleteBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(UndeleteOperation.class, "UndeleteBlobOperationLatencyMs"));
    updateBlobTtlOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(TtlUpdateOperation.class, "UpdateBlobTtlOperationLatencyMs"));
    routerRequestLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(NonBlockingRouter.class, "RouterRequestLatencyMs"));
    responseReceiveToHandleLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(NonBlockingRouter.class, "ResponseReceiveToHandleLatencyMs"));

    // Operation error count.
    putBlobErrorCount = metricRegistry.counter(MetricRegistry.name(PutOperation.class, "PutBlobErrorCount"));
    stitchBlobErrorCount = metricRegistry.counter(MetricRegistry.name(PutOperation.class, "StitchBlobErrorCount"));
    getBlobInfoErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoErrorCount"));
    getBlobErrorCount = metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetBlobErrorCount"));
    getBlobWithRangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeErrorCount"));
    getBlobWithSegmentErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithSegmentErrorCount"));
    putEncryptedBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobErrorCount"));
    stitchEncryptedBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(PutOperation.class, "StitchEncryptedBlobErrorCount"));
    getEncryptedBlobInfoErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "GetEncryptedBlobInfoErrorCount"));
    getEncryptedBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobErrorCount"));
    getEncryptedBlobWithRangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithRangeErrorCount"));
    getEncryptedBlobWithSegmentErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithSegmentErrorCount"));
    deleteBlobErrorCount = metricRegistry.counter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobErrorCount"));
    undeleteBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(UndeleteOperation.class, "UndeleteBlobErrorCount"));
    updateBlobTtlErrorCount =
        metricRegistry.counter(MetricRegistry.name(TtlUpdateOperation.class, "UpdateBlobTtlErrorCount"));
    operationAbortCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "OperationAbortCount"));
    routerRequestErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "RouterRequestErrorCount"));

    // Counters for various errors.
    ambryUnavailableErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "AmbryUnavailableErrorCount"));
    invalidBlobIdErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "InvalidBlobIdErrorCount"));
    invalidPutArgumentErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "InvalidPutArgumentErrorCount"));
    operationTimedOutErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "OperationTimedOutErrorCount"));
    routerClosedErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "RouterClosedErrorCount"));
    unexpectedInternalErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "UnexpectedInternalErrorCount"));
    blobTooLargeErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "BlobTooLargeErrorCount"));
    badInputChannelErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "BadInputChannelErrorCount"));
    insufficientCapacityErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "InsufficientCapacityErrorCount"));
    blobDeletedErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "BlobDeletedErrorCount"));
    blobDoesNotExistErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "BlobDoesNotExistErrorCount"));
    blobExpiredErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "BlobExpiredErrorCount"));
    rangeNotSatisfiableErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "RangeNotSatisfiableErrorCount"));
    channelClosedErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "ChannelClosedErrorCount"));
    unknownReplicaResponseError =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "UnknownReplicaResponseError"));
    unknownErrorCountForOperation =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "UnknownErrorCountForOperation"));
    responseDeserializationErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "ResponseDeserializationErrorCount"));
    operationManagerPollErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "OperationManagerPollErrorCount"));
    operationManagerHandleResponseErrorCount = metricRegistry.counter(
        MetricRegistry.name(NonBlockingRouter.class, "OperationManagerHandleResponseErrorCount"));
    requestResponseHandlerUnexpectedErrorCount = metricRegistry.counter(
        MetricRegistry.name(NonBlockingRouter.class, "RequestResponseHandlerUnexpectedErrorCount"));
    chunkFillerUnexpectedErrorCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "ChunkFillerUnexpectedErrorCount"));
    operationFailureWithUnsetExceptionCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "OperationFailureWithUnsetExceptionCount"));

    // Performance metrics for operation managers.
    putManagerPollTimeMs = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutManagerPollTimeMs"));
    getManagerPollTimeMs = metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetManagerPollTimeMs"));
    deleteManagerPollTimeMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteManager.class, "DeleteManagerPollTimeMs"));
    undeleteManagerPollTimeMs =
        metricRegistry.histogram(MetricRegistry.name(UndeleteManager.class, "UndeleteManagerPollTimeMs"));
    ttlUpdateManagerPollTimeMs =
        metricRegistry.histogram(MetricRegistry.name(TtlUpdateManager.class, "TtlUpdateManagerPollTimeMs"));
    putManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutManagerHandleResponseTimeMs"));
    getManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetManagerHandleResponseTimeMs"));
    deleteManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteManager.class, "DeleteManagerHandleResponseTimeMs"));
    undeleteManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(UndeleteManager.class, "UndeleteManagerHandleResponseTimeMs"));
    ttlUpdateManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(TtlUpdateManager.class, "TtlUpdateManagerHandleResponseTimeMs"));
    chunkFillTimeMs = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "ChunkFillTimeMs"));
    encryptTimeMs = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "EncryptTimeMs"));
    decryptTimeMs = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "DecryptTimeMs"));
    waitTimeForFreeChunkAvailabilityMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "WaitTimeForFreeChunkAvailabilityMs"));
    waitTimeForChannelDataAvailabilityMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "WaitTimeForChannelDataAvailabilityMs"));

    // Misc metrics.
    operationErrorRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationErrorRate"));
    encryptedOperationErrorRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "EncryptedOperationErrorRate"));
    ignoredResponseCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "IgnoredRequestCount"));
    slippedPutAttemptCount = metricRegistry.counter(MetricRegistry.name(PutOperation.class, "SlippedPutAttemptCount"));
    slippedPutSuccessCount = metricRegistry.counter(MetricRegistry.name(PutOperation.class, "SlippedPutSuccessCount"));
    crossColoRequestCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "CrossColoRequestCount"));
    crossColoSuccessCount =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "CrossColoSuccessCount"));
    simpleEncryptedBlobSizeMismatchCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "SimpleEncryptedBlobSizeMismatchCount"));
    simpleUnencryptedBlobSizeMismatchCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "SimpleUnencryptedBlobSizeMismatchCount"));
    compositeBlobSizeMismatchCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "CompositeBlobSizeMismatchCount"));

    // metrics to track blob sizes and chunking.
    putBlobSizeBytes = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutBlobSizeBytes"));
    putBlobChunkCount = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutBlobChunkCount"));
    getBlobSizeBytes = metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobSizeBytes"));
    getBlobChunkCount = metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobChunkCount"));
    getBlobWithRangeSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeSizeBytes"));
    getBlobWithSegmentSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithSegmentSizeBytes"));
    getBlobWithRangeTotalBlobSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeTotalBlobSizeBytes"));
    getBlobWithSegmentTotalBlobSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithSegmentTotalBlobSizeBytes"));
    simpleBlobPutCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "SimpleBlobPutCount"));
    simpleBlobGetCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "SimpleBlobGetCount"));
    compositeBlobPutCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "CompositeBlobPutCount"));
    compositeBlobGetCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "CompositeBlobGetCount"));
    skippedGetBlobCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "SkippedGetBlobCount"));
    rawBlobGetCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "RawBlobGetCount"));

    // Track metrics at the DataNode level.
    dataNodeToMetrics = new ConcurrentHashMap<>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      String dataNodeName =
          dataNodeId.getDatacenterName() + "." + dataNodeId.getHostname() + "." + dataNodeId.getPort();
      dataNodeToMetrics.put(dataNodeId, new NodeLevelMetrics(metricRegistry, dataNodeName));
    }

    // AdaptiveOperationTracker trackers
    getBlobLocalDcLatencyMs =
        (CachedHistogram) metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "LocalDcLatencyMs"),
            () -> createHistogram(routerConfig, true));
    getBlobCrossDcLatencyMs =
        (CachedHistogram) metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "CrossDcLatencyMs"),
            () -> createHistogram(routerConfig, true));
    getBlobPastDueCount = metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "PastDueCount"));

    getBlobInfoLocalDcLatencyMs =
        (CachedHistogram) metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "LocalDcLatencyMs"),
            () -> createHistogram(routerConfig, true));
    getBlobInfoCrossDcLatencyMs =
        (CachedHistogram) metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "CrossDcLatencyMs"),
            () -> createHistogram(routerConfig, true));
    getBlobInfoPastDueCount = metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "PastDueCount"));

    // Workload
    ageAtGet = new AgeAtAccessMetrics(metricRegistry, "OnGet");
    ageAtDelete = new AgeAtAccessMetrics(metricRegistry, "OnDelete");
    ageAtUndelete = new AgeAtAccessMetrics(metricRegistry, "OnUndelete");
    ageAtTtlUpdate = new AgeAtAccessMetrics(metricRegistry, "OnTtlUpdate");

    // Encrypt/Decrypt job metrics
    encryptJobMetrics = new CryptoJobMetrics(PutOperation.class, "Encrypt", metricRegistry);
    decryptJobMetrics = new CryptoJobMetrics(GetOperation.class, "Decrypt", metricRegistry);

    // Record type of adaptive tracker and configure custom percentiles
    if (routerConfig != null) {
      logger.info("The metric scope of adaptive tracker is {}", routerConfig.routerOperationTrackerMetricScope);
      registerCustomPercentiles(GetBlobOperation.class, "LocalDcLatencyMs", getBlobLocalDcLatencyMs,
          routerConfig.routerOperationTrackerCustomPercentiles);
      registerCustomPercentiles(GetBlobOperation.class, "CrossDcLatencyMs", getBlobCrossDcLatencyMs,
          routerConfig.routerOperationTrackerCustomPercentiles);
      registerCustomPercentiles(GetBlobInfoOperation.class, "LocalDcLatencyMs", getBlobInfoLocalDcLatencyMs,
          routerConfig.routerOperationTrackerCustomPercentiles);
      registerCustomPercentiles(GetBlobInfoOperation.class, "CrossDcLatencyMs", getBlobInfoCrossDcLatencyMs,
          routerConfig.routerOperationTrackerCustomPercentiles);
    }

    if (routerConfig != null && routerConfig.routerOperationTrackerMetricScope != OperationTrackerScope.Datacenter) {
      // pre-populate all resource-to-histogram maps here to allow lock-free hashmap in adaptive operation tracker
      initializeResourceToHistogramMap(clusterMap, routerConfig);
    }

    if (routerConfig != null && routerConfig.routerOperationTrackerHistogramDumpEnabled) {
      HistogramDumper histogramDumper = new HistogramDumper();
      scheduler = Utils.newScheduler(1, false);
      logger.info("Scheduling histogram dumper with a period of {} secs",
          routerConfig.routerOperationTrackerHistogramDumpPeriod);
      scheduler.scheduleAtFixedRate(histogramDumper, 0, routerConfig.routerOperationTrackerHistogramDumpPeriod,
          TimeUnit.SECONDS);
    }
  }

  /**
   * Initialize resource-to-latency-histogram maps based on given resource type. Here resource can be {@link PartitionId},
   * {@link DataNodeId}, etc. The resource type is defined by {@link RouterConfig#routerOperationTrackerMetricScope}.
   * @param clusterMap the {@link ClusterMap} that contains info of all resources.
   * @param routerConfig the {@link RouterConfig} that specifies histogram parameters.
   */
  private void initializeResourceToHistogramMap(ClusterMap clusterMap, RouterConfig routerConfig) {
    String localDatacenterName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    switch (routerConfig.routerOperationTrackerMetricScope) {
      case Partition:
        for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
          getBlobLocalDcResourceToLatency.put(partitionId, createHistogram(routerConfig, false));
          getBlobInfoLocalDcResourceToLatency.put(partitionId, createHistogram(routerConfig, false));
          getBlobCrossDcResourceToLatency.put(partitionId, createHistogram(routerConfig, false));
          getBlobInfoCrossDcResourceToLatency.put(partitionId, createHistogram(routerConfig, false));
        }
        break;
      case DataNode:
        List<? extends DataNodeId> dataNodeIds = clusterMap.getDataNodeIds();
        for (DataNodeId dataNodeId : dataNodeIds) {
          if (dataNodeId.getDatacenterName().equals(localDatacenterName)) {
            getBlobLocalDcResourceToLatency.put(dataNodeId, createHistogram(routerConfig, false));
            getBlobInfoLocalDcResourceToLatency.put(dataNodeId, createHistogram(routerConfig, false));
          } else {
            getBlobCrossDcResourceToLatency.put(dataNodeId, createHistogram(routerConfig, false));
            getBlobInfoCrossDcResourceToLatency.put(dataNodeId, createHistogram(routerConfig, false));
          }
        }
        break;
      case Disk:
        for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
          for (ReplicaId replicaId : partitionId.getReplicaIds()) {
            DiskId diskId = replicaId.getDiskId();
            if (getBlobLocalDcResourceToLatency.containsKey(diskId) || getBlobCrossDcResourceToLatency.containsKey(
                diskId)) {
              continue;
            }
            if (replicaId.getDataNodeId().getDatacenterName().equals(localDatacenterName)) {
              getBlobLocalDcResourceToLatency.put(diskId, createHistogram(routerConfig, false));
              getBlobInfoLocalDcResourceToLatency.put(diskId, createHistogram(routerConfig, false));
            } else {
              getBlobCrossDcResourceToLatency.put(diskId, createHistogram(routerConfig, false));
              getBlobInfoCrossDcResourceToLatency.put(diskId, createHistogram(routerConfig, false));
            }
          }
        }
      default:
        // if routerOperationTrackerMetricScope = Datacenter, do nothing in this method because datacenter-level
        // histograms and pastDueCounter are always instantiated.
    }
  }

  /**
   * Create a histogram with parameters specified by the provided {@link RouterConfig} that stores cached values for use
   * by {@link OperationTracker}.
   * @param routerConfig the {@link RouterConfig} containing the histogram parameters.
   * @param useDefaultReservoirParams {@code true} to use the default {@link ExponentiallyDecayingReservoir} constructor
   *                                  instead of the parameters from the config.
   * @return a configured {@link CachedHistogram}.
   */
  static CachedHistogram createHistogram(RouterConfig routerConfig, boolean useDefaultReservoirParams) {
    Reservoir reservoir;
    long cacheTimeoutMs;
    double quantile;
    if (routerConfig != null) {
      if (useDefaultReservoirParams) {
        reservoir = new ExponentiallyDecayingReservoir();
      } else {
        reservoir = new ExponentiallyDecayingReservoir(routerConfig.routerOperationTrackerReservoirSize,
            routerConfig.routerOperationTrackerReservoirDecayFactor);
      }
      cacheTimeoutMs = routerConfig.routerOperationTrackerHistogramCacheTimeoutMs;
      quantile = routerConfig.routerLatencyToleranceQuantile;
    } else {
      reservoir = new ExponentiallyDecayingReservoir();
      cacheTimeoutMs = RouterConfig.DEFAULT_OPERATION_TRACKER_HISTOGRAM_CACHE_TIMEOUT_MS;
      quantile = RouterConfig.DEFAULT_LATENCY_TOLERANCE_QUANTILE;
    }
    return new CachedHistogram(reservoir, cacheTimeoutMs, quantile);
  }

  /**
   * Register {@link Gauge} metric for each custom percentile of given {@link Histogram} with given class and name.
   * @param ownerClass the {@link Class} that is supposed to own the metrics.
   * @param name the name of metric that all percentiles belong to.
   * @param histogram the {@link Histogram} to use.
   * @param percentiles a list of interested percentiles (double value).
   */
  private void registerCustomPercentiles(Class ownerClass, String name, Histogram histogram, List<Double> percentiles) {
    percentiles.forEach(p -> {
      Gauge<Double> customPercentile = () -> histogram.getSnapshot().getValue(p);
      metricRegistry.register(MetricRegistry.name(ownerClass, name, String.valueOf(p * 100), "thPercentile"),
          customPercentile);
    });
  }

  /**
   * Initializes a {@link Gauge} metric for the status of {@code RequestResponseHandlerThread} of an
   * {@code OperationController}, to indicate if it is running
   * or not.
   * @param requestResponseHandlerThread The {@code RequestResponseHandlerThread} of which the status is
   *                                     to be monitored.
   */
  public void initializeOperationControllerMetrics(final Thread requestResponseHandlerThread) {
    requestResponseHandlerThreadRunning = () -> requestResponseHandlerThread.isAlive() ? 1L : 0L;
    metricRegistry.register(
        MetricRegistry.name(NonBlockingRouter.class, requestResponseHandlerThread.getName() + "Running"),
        requestResponseHandlerThreadRunning);
  }

  /**
   * Initializes a {@link Gauge} metric for the status of {@code ChunkFillerThread} of a {@link PutManager}, to
   * indicate if it is running or not.
   * @param chunkFillerThread The {@code ChunkFillerThread} of which the status is to be monitored.
   */
  public void initializePutManagerMetrics(final Thread chunkFillerThread) {
    chunkFillerThreadRunning = () -> chunkFillerThread.isAlive() ? 1L : 0L;
    metricRegistry.register(MetricRegistry.name(PutManager.class, chunkFillerThread.getName() + "Running"),
        chunkFillerThreadRunning);
  }

  /**
   * Initializes a {@link Gauge} metric to monitor the number of running
   * {@code OperationController} of a {@link NonBlockingRouter}.
   * @param currentOperationsCount The counter of {@code OperationController}.
   * @param currentBackgroundOperationsCount The counter of background operations submitted to the router that are not
   *                                         yet completed.
   */
  public void initializeNumActiveOperationsMetrics(final AtomicInteger currentOperationsCount,
      final AtomicInteger currentBackgroundOperationsCount) {
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "NumActiveOperations"),
        (Gauge<Integer>) currentOperationsCount::get);
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "NumActiveBackgroundOperations"),
        (Gauge<Integer>) currentBackgroundOperationsCount::get);
  }

  /**
   * @return the MetricRegistry being used in {@link NonBlockingRouterMetrics}
   */
  MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * Increment error metrics based on error type.
   * @param exception The exception associated with this error.
   */
  private void onError(Exception exception) {
    if (exception instanceof RouterException) {
      RouterErrorCode errorCode = ((RouterException) exception).getErrorCode();
      switch (errorCode) {
        case InvalidBlobId:
          invalidBlobIdErrorCount.inc();
          break;
        case InvalidPutArgument:
          invalidPutArgumentErrorCount.inc();
          break;
        case BlobTooLarge:
          blobTooLargeErrorCount.inc();
          break;
        case BadInputChannel:
          badInputChannelErrorCount.inc();
          break;
        case BlobDeleted:
          blobDeletedErrorCount.inc();
          break;
        case BlobExpired:
          blobExpiredErrorCount.inc();
          break;
        case RangeNotSatisfiable:
          rangeNotSatisfiableErrorCount.inc();
          break;
        case AmbryUnavailable:
          ambryUnavailableErrorCount.inc();
          break;
        case OperationTimedOut:
          operationTimedOutErrorCount.inc();
          break;
        case RouterClosed:
          routerClosedErrorCount.inc();
          break;
        case UnexpectedInternalError:
          unexpectedInternalErrorCount.inc();
          break;
        case InsufficientCapacity:
          insufficientCapacityErrorCount.inc();
          break;
        case BlobDoesNotExist:
          blobDoesNotExistErrorCount.inc();
          break;
        case ChannelClosed:
          channelClosedErrorCount.inc();
          break;
        default:
          unknownErrorCountForOperation.inc();
          break;
      }
    } else {
      unknownErrorCountForOperation.inc();
    }
  }

  /**
   * Update appropriate metrics on a putBlob operation related error.
   * @param e the {@link Exception} associated with the error.
   * @param encryptionEnabled {@code true} if encryption was enabled for this operation. {@code false} otherwise
   * @param stitchOperation {@code true} if this is a stitch operation.
   */
  void onPutBlobError(Exception e, boolean encryptionEnabled, boolean stitchOperation) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      if (stitchOperation) {
        (encryptionEnabled ? stitchEncryptedBlobErrorCount : stitchBlobErrorCount).inc();
      } else {
        (encryptionEnabled ? putEncryptedBlobErrorCount : putBlobErrorCount).inc();
      }
      (encryptionEnabled ? encryptedOperationErrorRate : operationErrorRate).mark();
    }
  }

  /**
   * Update appropriate metrics on a getBlob operation related error.
   * @param e the {@link Exception} associated with the error.
   * @param options the {@link GetBlobOptionsInternal} associated with the request.
   * @param encrypted {@code true} if blob is encrypted, {@code false} otherwise
   */
  void onGetBlobError(Exception e, GetBlobOptionsInternal options, boolean encrypted) {
    if (options.getBlobOptions.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
      onGetBlobInfoError(e, encrypted);
    } else {
      onGetBlobDataError(e, options, encrypted);
    }
  }

  /**
   * Update appropriate metrics on a getBlobInfo operation related error.
   * @param e the {@link Exception} associated with the error.
   * @param encrypted {@code true} if blob is encrypted, {@code false} otherwise
   */
  private void onGetBlobInfoError(Exception e, boolean encrypted) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      if (encrypted) {
        getEncryptedBlobInfoErrorCount.inc();
        encryptedOperationErrorRate.mark();
      } else {
        getBlobInfoErrorCount.inc();
        operationErrorRate.mark();
      }
    }
  }

  /**
   * Update appropriate metrics on a getBlob (Data or All) operation related error.
   * @param e the {@link Exception} associated with the error.
   * @param options the {@link GetBlobOptionsInternal} associated with the request.
   * @param encrypted {@code true} if blob is encrypted, {@code false} otherwise
   */
  private void onGetBlobDataError(Exception e, GetBlobOptionsInternal options, boolean encrypted) {
    onError(e);
    Counter blobErrorCount = encrypted ? getEncryptedBlobErrorCount : getBlobErrorCount;
    Counter blobWithRangeErrorCount = encrypted ? getEncryptedBlobWithRangeErrorCount : getBlobWithRangeErrorCount;
    Counter blobWithSegmentErrorCount =
        encrypted ? getEncryptedBlobWithSegmentErrorCount : getBlobWithSegmentErrorCount;
    Meter operationErrorRateMeter = encrypted ? encryptedOperationErrorRate : operationErrorRate;
    if (RouterUtils.isSystemHealthError(e)) {
      blobErrorCount.inc();
      if (options != null) {
        if (options.getBlobOptions.getRange() != null) {
          blobWithRangeErrorCount.inc();
        }
        if (options.getBlobOptions.hasBlobSegmentIdx()) {
          blobWithSegmentErrorCount.inc();
        }
      }
      operationErrorRateMeter.mark();
    }
  }

  /**
   * Update appropriate metrics on a deleteBlob operation related error.
   * @param e the {@link Exception} associated with the error.
   */
  void onDeleteBlobError(Exception e) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      deleteBlobErrorCount.inc();
      operationErrorRate.mark();
    }
  }

  /**
   * Update appropriate metrics on a updateBlobTtl operation related error.
   * @param e the {@link Exception} associated with the error.
   */
  void onUpdateBlobTtlError(Exception e) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      updateBlobTtlErrorCount.inc();
      operationErrorRate.mark();
    }
  }

  /**
   * Update appropriate metrics on a undeleteBlob operation related error.
   * @param e the {@link Exception} associated with the error.
   */
  void onUndeleteBlobError(Exception e) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      undeleteBlobErrorCount.inc();
      operationErrorRate.mark();
    }
  }

  /**
   * Get {@link NodeLevelMetrics} for a given {@link DataNodeId}. The construction of {@code dataNodeToMetrics}
   * and any {@link DataNodeId} as a key passed to this method are all based on the {@link ClusterMap}, and the
   * key should always exist. If the {@link DataNodeId} as the key does not exist, this will be a programming
   * error.
   * @param dataNodeId The {@link DataNodeId} to be indexed.
   * @return The {@link NodeLevelMetrics}.
   */
  NodeLevelMetrics getDataNodeBasedMetrics(DataNodeId dataNodeId) {
    // If datanode is not yet in the map, create a new NodeLevelMetrics and add it into map. This may happen when
    // dynamically adding new nodes into cluster.
    return dataNodeToMetrics.computeIfAbsent(dataNodeId, k -> new NodeLevelMetrics(metricRegistry,
        dataNodeId.getDatacenterName() + "." + dataNodeId.getHostname() + "." + dataNodeId.getPort()));
  }

  /**
   * A metrics class that tracks at the {@link DataNodeId} level. These metrics are collected based on the operation
   * requests sent to individual {@link DataNodeId}. An operation request is part of an operation, and conveys an actual
   * request to a {@link com.github.ambry.clustermap.ReplicaId} in a {@link DataNodeId}. An operation request can be
   * either for a metadata blob, or for a datachunk.
   */
  public class NodeLevelMetrics {

    // Request rate. For each operation type, this metrics tracks the request rate from the NonBlockingRouter to the
    // remote data node.
    public final Meter putRequestRate;
    public final Meter getBlobInfoRequestRate;
    public final Meter getRequestRate;
    public final Meter deleteRequestRate;
    public final Meter undeleteRequestRate;
    public final Meter ttlUpdateRequestRate;

    // Request latency. For each operation type, this metrics tracks the round-trip time between the NonBlockingRouter
    // and the remote data node.
    public final Histogram putRequestLatencyMs;
    public final Histogram getBlobInfoRequestLatencyMs;
    public final Histogram getRequestLatencyMs;
    public final Histogram deleteRequestLatencyMs;
    public final Histogram undeleteRequestLatencyMs;
    public final Histogram ttlUpdateRequestLatencyMs;

    // Request error count. For each operation type, this metrics tracks the total error count seen by the
    // NonBlockingRouter for the remote data node.
    public final Counter putRequestErrorCount;
    public final Counter getBlobInfoRequestErrorCount;
    public final Counter getRequestErrorCount;
    public final Counter deleteRequestErrorCount;
    public final Counter undeleteRequestErrorCount;
    public final Counter ttlUpdateRequestErrorCount;

    NodeLevelMetrics(MetricRegistry registry, String dataNodeName) {
      // Request rate.
      putRequestRate = registry.meter(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestRate"));
      getBlobInfoRequestRate =
          registry.meter(MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestRate"));
      getRequestRate = registry.meter(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestRate"));
      deleteRequestRate = registry.meter(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestRate"));
      undeleteRequestRate =
          registry.meter(MetricRegistry.name(UndeleteOperation.class, dataNodeName, "UndeleteRequestRate"));
      ttlUpdateRequestRate =
          registry.meter(MetricRegistry.name(TtlUpdateOperation.class, dataNodeName, "TtlUpdateRequestRate"));

      // Request latency.
      putRequestLatencyMs =
          registry.histogram(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestLatencyMs"));
      getBlobInfoRequestLatencyMs = registry.histogram(
          MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestLatencyMs"));
      getRequestLatencyMs =
          registry.histogram(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestLatencyMs"));
      deleteRequestLatencyMs =
          registry.histogram(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestLatencyMs"));
      undeleteRequestLatencyMs =
          registry.histogram(MetricRegistry.name(UndeleteOperation.class, dataNodeName, "UndeleteRequestLatencyMs"));
      ttlUpdateRequestLatencyMs =
          registry.histogram(MetricRegistry.name(TtlUpdateOperation.class, dataNodeName, "TtlUpdateRequestLatencyMs"));

      // Request error count.
      putRequestErrorCount =
          registry.counter(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestErrorCount"));
      getBlobInfoRequestErrorCount = registry.counter(
          MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestErrorCount"));
      getRequestErrorCount =
          registry.counter(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestErrorCount"));
      deleteRequestErrorCount =
          registry.counter(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestErrorCount"));
      undeleteRequestErrorCount =
          registry.counter(MetricRegistry.name(UndeleteOperation.class, dataNodeName, "UndeleteRequestErrorCount"));
      ttlUpdateRequestErrorCount =
          registry.counter(MetricRegistry.name(TtlUpdateOperation.class, dataNodeName, "TtlUpdateRequestErrorCount"));
    }
  }

  /**
   * Tracks the age of a blob at the time of access.
   */
  public static class AgeAtAccessMetrics {
    private final Histogram ageInMs;
    private final Counter lessThanMinuteOld;
    private final Counter betweenMinuteAndHourOld;
    private final Counter betweenHourAndDayOld;
    private final Counter betweenDayAndWeekOld;
    private final Counter betweenWeekAndMonthOld;
    private final Counter betweenMonthAndThreeMonthsOld;
    private final Counter betweenThreeMonthsAndSixMonthsOld;
    private final Counter betweenSixMonthsAndYearOld;
    private final Counter moreThanYearOld;

    /**
     * @param registry the {@link MetricRegistry} to use.
     * @param accessType the type of access (get, delete).
     */
    private AgeAtAccessMetrics(MetricRegistry registry, String accessType) {
      ageInMs = registry.histogram(MetricRegistry.name(NonBlockingRouter.class, accessType, "AgeInMs"));
      lessThanMinuteOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "LessThanMinuteOld"));
      betweenMinuteAndHourOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenMinuteAndHourOld"));
      betweenHourAndDayOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenHourAndDayOld"));
      betweenDayAndWeekOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenDayAndWeekOld"));
      betweenWeekAndMonthOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenWeekAndMonthOld"));
      betweenMonthAndThreeMonthsOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenMonthAndThreeMonthsOld"));
      betweenThreeMonthsAndSixMonthsOld = registry.counter(
          MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenThreeMonthsAndSixMonthsOld"));
      betweenSixMonthsAndYearOld =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "BetweenSixMonthsAndYearOld"));
      moreThanYearOld = registry.counter(MetricRegistry.name(NonBlockingRouter.class, accessType, "MoreThanYearOld"));
    }

    /**
     * Tracks the age of the time of access.
     * @param creationTimeMs the time of creation of the blob.
     */
    void trackAgeAtAccess(long creationTimeMs) {
      long ageInMs = SystemTime.getInstance().milliseconds() - creationTimeMs;
      this.ageInMs.update(ageInMs);
      if (ageInMs < TimeUnit.MINUTES.toMillis(1)) {
        lessThanMinuteOld.inc();
      } else if (ageInMs < TimeUnit.HOURS.toMillis(1)) {
        betweenMinuteAndHourOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(1)) {
        betweenHourAndDayOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(7)) {
        betweenDayAndWeekOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(30)) {
        betweenWeekAndMonthOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(30 * 3)) {
        betweenMonthAndThreeMonthsOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(30 * 6)) {
        betweenThreeMonthsAndSixMonthsOld.inc();
      } else if (ageInMs < TimeUnit.DAYS.toMillis(30 * 12)) {
        betweenSixMonthsAndYearOld.inc();
      } else {
        moreThanYearOld.inc();
      }
    }
  }

  /**
   * Close {@link NonBlockingRouterMetrics} by shutting down scheduler (if present) in this class.
   */
  public void close() {
    if (scheduler != null) {
      shutDownExecutorService(scheduler, 5, TimeUnit.SECONDS);
    }
  }

  /**
   * A thread that helps periodically dump resource-level histogram (with given percentile) into log file.
   */
  private class HistogramDumper implements Runnable {

    @Override
    public void run() {
      double percentile = routerConfig.routerLatencyToleranceQuantile * 100;
      for (Map.Entry<Resource, CachedHistogram> resourceToHistogram : getBlobLocalDcResourceToLatency.entrySet()) {
        Resource resource = resourceToHistogram.getKey();
        CachedHistogram histogram = resourceToHistogram.getValue();
        logger.debug("{} GetBlob local DC latency histogram {}th percentile in ms: {}", resource.toString(), percentile,
            histogram.getCachedValue());
      }
      for (Map.Entry<Resource, CachedHistogram> resourceToHistogram : getBlobCrossDcResourceToLatency.entrySet()) {
        Resource resource = resourceToHistogram.getKey();
        CachedHistogram histogram = resourceToHistogram.getValue();
        logger.trace("{} GetBlob cross DC latency histogram {}th percentile in ms: {}", resource.toString(), percentile,
            histogram.getCachedValue());
      }
      for (Map.Entry<Resource, CachedHistogram> resourceToHistogram : getBlobInfoLocalDcResourceToLatency.entrySet()) {
        Resource resource = resourceToHistogram.getKey();
        CachedHistogram histogram = resourceToHistogram.getValue();
        logger.debug("{} GetBlobInfo local DC latency histogram {}th percentile in ms: {}", resource.toString(),
            percentile, histogram.getCachedValue());
      }
      for (Map.Entry<Resource, CachedHistogram> resourceToHistogram : getBlobInfoCrossDcResourceToLatency.entrySet()) {
        Resource resource = resourceToHistogram.getKey();
        CachedHistogram histogram = resourceToHistogram.getValue();
        logger.trace("{} GetBlobInfo cross DC latency histogram {}th percentile in ms: {}", resource.toString(),
            percentile, histogram.getCachedValue());
      }
    }
  }
}

/**
 * A metrics object that is provided as input to {@link CryptoJob}.
 * </p>
 * It is expected that each type of crypto job will have it's own instance of CryptoJobMetrics and the same instance is
 * used to track all jobs of that type.
 */
class CryptoJobMetrics {
  private static final String JOB_QUEUING_TIME_SUFFIX = "JobQueuingTimeInMs";
  private static final String JOB_PROCESSING_TIME_SUFFIX = "JobProcessingTimeInMs";
  private static final String JOB_CALLBACK_PROCESSING_TIME_SUFFIX = "JobCallbackProcessingTimeInMs";
  private static final String JOB_RESULT_PROCESSING_WAIT_TIME_SUFFIX = "JobResultProcessingWaitTimeInMs";
  private static final String JOB_RESULT_PROCESSING_TIME_SUFFIX = "JobResultProcessingTimeInMs";
  private static final String ROUND_TRIP_TIME_SUFFIX = "RoundTripTimeInMs";

  private static final String OPERATION_RATE_SUFFIX = "Rate";
  private static final String OPERATION_ERROR_SUFFIX = "Error";

  final Histogram jobQueuingTimeInMs;
  final Histogram jobProcessingTimeInMs;
  final Histogram jobCallbackProcessingTimeMs;
  final Histogram jobResultProcessingWaitTimeMs;
  final Histogram jobResultProcessingTimeMs;
  final Histogram roundTripTimeInMs;

  final Meter operationRate;
  final Meter operationErrorRate;

  /**
   * Instantiates {@link CryptoJobMetrics} for {@code requestType} and attaches all the metrics related to the
   * cryptoJob to the given {@code ownerClass}. The metrics are also registered in the provided {@code metricRegistry}.
   * @param ownerClass the {@link Class} that is supposed to own the metrics created by this tracker.
   * @param requestType the type of request for which a tracker is being created.
   * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
   */
  CryptoJobMetrics(Class ownerClass, String requestType, MetricRegistry metricRegistry) {
    jobQueuingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + JOB_QUEUING_TIME_SUFFIX));
    jobProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + JOB_PROCESSING_TIME_SUFFIX));
    jobCallbackProcessingTimeMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + JOB_CALLBACK_PROCESSING_TIME_SUFFIX));
    jobResultProcessingWaitTimeMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + JOB_RESULT_PROCESSING_WAIT_TIME_SUFFIX));
    jobResultProcessingTimeMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + JOB_RESULT_PROCESSING_TIME_SUFFIX));
    roundTripTimeInMs = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + ROUND_TRIP_TIME_SUFFIX));

    operationRate = metricRegistry.meter(MetricRegistry.name(ownerClass, requestType + OPERATION_RATE_SUFFIX));
    operationErrorRate = metricRegistry.meter(
        MetricRegistry.name(ownerClass, requestType + OPERATION_ERROR_SUFFIX + OPERATION_RATE_SUFFIX));
  }
}

/**
 * Construct to support end-to-end metrics tracking for crypto jobs. Usually accompanies a single
 * {@link CryptoJob} i.e. there is a one-to-one mapping b/w a {@link CryptoJob} and a CryptoJobMetricsTracker
 * instance.
 * <p/>
 * A brief description of how the tracker works :-
 * - When an object of type {@link CryptoJob} is instantiated, it is also expected to be associated with a
 *    unique instance of CryptoJobMetricsTracker (tracker). The Tracker will be instantiated either with
 *    EncryptJobMetrics or DecryptJobMetrics
 * - As the cryptoJob passed through various phases, metrics associated with these are updated via methods exposed
 *    by this tracker.
 */
class CryptoJobMetricsTracker {
  private static final int UNINITIATED = -1;
  private final CryptoJobMetrics cryptoJobMetrics;
  private long jobSubmissionTimeMs = UNINITIATED;
  private long jobProcessingStartTimeMs = UNINITIATED;
  private long jobCallbackProcessingStartTimeMs = UNINITIATED;
  private long jobCallbackProcessingCompleteTimeMs = UNINITIATED;
  private long jobResultProcessingStartTimeMs = UNINITIATED;

  CryptoJobMetricsTracker(CryptoJobMetrics cryptoJobMetrics) {
    this.cryptoJobMetrics = cryptoJobMetrics;
  }

  /**
   * Initiates the timer for the crypto job on submission
   */
  void onJobSubmission() {
    cryptoJobMetrics.operationRate.mark();
    jobSubmissionTimeMs = System.currentTimeMillis();
  }

  /**
   * Notifies that the job processing has started for the corresponding CryptoJob
   */
  void onJobProcessingStart() {
    jobProcessingStartTimeMs = System.currentTimeMillis();
    cryptoJobMetrics.jobQueuingTimeInMs.update(jobProcessingStartTimeMs - jobSubmissionTimeMs);
  }

  /**
   * Notifies that the job processing has completed for the corresponding CryptoJob
   */
  void onJobProcessingComplete() {
    cryptoJobMetrics.jobProcessingTimeInMs.update(System.currentTimeMillis() - jobProcessingStartTimeMs);
  }

  /**
   * Signifies that the job callback processing has started
   */
  void onJobCallbackProcessingStart() {
    jobCallbackProcessingStartTimeMs = System.currentTimeMillis();
  }

  /**
   * Signifies that the job callback processing has completed
   */
  void onJobCallbackProcessingComplete() {
    jobCallbackProcessingCompleteTimeMs = System.currentTimeMillis();
    cryptoJobMetrics.jobCallbackProcessingTimeMs.update(
        jobCallbackProcessingCompleteTimeMs - jobCallbackProcessingStartTimeMs);
  }

  /**
   * Signifies that the job result processing has started
   */
  void onJobResultProcessingStart() {
    jobResultProcessingStartTimeMs = System.currentTimeMillis();
    if (jobCallbackProcessingCompleteTimeMs != UNINITIATED) {
      cryptoJobMetrics.jobResultProcessingWaitTimeMs.update(
          jobResultProcessingStartTimeMs - jobCallbackProcessingCompleteTimeMs);
    }
  }

  /**
   * Signifies that the job callback processing has completed
   */
  void onJobResultProcessingComplete() {
    long jobResultProcessingCompleteTimeMs = System.currentTimeMillis();
    cryptoJobMetrics.jobResultProcessingTimeMs.update(
        jobResultProcessingCompleteTimeMs - jobResultProcessingStartTimeMs);
    cryptoJobMetrics.roundTripTimeInMs.update(jobResultProcessingCompleteTimeMs - jobSubmissionTimeMs);
  }

  /**
   * Notifies an error for the corresponding CryptoJob
   */
  void incrementOperationError() {
    cryptoJobMetrics.operationErrorRate.mark();
  }
}

