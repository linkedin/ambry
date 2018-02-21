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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.utils.SystemTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * {@link NonBlockingRouter}-specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link NonBlockingRouter} to the provided {@link MetricRegistry}
 */
public class NonBlockingRouterMetrics {
  private final MetricRegistry metricRegistry;

  // Operation rate.
  public final Meter putBlobOperationRate;
  public final Meter getBlobInfoOperationRate;
  public final Meter getBlobOperationRate;
  public final Meter getBlobWithRangeOperationRate;
  public final Meter deleteBlobOperationRate;
  public final Meter putEncryptedBlobOperationRate;
  public final Meter getEncryptedBlobInfoOperationRate;
  public final Meter getEncryptedBlobOperationRate;
  public final Meter getEncryptedBlobWithRangeOperationRate;
  public final Meter operationQueuingRate;
  public final Meter operationDequeuingRate;
  public final Meter getBlobNotOriginateLocalOperationRate;
  public final Meter deleteBlobNotOriginateLocalOperationRate;

  // Latency.
  public final Histogram putBlobOperationLatencyMs;
  public final Histogram putChunkOperationLatencyMs;
  public final Histogram getBlobInfoOperationLatencyMs;
  public final Histogram getBlobOperationLatencyMs;
  public final Histogram getBlobOperationTotalTimeMs;
  public final Histogram putEncryptedBlobOperationLatencyMs;
  public final Histogram putEncryptedChunkOperationLatencyMs;
  public final Histogram getEncryptedBlobInfoOperationLatencyMs;
  public final Histogram getEncryptedBlobOperationLatencyMs;
  public final Histogram getEncryptedBlobOperationTotalTimeMs;
  public final Histogram deleteBlobOperationLatencyMs;
  public final Histogram routerRequestLatencyMs;

  // Operation error count.
  public final Counter putBlobErrorCount;
  public final Counter getBlobInfoErrorCount;
  public final Counter getBlobErrorCount;
  public final Counter getBlobWithRangeErrorCount;
  public final Counter putEncryptedBlobErrorCount;
  public final Counter getEncryptedBlobInfoErrorCount;
  public final Counter getEncryptedBlobErrorCount;
  public final Counter getEncryptedBlobWithRangeErrorCount;
  public final Counter deleteBlobErrorCount;
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
  public final Histogram putManagerHandleResponseTimeMs;
  public final Histogram getManagerHandleResponseTimeMs;
  public final Histogram deleteManagerHandleResponseTimeMs;
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
  public Gauge<Long> chunkFillerThreadRunning;
  public Gauge<Long> requestResponseHandlerThreadRunning;

  // metrics for tracking blob sizes and chunking.
  public final Histogram putBlobSizeBytes;
  public final Histogram putBlobChunkCount;
  public final Histogram getBlobSizeBytes;
  public final Histogram getBlobChunkCount;
  public final Histogram getBlobWithRangeSizeBytes;
  public final Histogram getBlobWithRangeTotalBlobSizeBytes;
  public final Counter simpleBlobPutCount;
  public final Counter simpleBlobGetCount;
  public final Counter compositeBlobPutCount;
  public final Counter compositeBlobGetCount;

  // AdaptiveOperationTracker metrics
  public final Histogram getBlobLocalColoLatencyMs;
  public final Histogram getBlobCrossColoLatencyMs;
  public final Counter getBlobPastDueCount;

  public final Histogram getBlobInfoLocalColoLatencyMs;
  public final Histogram getBlobInfoCrossColoLatencyMs;
  public final Counter getBlobInfoPastDueCount;

  // Workload characteristics
  public final AgeAtAccessMetrics ageAtGet;
  public final AgeAtAccessMetrics ageAtDelete;

  // Crypto job metrics
  public final CryptoJobMetrics encryptJobMetrics;
  public final CryptoJobMetrics decryptJobMetrics;

  // Map that stores dataNode-level metrics.
  private final Map<DataNodeId, NodeLevelMetrics> dataNodeToMetrics;

  public NonBlockingRouterMetrics(ClusterMap clusterMap) {
    metricRegistry = clusterMap.getMetricRegistry();

    // Operation Rate.
    putBlobOperationRate = metricRegistry.meter(MetricRegistry.name(PutOperation.class, "PutBlobOperationRate"));
    getBlobInfoOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoOperationRate"));
    getBlobOperationRate = metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationRate"));
    getBlobWithRangeOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeOperationRate"));
    putEncryptedBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobOperationRate"));
    getEncryptedBlobInfoOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobInfoOperation.class, "GetEncryptedBlobInfoOperationRate"));
    getEncryptedBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobOperationRate"));
    getEncryptedBlobWithRangeOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithRangeOperationRate"));
    deleteBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobOperationRate"));
    operationQueuingRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationQueuingRate"));
    operationDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationDequeuingRate"));
    getBlobNotOriginateLocalOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobOperation.class, "GetBlobNotOriginateLocalOperationRate"));
    deleteBlobNotOriginateLocalOperationRate =
        metricRegistry.meter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobNotOriginateLocalOperationRate"));

    // Latency.
    putBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutBlobOperationLatencyMs"));
    putChunkOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutChunkOperationLatencyMs"));
    getBlobInfoOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoOperationLatencyMs"));
    getBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationLatencyMs"));
    getBlobOperationTotalTimeMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobOperationTotalTimeMs"));
    putEncryptedBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobOperationLatencyMs"));
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
    routerRequestLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(NonBlockingRouter.class, "RouterRequestLatencyMs"));

    // Operation error count.
    putBlobErrorCount = metricRegistry.counter(MetricRegistry.name(PutOperation.class, "PutBlobErrorCount"));
    getBlobInfoErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "GetBlobInfoErrorCount"));
    getBlobErrorCount = metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetBlobErrorCount"));
    getBlobWithRangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeErrorCount"));
    putEncryptedBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(PutOperation.class, "PutEncryptedBlobErrorCount"));
    getEncryptedBlobInfoErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "GetEncryptedBlobInfoErrorCount"));
    getEncryptedBlobErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobErrorCount"));
    getEncryptedBlobWithRangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "GetEncryptedBlobWithRangeErrorCount"));
    deleteBlobErrorCount = metricRegistry.counter(MetricRegistry.name(DeleteOperation.class, "DeleteBlobErrorCount"));
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
    putManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutManagerHandleResponseTimeMs"));
    getManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetManagerHandleResponseTimeMs"));
    deleteManagerHandleResponseTimeMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteManager.class, "DeleteManagerHandleResponseTimeMs"));
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

    // metrics to track blob sizes and chunking.
    putBlobSizeBytes = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutBlobSizeBytes"));
    putBlobChunkCount = metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutBlobChunkCount"));
    getBlobSizeBytes = metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobSizeBytes"));
    getBlobChunkCount = metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobChunkCount"));
    getBlobWithRangeSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeSizeBytes"));
    getBlobWithRangeTotalBlobSizeBytes =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "GetBlobWithRangeTotalBlobSizeBytes"));
    simpleBlobPutCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "SimpleBlobPutCount"));
    simpleBlobGetCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "SimpleBlobGetCount"));
    compositeBlobPutCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "CompositeBlobPutCount"));
    compositeBlobGetCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "CompositeBlobGetCount"));

    // Track metrics at the DataNode level.
    dataNodeToMetrics = new HashMap<>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      String dataNodeName = dataNodeId.getDatacenterName() + "." + dataNodeId.getHostname() + "." + Integer.toString(
          dataNodeId.getPort());
      dataNodeToMetrics.put(dataNodeId, new NodeLevelMetrics(metricRegistry, dataNodeName));
    }

    // AdaptiveOperationTracker trackers
    getBlobLocalColoLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "LocalColoLatencyMs"));
    getBlobCrossColoLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobOperation.class, "CrossColoLatencyMs"));
    getBlobPastDueCount = metricRegistry.counter(MetricRegistry.name(GetBlobOperation.class, "PastDueCount"));

    getBlobInfoLocalColoLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "LocalColoLatencyMs"));
    getBlobInfoCrossColoLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobInfoOperation.class, "CrossColoLatencyMs"));
    getBlobInfoPastDueCount = metricRegistry.counter(MetricRegistry.name(GetBlobInfoOperation.class, "PastDueCount"));

    // Workload
    ageAtGet = new AgeAtAccessMetrics(metricRegistry, "OnGet");
    ageAtDelete = new AgeAtAccessMetrics(metricRegistry, "OnDelete");

    // Encrypt/Decrypt job metrics
    encryptJobMetrics = new CryptoJobMetrics(PutOperation.class, "Encrypt", metricRegistry);
    decryptJobMetrics = new CryptoJobMetrics(GetOperation.class, "Decrypt", metricRegistry);
  }

  /**
   * Initializes a {@link Gauge} metric for the status of {@code RequestResponseHandlerThread} of an
   * {@link com.github.ambry.router.NonBlockingRouter.OperationController}, to indicate if it is running
   * or not.
   * @param requestResponseHandlerThread The {@code RequestResponseHandlerThread} of which the status is
   *                                     to be monitored.
   */
  public void initializeOperationControllerMetrics(final Thread requestResponseHandlerThread) {
    requestResponseHandlerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return requestResponseHandlerThread.isAlive() ? 1L : 0L;
      }
    };
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
    chunkFillerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return chunkFillerThread.isAlive() ? 1L : 0L;
      }
    };
    metricRegistry.register(MetricRegistry.name(PutManager.class, chunkFillerThread.getName() + "Running"),
        chunkFillerThreadRunning);
  }

  /**
   * Initializes a {@link Gauge} metric to monitor the number of running
   * {@link com.github.ambry.router.NonBlockingRouter.OperationController} of a {@link NonBlockingRouter}.
   * @param currentOperationsCount The counter of {@link com.github.ambry.router.NonBlockingRouter.OperationController}.
   */
  public void initializeNumActiveOperationsMetrics(final AtomicInteger currentOperationsCount,
      final AtomicInteger currentBackgroundOperationsCount) {
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "NumActiveOperations"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return currentOperationsCount.get();
      }
    });
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "NumActiveBackgroundOperations"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return currentBackgroundOperationsCount.get();
          }
        });
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
   * @param encryptionEnabled {@code true} if encrpytion was enabled for this operation. {@code false} otherwise
   */
  void onPutBlobError(Exception e, boolean encryptionEnabled) {
    onError(e);
    if (RouterUtils.isSystemHealthError(e)) {
      if (encryptionEnabled) {
        putEncryptedBlobErrorCount.inc();
        encryptedOperationErrorRate.mark();
      } else {
        putBlobErrorCount.inc();
        operationErrorRate.mark();
      }
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
    Meter operationErrorRateMeter = encrypted ? encryptedOperationErrorRate : operationErrorRate;
    if (RouterUtils.isSystemHealthError(e)) {
      blobErrorCount.inc();
      if (options != null && options.getBlobOptions.getRange() != null) {
        blobWithRangeErrorCount.inc();
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
   * Get {@link NodeLevelMetrics} for a given {@link DataNodeId}. The construction of {@code dataNodeToMetrics}
   * and any {@link DataNodeId} as a key passed to this method are all based on the {@link ClusterMap}, and the
   * key should always exist. If the {@link DataNodeId} as the key does not exist, this will be a programming
   * error.
   * @param dataNodeId The {@link DataNodeId} to be indexed.
   * @return The {@link NodeLevelMetrics}.
   */
  NodeLevelMetrics getDataNodeBasedMetrics(DataNodeId dataNodeId) {
    return dataNodeToMetrics.get(dataNodeId);
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

    // Request latency. For each operation type, this metrics tracks the round-trip time between the NonBlockingRouter
    // and the remote data node.
    public final Histogram putRequestLatencyMs;
    public final Histogram getBlobInfoRequestLatencyMs;
    public final Histogram getRequestLatencyMs;
    public final Histogram deleteRequestLatencyMs;

    // Request error count. For each operation type, this metrics tracks the total error count seen by the
    // NonBlockingRouter for the remote data node.
    public final Counter putRequestErrorCount;
    public final Counter getBlobInfoRequestErrorCount;
    public final Counter getRequestErrorCount;
    public final Counter deleteRequestErrorCount;

    NodeLevelMetrics(MetricRegistry registry, String dataNodeName) {
      // Request rate.
      putRequestRate = registry.meter(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestRate"));
      getBlobInfoRequestRate =
          registry.meter(MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestRate"));
      getRequestRate = registry.meter(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestRate"));
      deleteRequestRate = registry.meter(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestRate"));

      // Request latency.
      putRequestLatencyMs =
          registry.histogram(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestLatencyMs"));
      getBlobInfoRequestLatencyMs = registry.histogram(
          MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestLatencyMs"));
      getRequestLatencyMs =
          registry.histogram(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestLatencyMs"));
      deleteRequestLatencyMs =
          registry.histogram(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestLatencyMs"));

      // Request error count.
      putRequestErrorCount =
          registry.counter(MetricRegistry.name(PutOperation.class, dataNodeName, "PutRequestErrorCount"));
      getBlobInfoRequestErrorCount = registry.counter(
          MetricRegistry.name(GetBlobInfoOperation.class, dataNodeName, "GetBlobInfoRequestErrorCount"));
      getRequestErrorCount =
          registry.counter(MetricRegistry.name(GetBlobOperation.class, dataNodeName, "GetRequestErrorCount"));
      deleteRequestErrorCount =
          registry.counter(MetricRegistry.name(DeleteOperation.class, dataNodeName, "DeleteRequestErrorCount"));
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

