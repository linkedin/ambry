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
import java.util.concurrent.atomic.AtomicInteger;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link NonBlockingRouter}-specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link NonBlockingRouter} to the provided {@link MetricRegistry}
 */
public class NonBlockingRouterMetrics {
  private final MetricRegistry metricRegistry;
  // @todo: Ensure all metrics here get updated appropriately.
  // @todo: chunk filling rate metrics.
  // @todo: More metrics for the RequestResponse handling (poll, handleResponse etc.)

  // Operation rate.
  public final Meter putBlobOperationRate;
  public final Meter getBlobInfoOperationRate;
  public final Meter getBlobOperationRate;
  public final Meter deleteBlobOperationRate;
  public final Meter operationQueuingRate;
  public final Meter operationDequeuingRate;

  // Latency.
  public final Histogram putBlobOperationLatencyMs;
  public final Histogram putChunkOperationLatencyMs;
  public final Histogram getBlobInfoOperationLatencyMs;
  public final Histogram getBlobOperationLatencyMs;
  public final Histogram deleteBlobOperationLatencyMs;
  public final Histogram routerRequestLatencyMs;

  // Operation error count.
  public final Counter putBlobErrorCount;
  public final Counter getBlobInfoErrorCount;
  public final Counter getBlobErrorCount;
  public final Counter deleteBlobErrorCount;
  public final Counter operationAbortCount;

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
  public final Counter unknownReplicaResponseError;
  public final Counter closeErrorCount;
  public final Counter unknownErrorCountForOperation;

  // Misc metrics.
  public final Meter operationErrorRate;
  public final Counter slippedPutSuccessCount;
  public final Counter ignoredResponseCount;
  public Gauge<Long> chunkFillerThreadRunning;
  public Gauge<Long> requestResponseHandlerThreadRunning;
  public Gauge<Integer> activeOperations;

  // Map that stores dataNode-level metrics.
  private final Map<DataNodeId, NodeLevelMetrics> dataNodeToMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public NonBlockingRouterMetrics(ClusterMap clusterMap) {
    metricRegistry = clusterMap.getMetricRegistry();

    // Operation Rate
    putBlobOperationRate = metricRegistry.meter(MetricRegistry.name(PutManager.class, "PutBlobRequestArrivalRate"));
    getBlobInfoOperationRate =
        metricRegistry.meter(MetricRegistry.name(GetManager.class, "GetBlobInfoRequestArrivalRate"));
    getBlobOperationRate = metricRegistry.meter(MetricRegistry.name(GetManager.class, "GetBlobRequestArrivalRate"));
    deleteBlobOperationRate =
        metricRegistry.meter(MetricRegistry.name(DeleteManager.class, "DeleteBlobRequestArrivalRate"));
    operationQueuingRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationQueuingRate"));
    operationDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationDequeuingRate"));

    // Latency
    putBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutBlobOperationLatencyMs"));
    putChunkOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "PutChunkOperationLatencyMs"));
    getBlobInfoOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobInfoOperationLatencyMs"));
    getBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(GetManager.class, "GetBlobOperationLatencyMs"));
    deleteBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteManager.class, "DeleteBlobOperationLatencyMs"));
    routerRequestLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(NonBlockingRouter.class, "RouterRequestLatencyMs"));

    // Operation error count.
    putBlobErrorCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "PutBlobErrorCount"));
    getBlobInfoErrorCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "GetBlobInfoErrorCount"));
    getBlobErrorCount = metricRegistry.counter(MetricRegistry.name(GetManager.class, "GetBlobErrorCount"));
    deleteBlobErrorCount = metricRegistry.counter(MetricRegistry.name(DeleteManager.class, "DeleteBlobErrorCount"));
    operationAbortCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "OperationAbortCount"));

    // Count for various errors.
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
    unknownReplicaResponseError =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "UnknownReplicaResponseError"));
    closeErrorCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "CloseErrorCount"));
    unknownErrorCountForOperation =
        metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "UnknownErrorCountForOperation"));

    // Misc metrics.
    operationErrorRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "OperationErrorArrivalRate"));
    slippedPutSuccessCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "SlippedPutSuccessCount"));
    ignoredResponseCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "IgnoredRequestCount"));

    // Track metrics at the DataNode level.
    dataNodeToMetrics = new HashMap<DataNodeId, NodeLevelMetrics>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      String dataNodeName = dataNodeId.getDatacenterName() + "." + dataNodeId.getHostname() + "." + Integer
          .toString(dataNodeId.getPort());
      dataNodeToMetrics.put(dataNodeId, new NodeLevelMetrics(metricRegistry, dataNodeName));
    }
    // A null key corresponds to the NodeLevelMetrics for all unknown DataNodeId.
    dataNodeToMetrics.put(null, new NodeLevelMetrics(metricRegistry, "UnknownDataNode.UnknownHostName.UnknownPort"));
  }

  public void initializeOperationControllerMetrics(final Thread requestResponseHandlerThread) {
    requestResponseHandlerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return requestResponseHandlerThread.isAlive() ? 1L : 0L;
      }
    };
    metricRegistry
        .register(MetricRegistry.name(NonBlockingRouter.class, requestResponseHandlerThread.getName() + "Running"),
            requestResponseHandlerThreadRunning);
  }

  public void initializePutManagerMetrics(final Thread chunkFillerThread) {
    chunkFillerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return chunkFillerThread.isAlive() ? 1L : 0L;
      }
    };
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, chunkFillerThread.getName() + "Running"),
        chunkFillerThreadRunning);
  }

  public void initializeNumActiveOperationsMetrics(final AtomicInteger currentOperationsCount) {
    activeOperations = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return currentOperationsCount.get();
      }
    };
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "NumActiveOperations"), activeOperations);
  }

  /**
   * Count error based on error type.
   * <p/>
   * This method should be called when an {@code Operation} is completed or aborted.
   * @param exception The exception to be counted.
   */
  void countError(Exception exception) {
    operationErrorRate.mark();
    if (exception instanceof RouterException) {
      switch (((RouterException) exception).getErrorCode()) {
        case AmbryUnavailable:
          ambryUnavailableErrorCount.inc();
          break;
        case InvalidBlobId:
          invalidBlobIdErrorCount.inc();
          break;
        case InvalidPutArgument:
          invalidPutArgumentErrorCount.inc();
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
        case BlobTooLarge:
          blobTooLargeErrorCount.inc();
          break;
        case BadInputChannel:
          blobTooLargeErrorCount.inc();
          break;
        case InsufficientCapacity:
          insufficientCapacityErrorCount.inc();
          break;
        case BlobDeleted:
          blobDeletedErrorCount.inc();
          break;
        case BlobDoesNotExist:
          blobDoesNotExistErrorCount.inc();
          break;
        case BlobExpired:
          blobExpiredErrorCount.inc();
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
   * Get {@link NodeLevelMetrics} for a given {@link DataNodeId}. If the {@link DataNodeId} as the key does not exist,
   * a {@link NodeLevelMetrics} corresponding to an unknown {@link DataNodeId} will be returned.
   * @param dataNodeId The {@link DataNodeId} to be indexed.
   * @return The {@link NodeLevelMetrics}.
   */
  NodeLevelMetrics getDataNodeBasedMetrics(DataNodeId dataNodeId) {
    return dataNodeToMetrics.containsKey(dataNodeId) ? dataNodeToMetrics.get(dataNodeId) : dataNodeToMetrics.get(null);
  }

  /**
   * A metrics class that tracks at the {@link DataNodeId} level. These metrics are collected based on the operation
   * requests sent to individual {@link DataNodeId}. An operation request is part of an operation, and conveys an actual
   * request to a {@link com.github.ambry.clustermap.ReplicaId} in a {@link DataNodeId}. An operation request can be
   * either for a metadata blob, or for a datachunk.
   */
  public class NodeLevelMetrics {

    // Request rate.
    public final Meter putRequestRate;
    public final Meter getBlobInfoRequestRate;
    public final Meter getRequestRate;
    public final Meter deleteRequestRate;

    // Request latency.
    public final Histogram putRequestLatencyMs;
    public final Histogram getBlobInfoRequestLatencyMs;
    public final Histogram getRequestLatencyMs;
    public final Histogram deleteRequestLatencyMs;

    // Request error count.
    public final Counter putRequestErrorCount;
    public final Counter getBlobInfoRequestErrorCount;
    public final Counter getRequestErrorCount;
    public final Counter deleteRequestErrorCount;

    // Timed-out request count for each operation type.
    public final Counter putRequestTimeoutCount;
    public final Counter getBlobInfoRequestTimeoutCount;
    public final Counter getRequestTimeoutCount;
    public final Counter deleteRequestTimeoutCount;

    // Count for various errors at request level.
    public final Counter ambryUnavailableErrorCountForRequest;
    public final Counter invalidBlobIdErrorCountForRequest;
    public final Counter invalidPutArgumentErrorCountForRequest;
    public final Counter requestTimedOutErrorCountForRequest;
    public final Counter routerClosedErrorCountForRequest;
    public final Counter unexpectedInternalErrorCountForRequest;
    public final Counter blobTooLargeErrorCountForRequest;
    public final Counter badInputChannelErrorCountForRequest;
    public final Counter insufficientCapacityErrorCountForRequest;
    public final Counter blobDeletedErrorCountForRequest;
    public final Counter blobDoesNotExistErrorCountForRequest;
    public final Counter blobExpiredErrorCountForRequest;
    public final Counter unknownErrorCountForRequest;

    // Misc metrics at the DataNode level.
    public final Meter requestErrorRate;
    public final Counter unknownReplicaResponseError;

    NodeLevelMetrics(MetricRegistry registry, String dataNodeName) {
      // Request rate.
      putRequestRate = registry.meter(MetricRegistry.name(PutManager.class, dataNodeName, "PutRequestArrivalRate"));
      getBlobInfoRequestRate =
          registry.meter(MetricRegistry.name(GetManager.class, dataNodeName, "GetBlobInfoRequestArrivalRate"));
      getRequestRate = registry.meter(MetricRegistry.name(GetManager.class, dataNodeName, "GetRequestArrivalRate"));
      deleteRequestRate =
          registry.meter(MetricRegistry.name(DeleteManager.class, dataNodeName, "DeleteRequestArrivalRate"));

      // Request latency.
      putRequestLatencyMs =
          registry.histogram(MetricRegistry.name(PutManager.class, dataNodeName, "PutRequestLatencyMs"));
      getBlobInfoRequestLatencyMs =
          registry.histogram(MetricRegistry.name(GetManager.class, dataNodeName, "GetBlobInfoRequestLatencyMs"));
      getRequestLatencyMs =
          registry.histogram(MetricRegistry.name(GetManager.class, dataNodeName, "GetRequestLatencyMs"));
      deleteRequestLatencyMs =
          registry.histogram(MetricRegistry.name(DeleteManager.class, dataNodeName, "DeleteRequestLatencyMs"));

      // Request error count.
      putRequestErrorCount =
          registry.counter(MetricRegistry.name(PutManager.class, dataNodeName, "PutRequestErrorCount"));
      getBlobInfoRequestErrorCount =
          registry.counter(MetricRegistry.name(GetManager.class, dataNodeName, "GetBlobInfoRequestErrorCount"));
      getRequestErrorCount =
          registry.counter(MetricRegistry.name(GetManager.class, dataNodeName, "GetRequestErrorCount"));
      deleteRequestErrorCount =
          registry.counter(MetricRegistry.name(DeleteManager.class, dataNodeName, "DeleteRequestErrorCount"));

      // Timed-out request count.
      putRequestTimeoutCount =
          registry.counter(MetricRegistry.name(PutManager.class, dataNodeName, "PutRequestTimeoutCount"));
      getBlobInfoRequestTimeoutCount =
          registry.counter(MetricRegistry.name(GetManager.class, dataNodeName, "GetBlobInfoRequestTimeoutCount"));
      getRequestTimeoutCount =
          registry.counter(MetricRegistry.name(GetManager.class, dataNodeName, "GetRequestTimeoutCount"));
      deleteRequestTimeoutCount =
          registry.counter(MetricRegistry.name(DeleteManager.class, dataNodeName, "DeleteRequestTimeoutCount"));

      // Count for various errors at request level.
      ambryUnavailableErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "AmbryUnavailableErrorForRequest"));
      invalidBlobIdErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "InvalidBlobIdErrorForRequest"));
      invalidPutArgumentErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "InvalidPutArgumentErrorForRequest"));
      requestTimedOutErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "RequestTimedOutErrorForRequest"));
      routerClosedErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "RouterClosedErrorForRequest"));
      unexpectedInternalErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "UnexpectedInternalErrorForRequest"));
      blobTooLargeErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "BlobTooLargeErrorForRequest"));
      badInputChannelErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "BadInputChannel"));
      insufficientCapacityErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "InsufficientCapacityErrorForRequest"));
      blobDeletedErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "BlobDeletedErrorForRequest"));
      blobDoesNotExistErrorCountForRequest = registry
          .counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "BlobDoesNotExistErrorForRequest"));
      blobExpiredErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "BlobExpiredErrorForRequest"));
      unknownErrorCountForRequest =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "UnknownRouterErrorForRequest"));

      // Misc metrics at the DataNode level.
      requestErrorRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "RequestErrorArrivalRate"));
      unknownReplicaResponseError =
          registry.counter(MetricRegistry.name(NonBlockingRouter.class, dataNodeName, "UnknownReplicaResponseError"));
    }

    /**
     * Count the {@link RouterException} caused by a request (or its response). This {@link RouterException}
     * may not be the final {@link RouterException} accompanied with the {@code Operation}, but such metrics
     * can assist a root-cause analysis at the {@link DataNodeId} level.
     * <p/>
     * This method should be called when a request is either completed or timed out.
     * @param error The {@link RouterErrorCode} of the exception to be counted.
     */
    void countError(RouterErrorCode error) {
      requestErrorRate.mark();
      switch (error) {
        case AmbryUnavailable:
          ambryUnavailableErrorCountForRequest.inc();
          break;
        case InvalidBlobId:
          invalidBlobIdErrorCountForRequest.inc();
          break;
        case InvalidPutArgument:
          invalidPutArgumentErrorCountForRequest.inc();
          break;
        case OperationTimedOut:
          requestTimedOutErrorCountForRequest.inc();
          break;
        case RouterClosed:
          routerClosedErrorCountForRequest.inc();
          break;
        case UnexpectedInternalError:
          unexpectedInternalErrorCountForRequest.inc();
          break;
        case BlobTooLarge:
          blobTooLargeErrorCountForRequest.inc();
          break;
        case BadInputChannel:
          badInputChannelErrorCountForRequest.inc();
          break;
        case InsufficientCapacity:
          insufficientCapacityErrorCountForRequest.inc();
          break;
        case BlobDeleted:
          blobDeletedErrorCountForRequest.inc();
          break;
        case BlobDoesNotExist:
          blobDoesNotExistErrorCountForRequest.inc();
          break;
        case BlobExpired:
          blobExpiredErrorCountForRequest.inc();
          break;
        default:
          unknownErrorCountForRequest.inc();
          break;
      }
    }
  }
}