/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * OperationController is the scaling unit for the NonBlockingRouter. The NonBlockingRouter can have multiple
 * OperationControllers. Any operation submitted to the NonBlockingRouter will be submitted to one of the
 * OperationControllers. A worker thread (the RequestResponseHandler thread) will poll The OperationController for
 * requests to be sent and will notify it on receiving responses. The OperationController in turn makes use of the
 * {@link PutManager}, {@link GetManager} and {@link DeleteManager} to perform puts, gets and deletes,
 * respectively. A {@link NetworkClient} is used to interact with the network.
 */
public class OperationController implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OperationController.class);
  protected final RouterCallback routerCallback;
  final PutManager putManager;
  final GetManager getManager;
  final DeleteManager deleteManager;
  final TtlUpdateManager ttlUpdateManager;
  final UndeleteManager undeleteManager;
  final ReplicateBlobManager replicateBlobManager;
  private final NetworkClient networkClient;
  private final ResponseHandler responseHandler;
  private final RouterConfig routerConfig;
  private final Thread requestResponseHandlerThread;
  private final CountDownLatch shutDownLatch = new CountDownLatch(1);
  private final List<BackgroundDeleteRequest> backgroundDeleteRequests = new ArrayList<>();

  protected final NonBlockingRouterMetrics routerMetrics;
  protected final NonBlockingRouter nonBlockingRouter;

  /**
   * Constructs an OperationController
   * @param suffix the suffix to associate with the thread names of this OperationController
   * @param defaultPartitionClass the default partition class to choose partitions from (if none is found in the
   *                              container config). Can be {@code null} if no affinity is required for the puts for
   *                              which the container contains no partition class hints.
   * @param accountService the {@link AccountService} to use.
   * @param networkClientFactory the {@link NetworkClientFactory} used by the {@link OperationController} to create
   *                             instances of {@link NetworkClient}.
   * @param clusterMap the cluster map for the cluster.
   * @param routerConfig the configs for the router.
   * @param responseHandler {@link ResponseHandler} object.
   * @param notificationSystem the notification system to use to notify about blob creations and deletions.
   * @param routerMetrics the metrics for the router.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the time instance.
   * @param nonBlockingRouter {@link NonBlockingRouter} object.
   * @throws IOException if the network components could not be created.
   */
  public OperationController(String suffix, String defaultPartitionClass, AccountService accountService,
      NetworkClientFactory networkClientFactory, ClusterMap clusterMap, RouterConfig routerConfig,
      ResponseHandler responseHandler, NotificationSystem notificationSystem, NonBlockingRouterMetrics routerMetrics,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time,
      NonBlockingRouter nonBlockingRouter) throws IOException {
    networkClient = networkClientFactory.getNetworkClient();
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.responseHandler = responseHandler;
    this.nonBlockingRouter = nonBlockingRouter;
    // Warm up connections to dataNodes in local and remote DCs.
    List<ResponseInfo> responseInfos = Collections.synchronizedList(new ArrayList<>());
    String localDatacenter = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    Map<Boolean, List<DataNodeId>> localAndRemoteNodes = clusterMap.getDataNodeIds()
        .stream()
        // ignore any in-process "data nodes" without TCP ports
        .filter(dataNodeId -> dataNodeId.getPort() != DataNodeId.UNKNOWN_PORT)
        .collect(Collectors.partitioningBy(dataNodeId -> localDatacenter.equals(dataNodeId.getDatacenterName())));
    logger.info("Warming up local datacenter connections to {} nodes. Connections warmup percentage: {}%.",
        localAndRemoteNodes.get(true).size(), routerConfig.routerConnectionsLocalDcWarmUpPercentage);
    networkClient.warmUpConnections(localAndRemoteNodes.get(true),
        routerConfig.routerConnectionsLocalDcWarmUpPercentage, routerConfig.routerConnectionsWarmUpTimeoutMs,
        responseInfos);
    logger.info("Warming up remote datacenter connections to {} nodes. Connections warmup percentage: {}%.",
        localAndRemoteNodes.get(false).size(), routerConfig.routerConnectionsRemoteDcWarmUpPercentage);
    networkClient.warmUpConnections(localAndRemoteNodes.get(false),
        routerConfig.routerConnectionsRemoteDcWarmUpPercentage, routerConfig.routerConnectionsWarmUpTimeoutMs,
        responseInfos);
    // Update ResponseHandler immediately if connections lost to certain nodes.
    // Note: If we return from networkClient.warmUpConnections() due to time out, 'responseInfo' list can be modified in
    // parallel by Netty threads. To avoid ConcurrentModificationException, use forEach to go over the list instead of
    // for-each loop since latter uses an iterator which is not protected by mutex of Collections.synchronisedList().
    responseInfos.forEach(responseInfo -> {
      if (responseInfo.getRequestInfo() == null) {
        responseHandler.onConnectionTimeout(responseInfo.getDataNode());
      }
    });
    routerCallback = new RouterCallback(networkClient, backgroundDeleteRequests);
    putManager =
        new PutManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics, routerCallback,
            suffix, kms, cryptoService, cryptoJobHandler, accountService, time, defaultPartitionClass, nonBlockingRouter);
    getManager =
        new GetManager(clusterMap, responseHandler, routerConfig, routerMetrics, routerCallback, kms, cryptoService,
            cryptoJobHandler, time, nonBlockingRouter.getBlobMetadataCache(), nonBlockingRouter);
    deleteManager =
        new DeleteManager(clusterMap, responseHandler, accountService, notificationSystem, routerConfig, routerMetrics,
            routerCallback, time, nonBlockingRouter);
    ttlUpdateManager =
        new TtlUpdateManager(clusterMap, responseHandler, notificationSystem, accountService, routerConfig,
            routerMetrics, time, nonBlockingRouter);
    replicateBlobManager =
        new ReplicateBlobManager(clusterMap, responseHandler, accountService, notificationSystem, routerConfig,
            routerMetrics, time, nonBlockingRouter);
    undeleteManager = new UndeleteManager(clusterMap, responseHandler, notificationSystem, accountService, routerConfig,
        routerMetrics, time, nonBlockingRouter);
    requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread-" + suffix, this, true);
  }

  /**
   * Start the thread to run the operation controller
   */
  protected void start() {
    requestResponseHandlerThread.start();
    routerMetrics.initializeOperationControllerMetrics(requestResponseHandlerThread);
  }

  /**
   * Requests for the blob (info, data, or both) asynchronously and invokes the {@link Callback} when the request
   * completes.
   * @param blobIdStr The ID of the blob for which blob data is requested.
   * @param options The {@link GetBlobOptionsInternal} associated with the request.
   * @param callback The callback which will be invoked on the completion of the request.
   */
  protected void getBlob(String blobIdStr, GetBlobOptionsInternal options,
      final Callback<GetBlobResult> callback, QuotaChargeCallback quotaChargeCallback) throws RouterException {
    getManager.submitGetBlobOperation(blobIdStr, options, callback, quotaChargeCallback);
    routerCallback.onPollReady();
  }

  /**
   * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobProperties The properties of the blob.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param futureResult A future that would contain the BlobId eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   */
  protected void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options, FutureResult<String> futureResult, Callback<String> callback,
      QuotaChargeCallback quotaChargeCallback) {
    if (!putManager.isOpen()) {
      handlePutManagerClosed(blobProperties, false, futureResult, callback);
    } else {
      putManager.submitPutBlobOperation(blobProperties, userMetadata, channel, options, futureResult, callback,
          quotaChargeCallback);
      routerCallback.onPollReady();
    }
  }

  /**
   * Requests for a new metadata blob to be put asynchronously and invokes the {@link Callback} when the request
   * completes. This metadata blob will contain references to the chunks provided as an argument. The blob ID returned
   * by this operation can be used to fetch the chunks as if they were a single blob.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param chunksToStitch the list of data chunks to stitch together. The router will treat the metadata in the
   *                       {@link ChunkInfo} object as a source of truth, so the caller should ensure that these
   *                       fields are set accurately.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   */
  protected void stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      FutureResult<String> futureResult, Callback<String> callback, QuotaChargeCallback quotaChargeCallback) {
    if (!putManager.isOpen()) {
      handlePutManagerClosed(blobProperties, true, futureResult, callback);
    } else {
      putManager.submitStitchBlobOperation(blobProperties, userMetadata, chunksToStitch, futureResult, callback,
          quotaChargeCallback);
      routerCallback.onPollReady();
    }
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobIdStr The ID of the blob that needs to be deleted in string form
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param futureResult A future that would contain information about whether the deletion succeeded or not,
   *                     eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @param attemptChunkDeletes whether delete of chunks of the given blob (if it turns out to be composite) should be
   *                            attempted. Set this to false if it is known that the given blob is a data chunk.
   */
  protected void deleteBlob(final String blobIdStr, final String serviceId, FutureResult<Void> futureResult,
      final Callback<Void> callback, boolean attemptChunkDeletes, QuotaChargeCallback quotaChargeCallback) {
    try {
      deleteManager.submitDeleteBlobOperation(blobIdStr, serviceId, futureResult,
          (Void result, Exception exception) -> {
            if (exception == null && attemptChunkDeletes) {
              try {
                nonBlockingRouter.initiateChunkDeletesIfAny(blobIdStr, serviceId, quotaChargeCallback);
              } catch (RouterException e) {
                logger.warn(
                    "RouterException for same reason should have been thrown by submitDeleteBlobOperation() and no callback should be triggered.",
                    e);
              }
            }
            if (callback != null) {
              callback.onCompletion(result, exception);
            }
          }, quotaChargeCallback);
    } catch (RouterException e) {
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onDeleteBlobError(e);
      nonBlockingRouter.completeOperation(futureResult, callback, null, e);
    }
    routerCallback.onPollReady();
  }

  /**
   * Requests for a blob to be undeleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobIdStr The ID of the blob that needs to be undeleted in string form
   * @param serviceId The service ID of the service undeleting the blob. This can be null if unknown.
   * @param futureResult A future that would contain information about whether the undelete succeeded or not,
   *                     eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   */
  protected void undeleteBlob(final String blobIdStr, final String serviceId, FutureResult<Void> futureResult,
      final Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    doOperationTowardsMaybeCompositeBlob(blobIdStr, futureResult, callback,
        new CompositeBlobOperationHelper("UNDELETE", GetOption.Include_Deleted_Blobs, routerMetrics.ageAtUndelete,
            (blobId, chunkIds) -> doUndeleteOperation(blobId, chunkIds, serviceId, futureResult, callback,
                quotaChargeCallback), blobInfo -> false, (exception) -> {
          nonBlockingRouter.completeUndeleteBlobOperation(exception, futureResult, callback);
        }), quotaChargeCallback);
  }

  /**
   * Requests for the ttl of a blob to be updated asynchronously and invokes the {@link Callback} when the request
   * completes.
   * @param blobIdStr The ID of the blob that needs the ttl update in string form
   * @param serviceId The service ID of the service updating the ttl of the blob. This can be null if unknown.
   * @param expiresAtMs The new expiry time (in ms) of the blob. Using {@link Utils#Infinite_Time} makes the blob
   *                    permanent
   * @param futureResult A future that would contain the BlobId eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @param quotaChargeCallback The {@link QuotaChargeCallback} object.
   */
  protected void updateBlobTtl(final String blobIdStr, final String serviceId, long expiresAtMs,
      FutureResult<Void> futureResult, Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    doOperationTowardsMaybeCompositeBlob(blobIdStr, futureResult, callback,
        new CompositeBlobOperationHelper("TTL UPDATE", GetOption.None, routerMetrics.ageAtTtlUpdate,
            (blobId, chunkIds) -> {
              doUpdateTtlOperation(blobId, chunkIds, serviceId, expiresAtMs, futureResult, callback,
                  quotaChargeCallback);
            }, (blobInfo -> isBlobTtlUpdated(blobInfo)), (exception) -> {
          nonBlockingRouter.completeUpdateBlobTtlOperation(exception, futureResult, callback);
        }), quotaChargeCallback);
  }

  /**
   * Requests for a blob to be replicated asynchronously and invokes the {@link Callback} when the request
   * completes.
   * @param blobId ID of the blob that needs to be replicated.
   * @param serviceId The service ID of the service replicating the blob. This can be null if unknown.
   * @param sourceDataNode The source {@link DataNodeId} where to get the Blob.
   * @param futureResult A future that would contain the BlobId eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   */
  protected void replicateBlob(String blobId, String serviceId, DataNodeId sourceDataNode,
      FutureResult<Void> futureResult, Callback<Void> callback) {
    try {
      replicateBlobManager.submitReplicateBlobOperation(blobId, serviceId, sourceDataNode, futureResult, callback);
      routerCallback.onPollReady();
    } catch (RouterException e) {
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onReplicateBlobError(e);
      nonBlockingRouter.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * A helper method to perform ttl update and undelete operation on a composite blob.
   * @param blobIdStr The ID of the blob that needs the ttl update in string form
   *                    permanent
   * @param futureResult A future that would contain the BlobId eventually.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @param helper The {@link CompositeBlobOperationHelper} that carries other information about this operation.
   * @param quotaChargeCallback The {@link QuotaChargeCallback} object to help with quota enforcement.
   */
  protected void doOperationTowardsMaybeCompositeBlob(final String blobIdStr, FutureResult<Void> futureResult,
      Callback<Void> callback, CompositeBlobOperationHelper helper, QuotaChargeCallback quotaChargeCallback) {
    // Can skip GET if we can determine this is not a metadata blob
    if (NonBlockingRouter.isMaybeMetadataBlob(blobIdStr)) {
      Callback<GetBlobResult> internalCallback = (GetBlobResult result, Exception exception) -> {
        if (exception != null) {
          nonBlockingRouter.completeOperation(futureResult, callback, null, exception, false);
        } else if (result != null && result.getBlobDataChannel() != null) {
          exception = new RouterException(
              String.format("GET blob call returned the blob instead of just the store keys (before {} )",
                  helper.getOpName()), RouterErrorCode.UnexpectedInternalError);
          nonBlockingRouter.completeOperation(futureResult, callback, null, exception, false);
        } else if (result.getBlobInfo() != null && helper.getAlreadyUpdated().apply(result.getBlobInfo())
            && canRelyOnMetadataForUpdate(result.getBlobInfo())) {
          // If we are here then we can rely that the metadata chunk was updated only after all data chunks were updated.
          // So this means we are sure that all data chunks are successfully updated.
          // Send an update for metadata chunk only to ensure that metadata chunk's update meets quorum.
          nonBlockingRouter.incrementOperationsCount(1);
          helper.getDoOperation().accept(blobIdStr, Collections.emptyList());
          routerMetrics.updateOptimizedCount.inc();
        } else {
          if (result.getBlobInfo() != null && helper.getAlreadyUpdated().apply(result.getBlobInfo())) {
            routerMetrics.updateUnOptimizedCount.inc();
          }
          List<String> blobIdStrs = new ArrayList<>();
          if (result != null && result.getBlobChunkIds() != null) {
            result.getBlobChunkIds().forEach(key -> blobIdStrs.add(key.getID()));
          }
          nonBlockingRouter.incrementOperationsCount(blobIdStrs.size());
          helper.getDoOperation().accept(blobIdStr, blobIdStrs);
        }
      };

      GetBlobOptions options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
          .getOption(helper.getGetOption())
          .build();
      GetBlobOptionsInternal optionsInternal = new GetBlobOptionsInternal(options, true, helper.getMetrics());
      try {
        getBlob(blobIdStr, optionsInternal, internalCallback, quotaChargeCallback);
      } catch (RouterException e) {
        helper.getCompleteOperationAtException().accept(e);
      }
    } else {
      // do update directly on single blobId
      routerMetrics.skippedGetBlobCount.inc();
      helper.getDoOperation().accept(blobIdStr, Collections.emptyList());
    }
  }

  /**
   * @return List of {@link BackgroundDeleteRequest}s.
   */
  protected List<BackgroundDeleteRequest> getBackGroundDeleteRequests() {
    return backgroundDeleteRequests;
  }

  /**
   * clear the {@link BackgroundDeleteRequest}s.
   */
  protected void clearBackGroundDeleteRequests() {
    backgroundDeleteRequests.clear();
  }

  /**
   * Check if ttl operation can rely on metadata blob to determine if all the chunks of the blob have been ttl updated.
   * Currently this behavior relies on the value of the config {@link RouterConfig#routerUpdateOpMetadataRelianceTimestampInMs}.
   * For more details please check the comments on the config.
   * @param blobInfo {@link BlobInfo} object.
   * @return {@code true} if ttl update has been optimized. {@code false} otherwise.
   */
  private boolean canRelyOnMetadataForUpdate(BlobInfo blobInfo) {
    return blobInfo.getBlobProperties().getCreationTimeInMs() > routerConfig.routerUpdateOpMetadataRelianceTimestampInMs;
  }

  /**
   * @param blobInfo {@link BlobInfo} object.
   * @return {@code true} if the blob is already ttl updated. {@code false} otherwise.
   */
  private boolean isBlobTtlUpdated(BlobInfo blobInfo) {
    return blobInfo.getBlobProperties().getTimeToLiveInSeconds() == Utils.Infinite_Time;
  }

  /**
   * Helper method that submits the TTL update operation and handles exceptions.
   * @param blobIdStr The blobId of the simple blob or the metadata blob in case of composite blob.
   * @param chunkIdStrs The blob ids of the metadata blob's chunks.
   * @param serviceId The service ID of the service updating the ttl of the blob(s). This can be null if unknown.
   * @param expiresAtMs The new expiry time (in ms) of the blob.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   * @param quotaChargeCallback {@link QuotaChargeCallback} for quota charging events.
   */
  private void doUpdateTtlOperation(String blobIdStr, List<String> chunkIdStrs, final String serviceId, long expiresAtMs,
      FutureResult<Void> futureResult, Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    try {
      logger.trace("Updatettl for blob {} with chunkids {}", blobIdStr, chunkIdStrs);
      ttlUpdateManager.submitTtlUpdateOperation(blobIdStr, chunkIdStrs, serviceId, expiresAtMs, futureResult, callback,
          quotaChargeCallback);
      routerCallback.onPollReady();
    } catch (RouterException e) {
      nonBlockingRouter.incrementOperationsCount(1 - chunkIdStrs.size());
      nonBlockingRouter.completeUpdateBlobTtlOperation(e, futureResult, callback);
    }
  }

  /**
   * Helper method that submits the undelete operation and handles exceptions.
   * @param blobIdStr The blobId of the simple blob or the metadata blob in case of composite blob.
   * @param chunkIdStrs The blob ids of the metadata blob's chunks.
   * @param serviceId The service ID of the service undeleting the blob(s). This can be null if unknown.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   * @param quotaChargeCallback The {@link QuotaChargeCallback} object.
   */
  private void doUndeleteOperation(String blobIdStr, List<String> chunkIdStrs, final String serviceId, FutureResult<Void> futureResult,
      Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    try {
      undeleteManager.submitUndeleteOperation(blobIdStr, chunkIdStrs, serviceId, futureResult, callback, quotaChargeCallback);
      routerCallback.onPollReady();
    } catch (RouterException e) {
      nonBlockingRouter.incrementOperationsCount(1 - chunkIdStrs.size());
      nonBlockingRouter.completeUndeleteBlobOperation(e, futureResult, callback);
    }
  }

  /**
   * To be called if a put/stitch call is made while {@link PutManager} is closed. It will complete the operation
   * with an exception and close the router.
   * @param blobProperties the {@link BlobProperties} for the put call.
   * @param stitchOperation {@code true} if this is a stitch operation.
   * @param futureResult A future to be completed by this method
   * @param callback The {@link Callback} which will be invoked by this method.
   */
  private void handlePutManagerClosed(BlobProperties blobProperties, boolean stitchOperation,
      FutureResult<String> futureResult, Callback<String> callback) {
    RouterException routerException =
        new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onPutBlobError(routerException, blobProperties.isEncrypted(), stitchOperation);
    nonBlockingRouter.completeOperation(futureResult, callback, null, routerException);
    // Close so that any existing operations are also disposed off.
    nonBlockingRouter.close();
  }

  /**
   * Shuts down the OperationController and cleans up all the resources associated with it.
   */
  void shutdown() {
    logger.info("OperationController is shutting down");
    try {
      if (!shutDownLatch.await(NonBlockingRouter.SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
        logger.error("RequestResponseHandler thread did not shut down gracefully, forcing shut down");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Exception while shutting down, forcing shutdown", e);
    }
    putManager.close();
    getManager.close();
    deleteManager.close();
    ttlUpdateManager.close();
    undeleteManager.close();
    replicateBlobManager.close();
  }

  /**
   * This method is used by the RequestResponseHandler thread to poll for requests to be sent
   * @param requestsToSend a list of {@link RequestInfo} that will contain the requests to be sent out.
   * @param requestsToDrop a list of correlation IDs that will contain the IDs for requests that the network layer
   *                       should drop.
   */
  protected void pollForRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    try {
      putManager.poll(requestsToSend, requestsToDrop);
      getManager.poll(requestsToSend, requestsToDrop);
      nonBlockingRouter.initiateBackgroundDeletes(backgroundDeleteRequests);
      backgroundDeleteRequests.clear();
      deleteManager.poll(requestsToSend, requestsToDrop);
      ttlUpdateManager.poll(requestsToSend, requestsToDrop);
      undeleteManager.poll(requestsToSend, requestsToDrop);
      replicateBlobManager.poll(requestsToSend, requestsToDrop);
    } catch (Exception e) {
      logger.error("Operation Manager poll received an unexpected error: ", e);
      routerMetrics.operationManagerPollErrorCount.inc();
    }
  }

  /**
   * Handle the response from polling the {@link NetworkClient}.
   * @param responseInfoList the list of {@link ResponseInfo} containing the responses.
   */
  protected void onResponse(List<ResponseInfo> responseInfoList) {
    for (ResponseInfo responseInfo : responseInfoList) {
      try {
        RequestInfo requestInfo = responseInfo.getRequestInfo();
        if (requestInfo == null) {
          // If requestInfo is null, it means request has been failed previously due to long wait in pending requests
          // queue. The failed request was already handled by one of the managers(PutManager, GetManager, etc). Current
          // response comes from timed-out connection associated with previous request. Router only needs to notify
          // responseHandler to mark the data node resource down.
          DataNodeId dataNodeId = responseInfo.getDataNode();
          responseHandler.onConnectionTimeout(dataNodeId);
        } else {
          if (!responseInfo.isQuotaRejected()) {
            long responseReceiveTime = requestInfo.getResponseHeaderReceiveTime();
            if (responseReceiveTime != -1) {
              routerMetrics.responseReceiveToHandleLatencyMs.update(System.currentTimeMillis() - responseReceiveTime);
            }
          }
          RequestOrResponseType type = ((RequestOrResponse) requestInfo.getRequest()).getRequestType();
          logger.debug("Handling response of type {} for {}", type, requestInfo.getRequest().getCorrelationId());
          switch (type) {
            case PutRequest:
              putManager.handleResponse(responseInfo);
              break;
            case GetRequest:
              getManager.handleResponse(responseInfo);
              break;
            case DeleteRequest:
              deleteManager.handleResponse(responseInfo);
              break;
            case TtlUpdateRequest:
              ttlUpdateManager.handleResponse(responseInfo);
              break;
            case UndeleteRequest:
              undeleteManager.handleResponse(responseInfo);
              break;
            case ReplicateBlobRequest:
              replicateBlobManager.handleResponse(responseInfo);
              break;
            default:
              logger.error("Unexpected response type: {} received, discarding", type);
          }
        }
      } catch (Exception e) {
        logger.error("Unexpected error received while handling a response: ", e);
        routerMetrics.operationManagerHandleResponseErrorCount.inc();
      }
    }
  }

  /**
   * @return a {@link List} of {@link ResponseInfo} objects corresponding to non quota compliant requests.
   */
  protected List<ResponseInfo> getNonQuotaCompliantResponses() {
    return Collections.emptyList();
  }

  /**
   * The RequestResponseHandler thread simply runs in a loop polling the OperationController for any
   * requests to be sent, and notifies it about network events.
   */
  @Override
  public void run() {
    // The timeout for the network client poll should be a function of the request timeout,
    // as the poll timeout should not cause the request to not time out for a lot longer than the configured request
    // timeout. In the worst case, the request will time out in (request_timeout_ms + poll_timeout_ms), so the poll
    // timeout should be at least an order of magnitude smaller.
    final int NETWORK_CLIENT_POLL_TIMEOUT = routerConfig.routerRequestNetworkTimeoutMs / 10;
    try {
      while (nonBlockingRouter.isOpen.get()) {
        List<RequestInfo> requestsToSend = new ArrayList<>();
        Set<Integer> requestsToDrop = new HashSet<>();
        pollForRequests(requestsToSend, requestsToDrop);

        List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestsToSend,
            routerConfig.routerDropRequestOnTimeout ? requestsToDrop : Collections.emptySet(),
            NETWORK_CLIENT_POLL_TIMEOUT);
        responseInfoList.addAll(getNonQuotaCompliantResponses());
        onResponse(responseInfoList);
        responseInfoList.forEach(ResponseInfo::release);
      }
    } catch (Throwable e) {
      logger.error("Aborting, as requestResponseHandlerThread received an unexpected error: ", e);
      routerMetrics.requestResponseHandlerUnexpectedErrorCount.inc();
    } finally {
      networkClient.close();
      shutDownLatch.countDown();
      // Close the router.
      nonBlockingRouter.shutDownOperationControllers();
    }
  }

  public Thread getRequestResponseHandlerThread() {
    return requestResponseHandlerThread;
  }
}

/**
 * A special {@link OperationController} that is responsible for handling background operations for this router.
 *
 * Background operations will be a scaling unit of its own (using its own {@link NetworkClient}), so that these
 * operations do not interfere or contend with resources used for regular operations.
 *
 * Background operations include:
 * 1. Deleting chunks of a composite blob that is deleted. When a composite blob is deleted, only the
 * associated metadata blob is deleted before notifying the caller. This keeps the latency low. In the background,
 * the associated metadata blob will be fetched, the chunk ids will be extracted and deleted.
 *
 * 2. (TBD) Deleting successfully put chunks of a failed composite blob put operation. Today, this is done by the
 * same {@link OperationController} doing the put.
 */
class BackgroundDeleter extends OperationController {
  private final AtomicInteger concurrentBackgroundDeleteOperationCount = new AtomicInteger();
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ConcurrentLinkedQueue<Supplier<Void>> deleteOperationQueue = new ConcurrentLinkedQueue<>();
  private final NonBlockingRouterMetrics routerMetrics;
  private final NonBlockingRouter nonBlockingRouter;
  private final RouterConfig routerConfig;

  /**
   * Instantiate the BackgroundDeleter
   * @throws IOException if the associated {@link OperationController} throws one.
   */
  BackgroundDeleter(AccountService accountService, NetworkClientFactory networkClientFactory, ClusterMap clusterMap,
      RouterConfig routerConfig, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      NonBlockingRouterMetrics routerMetrics, KeyManagementService kms, CryptoService cryptoService,
      CryptoJobHandler cryptoJobHandler, Time time, NonBlockingRouter nonBlockingRouter) throws IOException {
    super("backgroundDeleter", null, accountService, networkClientFactory, clusterMap, routerConfig, responseHandler,
        notificationSystem, routerMetrics, kms, cryptoService, cryptoJobHandler, time, nonBlockingRouter);
    this.routerMetrics = routerMetrics;
    this.nonBlockingRouter = nonBlockingRouter;
    this.routerConfig = routerConfig;
    putManager.close();
    ttlUpdateManager.close();
  }

  /**
   * @return The concurrent background delete operation counter.
   */
  protected AtomicInteger getConcurrentBackgroundDeleteOperationCount() {
    return concurrentBackgroundDeleteOperationCount;
  }

  /**
   * Put operations are disallowed in the BackgroundDeleter.
   */
  @Override
  protected void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options, FutureResult<String> futureResult, Callback<String> callback,
      QuotaChargeCallback quotaChargeCallback) {
    RouterException routerException = new RouterException("Illegal attempt to put blob through BackgroundDeleter",
        RouterErrorCode.UnexpectedInternalError);
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted(), false);
    nonBlockingRouter.completeOperation(futureResult, callback, null, routerException);
  }

  /**
   * Stitch operations are disallowed in the BackgroundDeleter.
   */
  @Override
  protected void stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      FutureResult<String> futureResult, Callback<String> callback, QuotaChargeCallback quotaChargeCallback) {
    RouterException routerException = new RouterException("Illegal attempt to stitch blob through BackgroundDeleter",
        RouterErrorCode.UnexpectedInternalError);
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted(), true);
    nonBlockingRouter.completeOperation(futureResult, callback, null, routerException);
  }

  /**
   * TTL update operations are disallowed in the BackgroundDeleter.
   */
  @Override
  protected void updateBlobTtl(String blobIdStr, final String serviceId, long expiresAtMs,
      FutureResult<Void> futureResult, Callback<Void> callback, QuotaChargeCallback quotaChargeCallback) {
    RouterException routerException =
        new RouterException("Illegal attempt to update TTL of blob through BackgroundDeleter",
            RouterErrorCode.UnexpectedInternalError);
    nonBlockingRouter.completeUpdateBlobTtlOperation(routerException, futureResult, callback);
  }

  /**
   * {@inheritDoc}
   *
   * If the maximum concurrent number of background delete operation in router configuration is not 0, then this delete
   * operation will be enqueued to a thread-safe queue and delete operations will be executed later.
   */
  @Override
  protected void deleteBlob(final String blobIdStr, final String serviceId, FutureResult<Void> futureResult,
      final Callback<Void> callback, boolean attemptChunkDeletes, QuotaChargeCallback quotaChargeCallback) {
    Supplier<Void> deleteCall = () -> {
      super.deleteBlob(blobIdStr, serviceId, futureResult, (Void result, Exception e) -> {
        callback.onCompletion(result, e);
        concurrentBackgroundDeleteOperationCount.decrementAndGet();
      }, attemptChunkDeletes, quotaChargeCallback);
      return null;
    };
    if (routerConfig.routerBackgroundDeleterMaxConcurrentOperations > 0) {
      deleteOperationQueue.offer(deleteCall);
    } else {
      concurrentBackgroundDeleteOperationCount.incrementAndGet();
      deleteCall.get();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void pollForRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    try {
      while (!deleteOperationQueue.isEmpty()) {
        if (concurrentBackgroundDeleteOperationCount.incrementAndGet()
            <= routerConfig.routerBackgroundDeleterMaxConcurrentOperations) {
          deleteOperationQueue.poll().get();
        } else {
          concurrentBackgroundDeleteOperationCount.decrementAndGet();
          break;
        }
      }
      getManager.poll(requestsToSend, requestsToDrop);
      deleteManager.poll(requestsToSend, requestsToDrop);
    } catch (Exception e) {
      logger.error("Background Deleter Operation Manager poll received an unexpected error: ", e);
      routerMetrics.operationManagerPollErrorCount.inc();
    }
  }
}
