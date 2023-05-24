/*
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

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Streaming, non-blocking router implementation for Ambry.
 */
class NonBlockingRouter implements Router {
  static final int SHUTDOWN_WAIT_MS = 10 * Time.MsPerSec;
  static final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouter.class);
  final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final ArrayList<OperationController> ocList;
  private final BackgroundDeleter backgroundDeleter;
  private final int ocCount;
  // Shared with the operation managers.
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final KeyManagementService kms;
  private final CryptoJobHandler cryptoJobHandler;
  // Resources that need to be shut down when the router does.
  private final List<Closeable> resourcesToClose;
  private final AtomicInteger currentBackgroundOperationsCount = new AtomicInteger(0);
  private static final int cacheMaxLimit = 1000;
  // Cache to store blob IDs which were not found in servers recently.
  private final Cache<String, Boolean> notFoundCache;
  private final AmbryCache blobMetadataCache;

  public final AtomicInteger currentOperationsCount = new AtomicInteger(0);

  /**
   * Constructs a NonBlockingRouter.
   * @param routerConfig the configs for the router.
   * @param routerMetrics the metrics for the router.
   * @param networkClientFactory the {@link NetworkClientFactory} used by the {@link OperationController} to create
   *                             instances of {@link NetworkClient}.
   * @param notificationSystem the notification system to use to notify about blob creations and deletions.
   * @param clusterMap the cluster map for the cluster.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param accountService the {@link AccountService} to use.
   * @param time the time instance.
   * @param defaultPartitionClass the default partition class to choose partitions from (if none is found in the
   *                              container config). Can be {@code null} if no affinity is required for the puts for
   *                              which the container contains no partition class hints.
   * @throws IOException if the OperationController could not be successfully created.
   * @throws ReflectiveOperationException if the OperationController could not be successfully created.
   */
  NonBlockingRouter(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkClientFactory networkClientFactory, NotificationSystem notificationSystem, ClusterMap clusterMap,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler,
      AccountService accountService, Time time, String defaultPartitionClass, AmbryCache blobMetadataCache)
      throws IOException, ReflectiveOperationException {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    ResponseHandler responseHandler = new ResponseHandler(clusterMap);
    this.kms = kms;
    this.cryptoJobHandler = cryptoJobHandler;
    /*
     * Initialize blobMetadata cache before the operation controllers
     * as it will be passed further down.
     */
    this.blobMetadataCache = blobMetadataCache;
    ocCount = routerConfig.routerScalingUnitCount;
    ocList = new ArrayList<>();
    for (int i = 0; i < ocCount; i++) {
      ocList.add(
          Utils.getObj(routerConfig.operationController, Integer.toString(i), defaultPartitionClass, accountService,
              networkClientFactory, clusterMap, routerConfig, responseHandler, notificationSystem, routerMetrics, kms,
              cryptoService, cryptoJobHandler, time, this));
    }
    backgroundDeleter =
        new BackgroundDeleter(accountService, networkClientFactory, clusterMap, routerConfig, responseHandler,
            notificationSystem, routerMetrics, kms, cryptoService, cryptoJobHandler, time, this);
    ocList.add(backgroundDeleter);
    ocList.forEach(OperationController::start);
    routerMetrics.initializeNumActiveOperationsMetrics(currentOperationsCount, currentBackgroundOperationsCount,
        backgroundDeleter.getConcurrentBackgroundDeleteOperationCount());
    resourcesToClose = new ArrayList<>();
    // Store the blob IDs which were not found in server nodes. This would avoid querying servers again and help in reducing the load.
    // Set the expiration time (routerConfig.routerNotFoundCacheTtlInMs) to 0 to disable the notFoundCache.
    notFoundCache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxLimit)
        .expireAfterWrite(routerConfig.routerNotFoundCacheTtlInMs, TimeUnit.MILLISECONDS)
        .recordStats()
        .build();
    routerMetrics.initializeNotFoundCacheMetrics(notFoundCache);
    routerMetrics.initializeQuotaOCMetrics(ocList);
  }

  /**
   * @return cache used to store IDs of blobs recently not found in server. Used only in tests.
   */
  Cache<String, Boolean> getNotFoundCache() {
    return notFoundCache;
  }

  /**
   * Returns an instance of blob metadata cache.
   * @return Returns an instance of blobMetadata cache
   */
  public AmbryCache getBlobMetadataCache() {
    return blobMetadataCache;
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param <T> the type of the operation result, which depends on the kind of operation.
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   */
  <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception) {
    completeOperation(futureResult, callback, operationResult, exception, true);
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param <T> the type of the operation result, which depends on the kind of operation.
   * @param futureResult the {@link FutureResult} that needs to be set. Can be null.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   * @param decrementOperationsCount if {@code true}, decrements current outstanding operations count.
   */
  <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception, boolean decrementOperationsCount) {
    if (decrementOperationsCount) {
      currentOperationsCount.decrementAndGet();
    }
    try {
      if (futureResult != null) {
        futureResult.done(operationResult, exception);
      }
      if (callback != null) {
        callback.onCompletion(operationResult, exception);
      }
    } catch (Exception e) {
      logger.error("Exception caught during future and callback completion", e);
    }
  }

  /**
   * Returns {@code true} if the input blobId may refer to a metadata blob, otherwise {@code false}.
   * The method will return {@code false} only for V5 and later blobs where the BlobDataType is available.
   * @param blobId the blobId to check.
   * @return boolean indicating whether the blob may be metadata.
   */
  static boolean isMaybeMetadataBlob(String blobId) {
    try {
      BlobId.BlobDataType dataType = BlobId.getBlobDataType(blobId);
      return (dataType == null || dataType == BlobId.BlobDataType.METADATA);
    } catch (Exception ex) {
      logger.error("Unexpected error getting blob data type for blobId {}", blobId, ex);
      return true;
    }
  }

  /**
   * Add a resource to close when the router shuts down.
   * @param resource the resource that needs closing.
   */
  void addResourceToClose(Closeable resource) {
    resourcesToClose.add(resource);
  }

  /**
   * Returns an {@link OperationController}
   * @return a randomly picked {@link OperationController} from the list of OperationControllers.
   */
  private OperationController getOperationController() {
    return ocList.get(ThreadLocalRandom.current().nextInt(ocCount));
  }

  /**
   * Requests for the blob data asynchronously with user-set {@link GetBlobOptions} and invokes the {@link Callback}
   * when the request completes.
   * @param blobIdStr The ID of the blob for which blob data is requested.
   * @param options The options associated with the request. This cannot be null.
   * @param callback The callback which will be invoked on the completion of the request.
   * @param quotaChargeCallback {@link QuotaChargeCallback} object.
   * @return A future that would eventually contain a {@link GetBlobResult} that can contain either
   *         the {@link BlobInfo}, the {@link ReadableStreamChannel} containing the blob data, or both.
   */
  @Override
  public Future<GetBlobResult> getBlob(String blobIdStr, GetBlobOptions options, final Callback<GetBlobResult> callback,
      QuotaChargeCallback quotaChargeCallback) {
    if (blobIdStr == null || options == null) {
      throw new IllegalArgumentException("blobId or options must not be null");
    }
    currentOperationsCount.incrementAndGet();
    final FutureResult<GetBlobResult> futureResult = new FutureResult<>();
    GetBlobOptionsInternal internalOptions =
        new GetBlobOptionsInternal(options, options.getOperationType() == GetBlobOptions.OperationType.BlobChunkIds,
            routerMetrics.ageAtGet);
    routerMetrics.operationQueuingRate.mark();
    try {
      if (isOpen.get()) {
        if (notFoundCache.getIfPresent(blobIdStr) != null) {
          // If we know that blob doesn't exist, complete the operation.
          logger.info("Blob {} is known to be missing in servers", blobIdStr);
          RouterException routerException;
          if (options.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
            routerException = new RouterException("GetBlobInfoOperation failed because of BlobNotFound",
                RouterErrorCode.BlobDoesNotExist);
          } else {
            routerException = new RouterException("GetBlobOperation failed because of BlobNotFound",
                RouterErrorCode.BlobDoesNotExist);
          }
          completeOperation(futureResult, callback, null, routerException);
        } else {
          getOperationController().getBlob(blobIdStr, internalOptions,
              new BlobOperationCallbackWrapper<>(blobIdStr, (getBlobResult, exception) -> {
                futureResult.done(getBlobResult, exception);
                if (callback != null) {
                  callback.onCompletion(getBlobResult, exception);
                }
              }), quotaChargeCallback);
        }
      } else {
        boolean isEncrypted = false;
        try {
          isEncrypted = BlobId.isEncrypted(blobIdStr);
        } catch (IOException e) {
          logger.warn("Blob ID string is not valid", e);
        }
        RouterException routerException =
            new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
        completeGetBlobOperation(routerException, internalOptions, futureResult, callback, isEncrypted);
      }
    } catch (RouterException e) {
      completeGetBlobOperation(e, internalOptions, futureResult, callback, false);
    }
    return futureResult;
  }

  /**
   * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options, Callback<String> callback, QuotaChargeCallback quotaChargeCallback) {
    if (blobProperties == null || channel == null || options == null) {
      throw new IllegalArgumentException("blobProperties, channel, or options must not be null");
    }
    if (userMetadata == null) {
      userMetadata = new byte[0];
    }
    currentOperationsCount.incrementAndGet();
    if (blobProperties.isEncrypted()) {
      routerMetrics.putEncryptedBlobOperationRate.mark();
    } else {
      routerMetrics.putBlobOperationRate.mark();
    }
    routerMetrics.operationQueuingRate.mark();
    FutureResult<String> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      getOperationController().putBlob(blobProperties, userMetadata, channel, options, futureResult, callback,
          quotaChargeCallback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties.isEncrypted(), false);
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be replicated asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob to be replicated.
   * @param serviceId The service ID of the service replicating the blob. This can be null if unknown.
   * @param sourceDataNode The source {@link DataNodeId} to get the blob from.
   * @return A future that would contain information about whether the replicateBlob succeeded or not, eventually.
   */
  public CompletableFuture<Void> replicateBlob(String blobId, String serviceId, DataNodeId sourceDataNode) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    replicateBlob(blobId, serviceId, sourceDataNode, CallbackUtils.fromCompletableFuture(future));
    return future;
  }

  /**
   * Requests for a new blob to be replicated on-demand asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob to be replicated.
   * @param serviceId The service ID of the service replicating the blob. This can be null if unknown.
   * @param sourceDataNode The source {@link DataNodeId} to get the blob from.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain information about whether the replicateBlob succeeded or not, eventually.
   */
  public Future<Void> replicateBlob(String blobId, String serviceId, DataNodeId sourceDataNode,
      Callback<Void> callback) {
    if (blobId == null || sourceDataNode == null) {
      throw new IllegalArgumentException("blobId or sourceHost must not be null");
    }

    currentOperationsCount.incrementAndGet();
    routerMetrics.replicateBlobOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    if (serviceId.equals(DeleteOperation.class.getSimpleName())) {
      routerMetrics.replicateBlobOperationOnDeleteRate.mark();
    } else if (serviceId.equals(TtlUpdateOperation.class.getSimpleName())){
      routerMetrics.replicateBlobOperationOnTtlUpdateRate.mark();
    }

    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      getOperationController().replicateBlob(blobId, serviceId, sourceDataNode, futureResult, callback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  @Override
  public Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback, QuotaChargeCallback quotaChargeCallback) {
    if (blobProperties == null || chunksToStitch == null) {
      throw new IllegalArgumentException("blobProperties or chunksToStitch must not be null");
    }
    if (userMetadata == null) {
      userMetadata = new byte[0];
    }
    currentOperationsCount.incrementAndGet();
    if (blobProperties.isEncrypted()) {
      routerMetrics.stitchEncryptedBlobOperationRate.mark();
    } else {
      routerMetrics.stitchBlobOperationRate.mark();
    }
    routerMetrics.operationQueuingRate.mark();
    FutureResult<String> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      getOperationController().stitchBlob(blobProperties, userMetadata, chunksToStitch, futureResult, callback,
          quotaChargeCallback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties.isEncrypted(), true);
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback,
      QuotaChargeCallback quotaChargeCallback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.deleteBlobOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      if (notFoundCache.getIfPresent(blobId) != null) {
        // If we know that blob doesn't exist, complete the operation
        logger.info("Blob {} is known to be missing in servers", blobId);
        RouterException routerException =
            new RouterException("DeleteOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist);
        completeOperation(futureResult, callback, null, routerException);
      } else {
        // Can skip attemptChunkDeletes if we can determine this is not a metadata blob
        boolean attemptChunkDeletes = isMaybeMetadataBlob(blobId);
        getOperationController().deleteBlob(blobId, serviceId, futureResult,
            new BlobOperationCallbackWrapper<>(blobId, callback), attemptChunkDeletes, quotaChargeCallback);
        if (!attemptChunkDeletes) {
          routerMetrics.skippedGetBlobCount.inc();
        }
      }
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onDeleteBlobError(routerException);
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be undeleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be undeleted.
   * @param serviceId The service ID of the service undeleting the blob. This can be null if unknown.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the undelete succeeded or not, eventually.
   */
  @Override
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback,
      QuotaChargeCallback quotaChargeCallback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.undeleteBlobOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      if (notFoundCache.getIfPresent(blobId) != null) {
        // If we know that blob doesn't exist, complete the operation.
        logger.info("Blob {} is known to be missing in servers", blobId);
        RouterException routerException =
            new RouterException("UndeleteOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist);
        completeOperation(futureResult, callback, null, routerException);
      } else {
        getOperationController().undeleteBlob(blobId, serviceId, futureResult,
            new BlobOperationCallbackWrapper<>(blobId, callback), quotaChargeCallback);
      }
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onUndeleteBlobError(routerException);
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  /**
   * Requests that a blob's TTL be updated asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs its TTL updated.
   * @param serviceId The service ID of the service updating the blob. This can be null if unknown.
   * @param expiresAtMs The new expiry time (in ms) of the blob. Using {@link Utils#Infinite_Time} makes the blob
   *                    permanent
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the update succeeded or not, eventually.
   */
  @Override
  public Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback,
      QuotaChargeCallback quotaChargeCallback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.updateBlobTtlOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      if (notFoundCache.getIfPresent(blobId) != null) {
        // If we know that blob doesn't exist, complete the operation.
        logger.info("Blob {} is known to be missing in servers", blobId);
        RouterException routerException =
            new RouterException("TtlUpdateOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist);
        completeOperation(futureResult, callback, null, routerException);
      } else {
        getOperationController().updateBlobTtl(blobId, serviceId, expiresAtMs, futureResult,
            new BlobOperationCallbackWrapper<>(blobId, callback), quotaChargeCallback);
      }
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      completeUpdateBlobTtlOperation(routerException, futureResult, callback);
    }
    return futureResult;
  }

  @Override
  public RouterConfig getRouterConfig() {
    return routerConfig;
  }

  /**
   * Initiated deletes of the blobIds in the given list of ids via the {@link BackgroundDeleter}
   * @param deleteRequests the list of {@link BackgroundDeleteRequest}s to execute.
   */
  void initiateBackgroundDeletes(List<BackgroundDeleteRequest> deleteRequests) {
    for (BackgroundDeleteRequest deleteRequest : deleteRequests) {
      currentOperationsCount.incrementAndGet();
      currentBackgroundOperationsCount.incrementAndGet();
      backgroundDeleter.deleteBlob(deleteRequest.getBlobId(), deleteRequest.getServiceId(), new FutureResult<>(),
          (Void result, Exception exception) -> {
            if (exception != null) {
              logger.error("Background delete operation failed with exception", exception);
            }
            currentBackgroundOperationsCount.decrementAndGet();
          }, false, deleteRequest.getQuotaChargeCallback());
    }
  }

  /**
   * Initiate the deletes of the data chunks associated with this blobId, if this blob turns out to be a composite
   * blob. Note that this causes the rate of gets to increase at the servers.
   * @param blobIdStr the original string of a {@link BlobId} which associated with the possibly composite blob.
   * @param serviceId the service ID associated with the original delete request.
   * @param quotaChargeCallback {@link QuotaChargeCallback} object for performing quota compliance checks.
   */
  void initiateChunkDeletesIfAny(final String blobIdStr, final String serviceId,
      final QuotaChargeCallback quotaChargeCallback) throws RouterException {
    Callback<GetBlobResult> callback = (GetBlobResult result, Exception exception) -> {
      if (exception != null) {
        // It is expected that these requests will not always succeed. For example, this may have been triggered by a
        // duplicate delete and the blob could have already been hard deleted, so the deserialization can fail, or the
        // blob could have been garbage collected and not found at all and so on.
        logger.trace("Encountered exception when attempting to get chunks of a possibly composite deleted blob {} ",
            blobIdStr, exception);
      } else if (result.getBlobDataChannel() != null) {
        logger.error("Unexpected result returned by background get operation to fetch chunk ids.");
      } else if (result.getBlobChunkIds() != null) {
        List<StoreKey> blobChunkIds = result.getBlobChunkIds();
        List<BackgroundDeleteRequest> deleteRequests = new ArrayList<>(blobChunkIds.size());
        for (StoreKey storeKey : blobChunkIds) {
          logger.trace("Initiating delete of chunk blob: {}", storeKey);
          deleteRequests.add(new BackgroundDeleteRequest(storeKey, serviceId, quotaChargeCallback));
        }
        initiateBackgroundDeletes(deleteRequests);
        if (blobMetadataCache != null) {
          boolean deleteResult = blobMetadataCache.deleteObject(blobIdStr);
          logger.debug("[{}] Issued delete-metadata for blobId = {}, reason = Background delete operation, result = {}", blobMetadataCache.getCacheId(),
              blobIdStr, deleteResult);

        }
      }
      currentBackgroundOperationsCount.decrementAndGet();
    };
    currentOperationsCount.incrementAndGet();
    currentBackgroundOperationsCount.incrementAndGet();
    GetBlobOptions options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
        .getOption(GetOption.Include_All)
        .build();
    GetBlobOptionsInternal optionsInternal = new GetBlobOptionsInternal(options, true, routerMetrics.ageAtDelete);
    backgroundDeleter.getBlob(blobIdStr, optionsInternal, callback, quotaChargeCallback);
  }

  /**
   * Completes a getBlob operation by invoking the {@code callback} and setting the {@code futureResult} with the given
   * {@code {@link RouterException}}
   * @param routerException {@link RouterException} to be set in the callback and future result
   * @param internalOptions instance of {@link GetBlobOptionsInternal} to use
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
   */
  void completeGetBlobOperation(RouterException routerException, GetBlobOptionsInternal internalOptions,
      FutureResult<GetBlobResult> futureResult, Callback<GetBlobResult> callback, boolean isEncrypted) {
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onGetBlobError(routerException, internalOptions, isEncrypted);
    completeOperation(futureResult, callback, null, routerException);
  }

  /**
   * Completes a updateBlobTtl operation by invoking the {@code callback} and setting the {@code futureResult} with the
   * given {@code routerException}.
   * @param routerException {@link RouterException} to be set in the callback and future result
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   */
  void completeUpdateBlobTtlOperation(RouterException routerException, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onUpdateBlobTtlError(routerException);
    completeOperation(futureResult, callback, null, routerException);
  }

  /**
   * Completes a undelete blob operation by invoking the {@code callback} and setting the {@code futureResult} with the
   * given {@code routerException}.
   * @param routerException {@link RouterException} to be set in the callback and future result
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   */
  void completeUndeleteBlobOperation(RouterException routerException, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.onUndeleteBlobError(routerException);
    completeOperation(futureResult, callback, null, routerException);
  }

  /**
   * Closes the router and releases any resources held by the router. If the router is already closed, then this
   * method has no effect.
   * <p/>
   * After a router is closed, any further attempt to invoke Router operations will cause a {@link RouterException} with
   * error code {@link RouterErrorCode#RouterClosed} to be returned as part of the {@link Future} and {@link Callback}
   * if any.
   */
  @Override
  public void close() {
    shutDownOperationControllers();
    // wait for all the threads to actually exit
    waitForResponseHandlerThreadExit();
    // close the KeyManagementService
    if (kms != null) {
      try {
        kms.close();
      } catch (IOException e) {
        logger.error("Exception thrown on closing KMS", e);
      }
    }
    // close the crypto job handler
    if (cryptoJobHandler != null) {
      cryptoJobHandler.close();
    }
    for (Closeable resource : resourcesToClose) {
      try {
        resource.close();
      } catch (IOException e) {
        logger.error("Exception thrown on closing {}", resource.getClass().getName());
      }
    }
    // close router metrics
    routerMetrics.close();
  }

  /**
   * Wait for all the threads to finish up.
   */
  private void waitForResponseHandlerThreadExit() {
    for (OperationController oc : ocList) {
      try {
        oc.getRequestResponseHandlerThread().join(SHUTDOWN_WAIT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Initiate the shutdown of all the OperationControllers. This method can get executed in the context of
   * both the calling thread of the {@link #close()} method, and the RequestResponseHandler thread of any of the
   * Operation Controllers.
   */
  void shutDownOperationControllers() {
    if (isOpen.compareAndSet(true, false)) {
      logger.info("Closing the router");
      for (OperationController oc : ocList) {
        oc.shutdown();
      }
    }
  }

  /**
   * Returns whether the router is open or closed.
   * @return true if the router is open.
   */
  boolean isOpen() {
    return isOpen.get();
  }

  /**
   * Return the count of the number of operations submitted to the router that are not yet completed.
   * @return number of operations being handled at the time of this call.
   */
  int getOperationsCount() {
    return currentOperationsCount.get();
  }

  void incrementOperationsCount(int delta) {
    currentOperationsCount.addAndGet(delta);
  }

  /**
   * Return the count of the number of background operations submitted to the router that are not yet
   * completed.
   * @return number of background operations being handled at the time of this call.
   */
  int getBackgroundOperationsCount() {
    return currentBackgroundOperationsCount.get();
  }

  /**
   * Wrapper for callbacks provided to router operations to intercept the responses and do operations such as keeping
   * track of errors, etc. Currently, it caches the blob IDs which were not found in the servers to avoid querying them
   * again for a configurable duration.
   */
  protected class BlobOperationCallbackWrapper<T> implements Callback<T> {

    private final String blobId;
    private final Callback<T> callback;

    /**
     * Creates an instance of BlobOperationCallbackWrapper with the given {@code callback}.
     * @param blobId Blob Id on which operation is requested.
     * @param callback the {@link Callback} to invoke on operation completion.
     */
    public BlobOperationCallbackWrapper(String blobId, Callback<T> callback) {
      this.blobId = blobId;
      this.callback = callback;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      // If blob doesn't exist, add it to notFoundCache to avoid querying server nodes again.
      if (exception instanceof RouterException && ((RouterException) exception).getErrorCode()
          .equals(RouterErrorCode.BlobDoesNotExist)) {
        notFoundCache.put(blobId, true);
      }
      if (callback != null) {
        callback.onCompletion(result, exception);
      }
    }
  }
}

