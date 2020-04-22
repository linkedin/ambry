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
import com.github.ambry.commons.BlobId;
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
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Streaming, non-blocking router implementation for Ambry.
 */
class NonBlockingRouter implements Router {
  private final NetworkClientFactory networkClientFactory;
  private final ArrayList<OperationController> ocList;
  private final BackgroundDeleter backgroundDeleter;
  private final int ocCount;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  // Shared with the operation managers.
  private final RouterConfig routerConfig;
  private final NotificationSystem notificationSystem;
  private final ClusterMap clusterMap;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ResponseHandler responseHandler;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
  private final AccountService accountService;
  private final Time time;
  // Resources that need to be shut down when the router does.
  private final List<Closeable> resourcesToClose;

  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouter.class);
  static final AtomicInteger currentOperationsCount = new AtomicInteger(0);
  private final AtomicInteger currentBackgroundOperationsCount = new AtomicInteger(0);
  static final int SHUTDOWN_WAIT_MS = 10 * Time.MsPerSec;
  static final AtomicInteger correlationIdGenerator = new AtomicInteger(0);

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
   */
  NonBlockingRouter(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkClientFactory networkClientFactory, NotificationSystem notificationSystem, ClusterMap clusterMap,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler,
      AccountService accountService, Time time, String defaultPartitionClass) throws IOException {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.networkClientFactory = networkClientFactory;
    this.notificationSystem = notificationSystem;
    this.clusterMap = clusterMap;
    responseHandler = new ResponseHandler(clusterMap);
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.accountService = accountService;
    this.time = time;
    ocCount = routerConfig.routerScalingUnitCount;
    ocList = new ArrayList<>();
    for (int i = 0; i < ocCount; i++) {
      ocList.add(new OperationController(Integer.toString(i), defaultPartitionClass, accountService));
    }
    backgroundDeleter = new BackgroundDeleter();
    ocList.add(backgroundDeleter);
    routerMetrics.initializeNumActiveOperationsMetrics(currentOperationsCount, currentBackgroundOperationsCount);
    resourcesToClose = new ArrayList<>();
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
   * @return A future that would eventually contain a {@link GetBlobResult} that can contain either
   *         the {@link BlobInfo}, the {@link ReadableStreamChannel} containing the blob data, or both.
   */
  @Override
  public Future<GetBlobResult> getBlob(String blobIdStr, GetBlobOptions options,
      final Callback<GetBlobResult> callback) {
    if (blobIdStr == null || options == null) {
      throw new IllegalArgumentException("blobId or options must not be null");
    }
    currentOperationsCount.incrementAndGet();
    final FutureResult<GetBlobResult> futureResult = new FutureResult<>();
    GetBlobOptionsInternal internalOptions = new GetBlobOptionsInternal(options, false, routerMetrics.ageAtGet);
    routerMetrics.operationQueuingRate.mark();
    try {
      if (isOpen.get()) {
        getOperationController().getBlob(blobIdStr, internalOptions, (internalResult, exception) -> {
          GetBlobResult getBlobResult = internalResult == null ? null : internalResult.getBlobResult;
          futureResult.done(getBlobResult, exception);
          if (callback != null) {
            callback.onCompletion(getBlobResult, exception);
          }
        });
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
      PutBlobOptions options, Callback<String> callback) {
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
      getOperationController().putBlob(blobProperties, userMetadata, channel, options, futureResult, callback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties.isEncrypted(), false);
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  @Override
  public Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback) {
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
      getOperationController().stitchBlob(blobProperties, userMetadata, chunksToStitch, futureResult, callback);
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
  public Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.deleteBlobOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      // Can skip attemptChunkDeletes if we can determine this is not a metadata blob
      boolean attemptChunkDeletes = isMaybeMetadataBlob(blobId);
      getOperationController().deleteBlob(blobId, serviceId, futureResult, callback, attemptChunkDeletes);
      if (!attemptChunkDeletes) {
        routerMetrics.skippedGetBlobCount.inc();
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
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.undeleteBlobOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      getOperationController().undeleteBlob(blobId, serviceId, futureResult, callback);
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
  public Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback) {
    if (blobId == null) {
      throw new IllegalArgumentException("blobId must not be null");
    }
    currentOperationsCount.incrementAndGet();
    routerMetrics.updateBlobTtlOperationRate.mark();
    routerMetrics.operationQueuingRate.mark();
    FutureResult<Void> futureResult = new FutureResult<>();
    if (isOpen.get()) {
      getOperationController().updateBlobTtl(blobId, serviceId, expiresAtMs, futureResult, callback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      completeUpdateBlobTtlOperation(routerException, futureResult, callback);
    }
    return futureResult;
  }

  /**
   * Initiated deletes of the blobIds in the given list of ids via the {@link BackgroundDeleter}
   * @param deleteRequests the list of {@link BackgroundDeleteRequest}s to execute.
   */
  private void initiateBackgroundDeletes(List<BackgroundDeleteRequest> deleteRequests) {
    for (BackgroundDeleteRequest deleteRequest : deleteRequests) {
      currentOperationsCount.incrementAndGet();
      currentBackgroundOperationsCount.incrementAndGet();
      backgroundDeleter.deleteBlob(deleteRequest.getBlobId(), deleteRequest.getServiceId(), new FutureResult<>(),
          (Void result, Exception exception) -> {
            if (exception != null) {
              logger.error("Background delete operation failed with exception", exception);
            }
            currentBackgroundOperationsCount.decrementAndGet();
          }, false);
    }
  }

  /**
   * Initiate the deletes of the data chunks associated with this blobId, if this blob turns out to be a composite
   * blob. Note that this causes the rate of gets to increase at the servers.
   * @param blobIdStr the original string of a {@link BlobId} which associated with the possibly composite blob.
   * @param serviceId the service ID associated with the original delete request.
   */
  private void initiateChunkDeletesIfAny(final String blobIdStr, final String serviceId) throws RouterException {
    Callback<GetBlobResultInternal> callback = (GetBlobResultInternal result, Exception exception) -> {
      if (exception != null) {
        // It is expected that these requests will not always succeed. For example, this may have been triggered by a
        // duplicate delete and the blob could have already been hard deleted, so the deserialization can fail, or the
        // blob could have been garbage collected and not found at all and so on.
        logger.trace("Encountered exception when attempting to get chunks of a possibly composite deleted blob {} ",
            blobIdStr, exception);
      } else if (result.getBlobResult != null) {
        logger.error("Unexpected result returned by background get operation to fetch chunk ids.");
      } else if (result.storeKeys != null) {
        List<BackgroundDeleteRequest> deleteRequests = new ArrayList<>(result.storeKeys.size());
        for (StoreKey storeKey : result.storeKeys) {
          logger.trace("Initiating delete of chunk blob: {}", storeKey);
          deleteRequests.add(new BackgroundDeleteRequest(storeKey, serviceId));
        }
        initiateBackgroundDeletes(deleteRequests);
      }
      currentBackgroundOperationsCount.decrementAndGet();
    };
    currentOperationsCount.incrementAndGet();
    currentBackgroundOperationsCount.incrementAndGet();
    GetBlobOptions options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
        .getOption(GetOption.Include_All)
        .build();
    GetBlobOptionsInternal optionsInternal = new GetBlobOptionsInternal(options, true, routerMetrics.ageAtDelete);
    backgroundDeleter.getBlob(blobIdStr, optionsInternal, callback);
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
  private void completeGetBlobOperation(RouterException routerException, GetBlobOptionsInternal internalOptions,
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
  private void completeUpdateBlobTtlOperation(RouterException routerException, FutureResult<Void> futureResult,
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
  private void completeUndeleteBlobOperation(RouterException routerException, FutureResult<Void> futureResult,
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
        oc.requestResponseHandlerThread.join(SHUTDOWN_WAIT_MS);
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
  private void shutDownOperationControllers() {
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

  /**
   * Return the count of the number of background operations submitted to the router that are not yet
   * completed.
   * @return number of background operations being handled at the time of this call.
   */
  int getBackgroundOperationsCount() {
    return currentBackgroundOperationsCount.get();
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
  static <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
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
  static <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception, boolean decrementOperationsCount) {
    if (decrementOperationsCount) {
      NonBlockingRouter.currentOperationsCount.decrementAndGet();
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
  private static boolean isMaybeMetadataBlob(String blobId) {
    try {
      BlobId.BlobDataType dataType = BlobId.getBlobDataType(blobId);
      return (dataType == null || dataType == BlobId.BlobDataType.METADATA);
    } catch (Exception ex) {
      logger.error("Unexpected error getting blob data type for blobId " + blobId, ex);
      return true;
    }
  }

  /**
   * OperationController is the scaling unit for the NonBlockingRouter. The NonBlockingRouter can have multiple
   * OperationControllers. Any operation submitted to the NonBlockingRouter will be submitted to one of the
   * OperationControllers. A worker thread (the RequestResponseHandler thread) will poll The OperationController for
   * requests to be sent and will notify it on receiving responses. The OperationController in turn makes use of the
   * {@link PutManager}, {@link GetManager} and {@link DeleteManager} to perform puts, gets and deletes,
   * respectively. A {@link NetworkClient} is used to interact with the network.
   */
  private class OperationController implements Runnable {
    final PutManager putManager;
    final GetManager getManager;
    final DeleteManager deleteManager;
    final TtlUpdateManager ttlUpdateManager;
    final UndeleteManager undeleteManager;
    private final NetworkClient networkClient;
    private final Thread requestResponseHandlerThread;
    private final CountDownLatch shutDownLatch = new CountDownLatch(1);
    protected final RouterCallback routerCallback;
    private final List<BackgroundDeleteRequest> backgroundDeleteRequests = new ArrayList<>();

    /**
     * Constructs an OperationController
     * @param suffix the suffix to associate with the thread names of this OperationController
     * @param defaultPartitionClass the default partition class to choose partitions from (if none is found in the
     *                              container config). Can be {@code null} if no affinity is required for the puts for
     *                              which the container contains no partition class hints.
     * @param accountService the {@link AccountService} to use.
     * @throws IOException if the network components could not be created.
     */
    OperationController(String suffix, String defaultPartitionClass, AccountService accountService) throws IOException {
      networkClient = networkClientFactory.getNetworkClient();
      // Warm up connections to dataNodes in local and remote DCs.
      List<ResponseInfo> responseInfos = new ArrayList<>();

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
      for (ResponseInfo responseInfo : responseInfos) {
        if (responseInfo.getRequestInfo() == null) {
          responseHandler.onConnectionTimeout(responseInfo.getDataNode());
        }
      }
      routerCallback = new RouterCallback(networkClient, backgroundDeleteRequests);
      putManager =
          new PutManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics, routerCallback,
              suffix, kms, cryptoService, cryptoJobHandler, accountService, time, defaultPartitionClass);
      getManager =
          new GetManager(clusterMap, responseHandler, routerConfig, routerMetrics, routerCallback, kms, cryptoService,
              cryptoJobHandler, time);
      deleteManager = new DeleteManager(clusterMap, responseHandler, accountService, notificationSystem, routerConfig,
          routerMetrics, routerCallback, time);
      ttlUpdateManager =
          new TtlUpdateManager(clusterMap, responseHandler, notificationSystem, accountService, routerConfig,
              routerMetrics, time);
      undeleteManager =
          new UndeleteManager(clusterMap, responseHandler, notificationSystem, accountService, routerConfig,
              routerMetrics, time);
      requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread-" + suffix, this, true);
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
        final Callback<GetBlobResultInternal> callback) throws RouterException {
      getManager.submitGetBlobOperation(blobIdStr, options, callback);
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
        PutBlobOptions options, FutureResult<String> futureResult, Callback<String> callback) {
      if (!putManager.isOpen()) {
        handlePutManagerClosed(blobProperties, false, futureResult, callback);
      } else {
        putManager.submitPutBlobOperation(blobProperties, userMetadata, channel, options, futureResult, callback);
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
        FutureResult<String> futureResult, Callback<String> callback) {
      if (!putManager.isOpen()) {
        handlePutManagerClosed(blobProperties, true, futureResult, callback);
      } else {
        putManager.submitStitchBlobOperation(blobProperties, userMetadata, chunksToStitch, futureResult, callback);
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
        final Callback<Void> callback, boolean attemptChunkDeletes) {
      try {
        deleteManager.submitDeleteBlobOperation(blobIdStr, serviceId, futureResult,
            (Void result, Exception exception) -> {
              if (exception == null && attemptChunkDeletes) {
                try {
                  initiateChunkDeletesIfAny(blobIdStr, serviceId);
                } catch (RouterException e) {
                  logger.warn(
                      "RouterException for same reason should have been thrown by submitDeleteBlobOperation() and no callback should be triggered.",
                      e);
                }
              }
              if (callback != null) {
                callback.onCompletion(result, exception);
              }
            });
      } catch (RouterException e) {
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.onDeleteBlobError(e);
        NonBlockingRouter.completeOperation(futureResult, callback, null, e);
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
        final Callback<Void> callback) {
      doOperationTowardsMaybeCompositeBlob(blobIdStr, futureResult, callback,
          new CompositeBlobOperationHelper("UNDELETE", GetOption.Include_Deleted_Blobs, routerMetrics.ageAtUndelete,
              (blobIds) -> {
                doUndeleteOperation(blobIds, serviceId, futureResult, callback);
              }, (exception) -> {
            completeUndeleteBlobOperation(exception, futureResult, callback);
          }));
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
     */
    protected void updateBlobTtl(final String blobIdStr, final String serviceId, long expiresAtMs,
        FutureResult<Void> futureResult, Callback<Void> callback) {
      doOperationTowardsMaybeCompositeBlob(blobIdStr, futureResult, callback,
          new CompositeBlobOperationHelper("TTL UPDATE", GetOption.None, routerMetrics.ageAtTtlUpdate, (blobIds) -> {
            doUpdateTtlOperation(blobIds, serviceId, expiresAtMs, futureResult, callback);
          }, (exception) -> {
            completeUpdateBlobTtlOperation(exception, futureResult, callback);
          }));
    }

    /**
     * A helper method to perform ttl update and undelete operation on a composite blob.
     * @param blobIdStr The ID of the blob that needs the ttl update in string form
     *                    permanent
     * @param futureResult A future that would contain the BlobId eventually.
     * @param callback The {@link Callback} which will be invoked on the completion of the request.
     * @param helper The {@link CompositeBlobOperationHelper} that carries other information about this operation.
     */
    protected void doOperationTowardsMaybeCompositeBlob(final String blobIdStr, FutureResult<Void> futureResult,
        Callback<Void> callback, CompositeBlobOperationHelper helper) {
      // Can skip GET if we can determine this is not a metadata blob
      if (isMaybeMetadataBlob(blobIdStr)) {
        Callback<GetBlobResultInternal> internalCallback = (GetBlobResultInternal result, Exception exception) -> {
          if (exception != null) {
            completeOperation(futureResult, callback, null, exception, false);
          } else if (result.getBlobResult != null) {
            exception = new RouterException(
                String.format("GET blob call returned the blob instead of just the store keys (before {} )",
                    helper.opName), RouterErrorCode.UnexpectedInternalError);
            completeOperation(futureResult, callback, null, exception, false);
          } else {
            List<String> blobIdStrs = new ArrayList<>();
            blobIdStrs.add(blobIdStr);
            if (result.storeKeys != null) {
              result.storeKeys.forEach(key -> blobIdStrs.add(key.getID()));
            }
            currentOperationsCount.addAndGet(blobIdStrs.size());
            helper.doOperation.accept(blobIdStrs);
          }
        };

        GetBlobOptions options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
            .getOption(helper.getOption)
            .build();
        GetBlobOptionsInternal optionsInternal = new GetBlobOptionsInternal(options, true, helper.metrics);
        try {
          getBlob(blobIdStr, optionsInternal, internalCallback);
        } catch (RouterException e) {
          helper.completeOperationAtException.accept(e);
        }
      } else {
        // do update directly on single blobId
        routerMetrics.skippedGetBlobCount.inc();
        helper.doOperation.accept(Collections.singletonList(blobIdStr));
      }
    }

    /**
     * Helper method that submits the TTL update operation and handles exceptions.
     * @param blobIdStrs The original blobId strings
     * @param serviceId The service ID of the service updating the ttl of the blob(s). This can be null if unknown.
     * @param expiresAtMs The new expiry time (in ms) of the blob.
     * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
     * @param callback The {@link Callback} that will be called on completion of the request.
     */
    private void doUpdateTtlOperation(List<String> blobIdStrs, final String serviceId, long expiresAtMs,
        FutureResult<Void> futureResult, Callback<Void> callback) {
      try {
        ttlUpdateManager.submitTtlUpdateOperation(blobIdStrs, serviceId, expiresAtMs, futureResult, callback);
        routerCallback.onPollReady();
      } catch (RouterException e) {
        currentOperationsCount.addAndGet(1 - blobIdStrs.size());
        completeUpdateBlobTtlOperation(e, futureResult, callback);
      }
    }

    /**
     * Helper method that submits the undelete operation and handles exceptions.
     * @param blobIdStrs The original blobId strings
     * @param serviceId The service ID of the service undeleting the blob(s). This can be null if unknown.
     * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
     * @param callback The {@link Callback} that will be called on completion of the request.
     */
    private void doUndeleteOperation(List<String> blobIdStrs, final String serviceId, FutureResult<Void> futureResult,
        Callback<Void> callback) {
      try {
        undeleteManager.submitUndeleteOperation(blobIdStrs, serviceId, futureResult, callback);
        routerCallback.onPollReady();
      } catch (RouterException e) {
        currentOperationsCount.addAndGet(1 - blobIdStrs.size());
        completeUndeleteBlobOperation(e, futureResult, callback);
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
      completeOperation(futureResult, callback, null, routerException);
      // Close so that any existing operations are also disposed off.
      close();
    }

    /**
     * Shuts down the OperationController and cleans up all the resources associated with it.
     */
    private void shutdown() {
      logger.info("OperationController is shutting down");
      try {
        if (!shutDownLatch.await(SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
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
        initiateBackgroundDeletes(backgroundDeleteRequests);
        backgroundDeleteRequests.clear();
        deleteManager.poll(requestsToSend, requestsToDrop);
        ttlUpdateManager.poll(requestsToSend, requestsToDrop);
        undeleteManager.poll(requestsToSend, requestsToDrop);
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
            long responseReceiveTime = requestInfo.getStreamHeaderFrameReceiveTime();
            if (responseReceiveTime != -1) {
              routerMetrics.responseReceiveToHandleLatencyMs.update(System.currentTimeMillis() - responseReceiveTime);
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
              default:
                logger.error("Unexpected response type: " + type + " received, discarding");
            }
          }
        } catch (Exception e) {
          logger.error("Unexpected error received while handling a response: ", e);
          routerMetrics.operationManagerHandleResponseErrorCount.inc();
        }
      }
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
      final int NETWORK_CLIENT_POLL_TIMEOUT = routerConfig.routerRequestTimeoutMs / 10;
      try {
        while (isOpen.get()) {
          List<RequestInfo> requestsToSend = new ArrayList<>();
          Set<Integer> requestsToDrop = new HashSet<>();
          pollForRequests(requestsToSend, requestsToDrop);

          List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestsToSend,
              routerConfig.routerDropRequestOnTimeout ? requestsToDrop : Collections.emptySet(),
              NETWORK_CLIENT_POLL_TIMEOUT);
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
        shutDownOperationControllers();
      }
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
  private class BackgroundDeleter extends OperationController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Instantiate the BackgroundDeleter
     * @throws IOException if the associated {@link OperationController} throws one.
     */
    BackgroundDeleter() throws IOException {
      super("backgroundDeleter", null, accountService);
      putManager.close();
      ttlUpdateManager.close();
    }

    /**
     * Put operations are disallowed in the BackgroundDeleter.
     */
    @Override
    protected void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        PutBlobOptions options, FutureResult<String> futureResult, Callback<String> callback) {
      RouterException routerException = new RouterException("Illegal attempt to put blob through BackgroundDeleter",
          RouterErrorCode.UnexpectedInternalError);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted(), false);
      completeOperation(futureResult, callback, null, routerException);
    }

    /**
     * Stitch operations are disallowed in the BackgroundDeleter.
     */
    @Override
    protected void stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
        FutureResult<String> futureResult, Callback<String> callback) {
      RouterException routerException = new RouterException("Illegal attempt to stitch blob through BackgroundDeleter",
          RouterErrorCode.UnexpectedInternalError);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted(), true);
      completeOperation(futureResult, callback, null, routerException);
    }

    /**
     * TTL update operations are disallowed in the BackgroundDeleter.
     */
    @Override
    protected void updateBlobTtl(String blobIdStr, final String serviceId, long expiresAtMs,
        FutureResult<Void> futureResult, Callback<Void> callback) {
      RouterException routerException =
          new RouterException("Illegal attempt to update TTL of blob through BackgroundDeleter",
              RouterErrorCode.UnexpectedInternalError);
      completeUpdateBlobTtlOperation(routerException, futureResult, callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void pollForRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
      try {
        getManager.poll(requestsToSend, requestsToDrop);
        deleteManager.poll(requestsToSend, requestsToDrop);
      } catch (Exception e) {
        logger.error("Background Deleter Operation Manager poll received an unexpected error: ", e);
        routerMetrics.operationManagerPollErrorCount.inc();
      }
    }
  }

  /**
   * A helper class that carries information for ttl update and undelete operation on a potential composite blob.
   */
  class CompositeBlobOperationHelper {
    String opName;
    GetOption getOption;
    NonBlockingRouterMetrics.AgeAtAccessMetrics metrics;
    Consumer<List<String>> doOperation;
    Consumer<RouterException> completeOperationAtException;

    /**
     * Constructor of this helper object.
     * @param opName The name of this operation.
     * @param getOption The {@link GetOption} while fetching the blob ids of a composite blob.
     * @param metrics The {@link NonBlockingRouterMetrics.AgeAtAccessMetrics} to use while fetching blob ids of a composite
     *                blob with {@link GetManager}.
     * @param doOperation The function to call to submit a list of blob ids in String to corresponding manager after fetching
     *                    blob ids.
     * @param completeOperationAtException The function to call when there is exception when calling {@link  #getBlob}.
     */
    CompositeBlobOperationHelper(String opName, GetOption getOption,
        NonBlockingRouterMetrics.AgeAtAccessMetrics metrics, Consumer<List<String>> doOperation,
        Consumer<RouterException> completeOperationAtException) {
      this.opName = opName;
      this.getOption = getOption;
      this.metrics = metrics;
      this.doOperation = doOperation;
      this.completeOperationAtException = completeOperationAtException;
    }
  }
}

