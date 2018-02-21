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

import com.codahale.metrics.Meter;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
  private final Time time;

  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouter.class);
  static final AtomicInteger currentOperationsCount = new AtomicInteger(0);
  private final AtomicInteger currentBackgroundOperationsCount = new AtomicInteger(0);
  static final int MAX_IN_MEM_CHUNKS = 4;
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
   * @param time the time instance.
   * @throws IOException if the OperationController could not be successfully created.
   */
  NonBlockingRouter(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkClientFactory networkClientFactory, NotificationSystem notificationSystem, ClusterMap clusterMap,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time)
      throws IOException {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.networkClientFactory = networkClientFactory;
    this.notificationSystem = notificationSystem;
    this.clusterMap = clusterMap;
    responseHandler = new ResponseHandler(clusterMap);
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.time = time;
    ocCount = routerConfig.routerScalingUnitCount;
    ocList = new ArrayList<>();
    for (int i = 0; i < ocCount; i++) {
      ocList.add(new OperationController(Integer.toString(i)));
    }
    backgroundDeleter = new BackgroundDeleter();
    ocList.add(backgroundDeleter);
    routerMetrics.initializeNumActiveOperationsMetrics(currentOperationsCount, currentBackgroundOperationsCount);
  }

  /**
   * Returns an {@link OperationController}
   * @return a randomly picked {@link OperationController} from the list of OperationControllers.
   */
  private OperationController getOperationController() {
    return ocList.get(ThreadLocalRandom.current().nextInt(ocCount));
  }

  /**
   * Requests for blob data asynchronously with user-set {@link GetBlobOptions} and returns a future that will
   * eventually contain a {@link GetBlobResult} that can contain either the {@link BlobInfo}, the
   * {@link ReadableStreamChannel} containing the blob data, or both.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param options The options associated with the request. This cannot be null.
   * @return A future that would eventually contain a {@link GetBlobResult} that can contain either
   *         the {@link BlobInfo}, the {@link ReadableStreamChannel} containing the blob data, or both.
   */
  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options) {
    return getBlob(blobId, options, null);
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
    try {
      BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
      if (blobId.getDatacenterId() != ClusterMapUtils.UNKNOWN_DATACENTER_ID
          && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId()) {
        routerMetrics.getBlobNotOriginateLocalOperationRate.mark();
      }
      trackGetBlobRateMetrics(options, blobId.isEncrypted());
      routerMetrics.operationQueuingRate.mark();
      if (isOpen.get()) {
        getOperationController().getBlob(blobId, internalOptions, new Callback<GetBlobResultInternal>() {
          @Override
          public void onCompletion(GetBlobResultInternal internalResult, Exception exception) {
            GetBlobResult getBlobResult = internalResult == null ? null : internalResult.getBlobResult;
            futureResult.done(getBlobResult, exception);
            if (callback != null) {
              callback.onCompletion(getBlobResult, exception);
            }
          }
        });
      } else {
        RouterException routerException =
            new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
        completeGetBlobOperation(routerException, internalOptions, futureResult, callback, blobId.isEncrypted());
      }
    } catch (RouterException e) {
      completeGetBlobOperation(e, internalOptions, futureResult, callback, false);
    }
    return futureResult;
  }

  /**
   * Requests for a new blob to be put asynchronously and returns a future that will eventually contain the BlobId of
   * the new blob on a successful response.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel) {
    return putBlob(blobProperties, userMetadata, channel, null);
  }

  /**
   * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    if (blobProperties == null || channel == null) {
      throw new IllegalArgumentException("blobProperties or channel must not be null");
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
    FutureResult<String> futureResult = new FutureResult<String>();
    if (isOpen.get()) {
      getOperationController().putBlob(blobProperties, userMetadata, channel, futureResult, callback);
    } else {
      RouterException routerException =
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted());
      completeOperation(futureResult, callback, null, routerException);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be deleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, String serviceId) {
    return deleteBlob(blobId, serviceId, null);
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   *@param callback The {@link Callback} which will be invoked on the completion of a request.  @return A future that would contain information about whether the deletion succeeded or not, eventually.
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
      getOperationController().deleteBlob(blobId, serviceId, futureResult, callback, true);
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
   * @param blobId the {@link BlobId} associated with the possibly composite blob.
   * @param serviceId the service ID associated with the original delete request.
   */
  private void initiateChunkDeletesIfAny(final BlobId blobId, final String serviceId) {
    Callback<GetBlobResultInternal> callback = (GetBlobResultInternal result, Exception exception) -> {
      if (exception != null) {
        // It is expected that these requests will not always succeed. For example, this may have been triggered by a
        // duplicate delete and the blob could have already been hard deleted, so the deserialization can fail, or the
        // blob could have been garbage collected and not found at all and so on.
        logger.trace("Encountered exception when attempting to get chunks of a possibly composite deleted blob {} ",
            blobId, exception);
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
    GetBlobOptionsInternal options =
        new GetBlobOptionsInternal(new GetBlobOptions(GetBlobOptions.OperationType.All, GetOption.Include_All, null),
            true, routerMetrics.ageAtDelete);
    backgroundDeleter.getBlob(blobId, options, callback);
  }

  /**
   * Track get blob rate metrics based on the {@link GetBlobOptions} and whether the blob is encrypted or not
   * @param options {@link GetBlobOptions} instance to use
   * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
   */
  private void trackGetBlobRateMetrics(GetBlobOptions options, boolean isEncrypted) {
    if (options.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
      Meter blobInfoOperationRate =
          isEncrypted ? routerMetrics.getEncryptedBlobInfoOperationRate : routerMetrics.getBlobInfoOperationRate;
      blobInfoOperationRate.mark();
    } else {
      Meter blobOperationRate =
          isEncrypted ? routerMetrics.getEncryptedBlobOperationRate : routerMetrics.getBlobOperationRate;
      blobOperationRate.mark();
    }
    if (options.getRange() != null) {
      Meter blobWithRangeOperationRate = isEncrypted ? routerMetrics.getEncryptedBlobWithRangeOperationRate
          : routerMetrics.getBlobWithRangeOperationRate;
      blobWithRangeOperationRate.mark();
    }
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
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   * @param <T> the type of the operation result, which depends on the kind of operation.
   */
  static <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception) {
    NonBlockingRouter.currentOperationsCount.decrementAndGet();
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
    private final NetworkClient networkClient;
    private final Thread requestResponseHandlerThread;
    private final CountDownLatch shutDownLatch = new CountDownLatch(1);
    protected final RouterCallback routerCallback;
    private final List<BackgroundDeleteRequest> backgroundDeleteRequests = new ArrayList<>();

    /**
     * Constructs an OperationController
     * @param suffix the suffix to associate with the thread names of this OperationController
     * @throws IOException if the network components could not be created.
     */
    OperationController(String suffix) throws IOException {
      networkClient = networkClientFactory.getNetworkClient();
      routerCallback = new RouterCallback(networkClient, backgroundDeleteRequests);
      putManager =
          new PutManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics, routerCallback,
              suffix, kms, cryptoService, cryptoJobHandler, time);
      getManager =
          new GetManager(clusterMap, responseHandler, routerConfig, routerMetrics, routerCallback, kms, cryptoService,
              cryptoJobHandler, time);
      deleteManager = new DeleteManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics,
          routerCallback, time);
      requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread-" + suffix, this, true);
      requestResponseHandlerThread.start();
      routerMetrics.initializeOperationControllerMetrics(requestResponseHandlerThread);
    }

    /**
     * Requests for the blob (info, data, or both) asynchronously and invokes the {@link Callback} when the request
     * completes.
     * @param blobId The {@link BlobId} for which blob data is requested.
     * @param options The {@link GetBlobOptionsInternal} associated with the request.
     * @param callback The callback which will be invoked on the completion of the request.
     */
    protected void getBlob(BlobId blobId, GetBlobOptionsInternal options,
        final Callback<GetBlobResultInternal> callback) {
      getManager.submitGetBlobOperation(blobId, options, callback);
      routerCallback.onPollReady();
    }

    /**
     * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
     * @param blobProperties The properties of the blob.
     * @param userMetadata Optional user metadata about the blob. This can be null.
     * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
     * @param futureResult A future that would contain the BlobId eventually.
     * @param callback The {@link Callback} which will be invoked on the completion of the request .
     */
    protected void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        FutureResult<String> futureResult, Callback<String> callback) {
      if (!putManager.isOpen()) {
        RouterException routerException =
            new RouterException(" because Router is closed", RouterErrorCode.RouterClosed);
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted());
        completeOperation(futureResult, callback, null, routerException);
        // Close so that any existing operations are also disposed off.
        close();
      } else {
        putManager.submitPutBlobOperation(blobProperties, userMetadata, channel, futureResult, callback);
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
        final BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
        if (blobId.getDatacenterId() != ClusterMapUtils.UNKNOWN_DATACENTER_ID
            && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId()) {
          routerMetrics.deleteBlobNotOriginateLocalOperationRate.mark();
        }
        deleteManager.submitDeleteBlobOperation(blobId, serviceId, futureResult, (Void result, Exception exception) -> {
          if (exception == null && attemptChunkDeletes) {
            initiateChunkDeletesIfAny(blobId, serviceId);
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
    }

    /**
     * This method is used by the RequestResponseHandler thread to poll for requests to be sent
     * @return a list of {@link RequestInfo} that contains the requests to be sent out.
     */
    protected List<RequestInfo> pollForRequests() {
      List<RequestInfo> requests = new ArrayList<>();
      try {
        putManager.poll(requests);
        getManager.poll(requests);
        initiateBackgroundDeletes(backgroundDeleteRequests);
        backgroundDeleteRequests.clear();
        deleteManager.poll(requests);
      } catch (Exception e) {
        logger.error("Operation Manager poll received an unexpected error: ", e);
        routerMetrics.operationManagerPollErrorCount.inc();
      }
      return requests;
    }

    /**
     * Handle the response from polling the {@link NetworkClient}.
     * @param responseInfoList the list of {@link ResponseInfo} containing the responses.
     */
    protected void onResponse(List<ResponseInfo> responseInfoList) {
      for (ResponseInfo responseInfo : responseInfoList) {
        try {
          RouterRequestInfo routerRequestInfo = (RouterRequestInfo) responseInfo.getRequestInfo();
          RequestOrResponseType type = ((RequestOrResponse) routerRequestInfo.getRequest()).getRequestType();
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
            default:
              logger.error("Unexpected response type: " + type + " received, discarding");
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
          List<RequestInfo> requestInfoList = pollForRequests();
          List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestInfoList, NETWORK_CLIENT_POLL_TIMEOUT);
          onResponse(responseInfoList);
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
      super("backgroundDeleter");
      putManager.close();
    }

    /**
     * Put operations are disallowed in the BackgroundDeleter.
     */
    @Override
    protected void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        FutureResult<String> futureResult, Callback<String> callback) {
      RouterException routerException =
          new RouterException("Illegal attempt to put blob through backgroundDeleteOperationController",
              RouterErrorCode.UnexpectedInternalError);
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.onPutBlobError(routerException, blobProperties != null && blobProperties.isEncrypted());
      completeOperation(futureResult, callback, null, routerException);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<RequestInfo> pollForRequests() {
      List<RequestInfo> requests = new ArrayList<>();
      try {
        getManager.poll(requests);
        deleteManager.poll(requests);
      } catch (Exception e) {
        logger.error("Background Deleter Operation Manager poll received an unexpected error: ", e);
        routerMetrics.operationManagerPollErrorCount.inc();
      }
      return requests;
    }
  }
}

