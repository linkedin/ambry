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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
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
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  // Shared with the operation managers.
  private final RouterConfig routerConfig;
  private final NotificationSystem notificationSystem;
  private final ClusterMap clusterMap;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ResponseHandler responseHandler;
  private final Time time;
  private final List<String> idsToDelete = new ArrayList<String>();

  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouter.class);
  private final AtomicInteger currentOperationsCount = new AtomicInteger(0);
  private final OperationCompleteCallback operationCompleteCallback =
      new OperationCompleteCallback(currentOperationsCount);

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
   * @param time the time instance.
   * @throws IOException if the OperationController could not be successfully created.
   */
  NonBlockingRouter(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkClientFactory networkClientFactory, NotificationSystem notificationSystem, ClusterMap clusterMap,
      Time time)
      throws IOException {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.networkClientFactory = networkClientFactory;
    this.notificationSystem = notificationSystem;
    this.clusterMap = clusterMap;
    this.responseHandler = new ResponseHandler(clusterMap);
    this.time = time;
    ocList = new ArrayList<OperationController>(routerConfig.routerScalingUnitCount);
    for (int i = 0; i < routerConfig.routerScalingUnitCount; i++) {
      ocList.add(new OperationController());
    }
  }

  /**
   * Returns an {@link OperationController}
   * @return a randomly picked {@link OperationController} from the list of OperationControllers.
   */
  private OperationController getOperationController() {
    return ocList.get(ThreadLocalRandom.current().nextInt(ocList.size()));
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and returns a future that will eventually contain the
   * {@link BlobInfo} on a successful response.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getBlobInfo(blobId, null);
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    currentOperationsCount.incrementAndGet();
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    if (isOpen.get()) {
      getOperationController().getBlobInfo(blobId, futureResult, callback);
    } else {
      operationCompleteCallback.completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    }
    return futureResult;
  }

  /**
   * Requests for blob data asynchronously and returns a future that will eventually contain a
   * {@link ReadableStreamChannel} that represents blob data on a successful response.
   * @param blobId The ID of the blob for which blob data is requested.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    return getBlob(blobId, null);
  }

  /**
   * Requests for the blob data asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param callback The callback which will be invoked on the completion of the request.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    currentOperationsCount.incrementAndGet();
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    if (isOpen.get()) {
      getOperationController().getBlob(blobId, futureResult, callback);
    } else {
      operationCompleteCallback.completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    }
    return futureResult;
  }

  /**
   * Requests for a new blob to be put asynchronously and returns a future that will eventually contain the BlobId of
   * the new blob on a successful response.
   * @param blobProperties The properties of the blob.
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
   * @param blobProperties The properties of the blob.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    currentOperationsCount.incrementAndGet();
    FutureResult<String> futureResult = new FutureResult<String>();
    if (isOpen.get()) {
      getOperationController().putBlob(blobProperties, userMetadata, channel, futureResult, callback);
    } else {
      operationCompleteCallback.completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be deleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be deleted.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    currentOperationsCount.incrementAndGet();
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (isOpen.get()) {
      getOperationController().deleteBlob(blobId, futureResult, callback);
    } else {
      operationCompleteCallback.completeOperation(futureResult, callback, null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
    }
    return futureResult;
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
   * Return an approximate count of the number of operations submitted to the router that are not yet completed.
   * @return (approximate) number of operations being handled at the time of this call.
   */
  int getOperationsCount() {
    return currentOperationsCount.get();
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
    private final PutManager putManager;
    private final GetManager getManager;
    private final DeleteManager deleteManager;
    private final NetworkClient networkClient;
    private final Thread requestResponseHandlerThread;
    private final CountDownLatch shutDownLatch = new CountDownLatch(1);

    /**
     * Constructs an OperationController
     * @throws IOException if the network components could not be created.
     */
    OperationController()
        throws IOException {
      networkClient = networkClientFactory.getNetworkClient();
      putManager = new PutManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics,
          operationCompleteCallback, time);
      getManager =
          new GetManager(clusterMap, responseHandler, routerConfig, routerMetrics, operationCompleteCallback, time);
      deleteManager = new DeleteManager(clusterMap, responseHandler, notificationSystem, routerConfig, routerMetrics,
          operationCompleteCallback, time);
      requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread", this, true);
      requestResponseHandlerThread.start();
    }

    /**
     * Requests for the {@link BlobInfo} asynchronously and invokes the {@link Callback} when the request completes.
     * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
     * @param futureResult The future that would contain the {@link BlobInfo} eventually.
     * @param callback The {@link Callback} which will be invoked on the completion of the request.
     */
    private void getBlobInfo(String blobId, FutureResult<BlobInfo> futureResult, Callback<BlobInfo> callback) {
      getManager.submitGetBlobInfoOperation(blobId, futureResult, callback);
    }

    /**
     * Requests for the blob data asynchronously and invokes the {@link Callback} when the request completes.
     * @param blobId The ID of the blob for which blob data is requested.
     * @param futureResult A future that would contain a {@link ReadableStreamChannel} that represents the blob data
     *                     eventually.
     * @param callback The callback which will be invoked on the completion of the request.
     */
    private void getBlob(String blobId, FutureResult<ReadableStreamChannel> futureResult,
        Callback<ReadableStreamChannel> callback) {
      getManager.submitGetBlobOperation(blobId, futureResult, callback);
    }

    /**
     * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
     * @param blobProperties The properties of the blob.
     * @param userMetadata Optional user metadata about the blob. This can be null.
     * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
     * @param futureResult A future that would contain the BlobId eventually.
     * @param callback The {@link Callback} which will be invoked on the completion of the request .
     */
    private void putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        FutureResult<String> futureResult, Callback<String> callback) {
      if (!putManager.isOpen()) {
        operationCompleteCallback.completeOperation(futureResult, callback, null,
            new RouterException("Aborted operation because Router is closed", RouterErrorCode.RouterClosed));
        // Close so that any existing operations are also disposed off.
        close();
      } else {
        putManager.submitPutBlobOperation(blobProperties, userMetadata, channel, futureResult, callback);
      }
    }

    /**
     * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
     * @param blobId The ID of the blob that needs to be deleted.
     * @param futureResult A future that would contain information about whether the deletion succeeded or not,
     *                     eventually.
     * @param callback The {@link Callback} which will be invoked on the completion of a request.
     */
    private void deleteBlob(String blobId, FutureResult<Void> futureResult, Callback<Void> callback) {
      deleteManager.submitDeleteBlobOperation(blobId, futureResult, callback);
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
    private List<RequestInfo> pollForRequests() {
      // these are ids that were successfully put for an operation that eventually failed
      idsToDelete.clear();
      putManager.getIdsToDelete(idsToDelete);
      // this is a best effort to delete ids for cleanup purposes (these may fail and we will
      // not do anything about it at this time).
      for (String blobId : idsToDelete) {
        // possibly add a batch api going forward.
        deleteManager.submitDeleteBlobOperation(blobId, new FutureResult<Void>(), null);
      }
      List<RequestInfo> requests = new ArrayList<RequestInfo>();
      putManager.poll(requests);
      getManager.poll(requests);
      deleteManager.poll(requests);
      return requests;
    }

    /**
     * Handle the response from polling the {@link NetworkClient}.
     * @param responseInfoList the list of {@link ResponseInfo} containing the responses.
     */
    private void onResponse(List<ResponseInfo> responseInfoList) {
      for (ResponseInfo responseInfo : responseInfoList) {
        RequestOrResponseType type = ((RequestOrResponse) responseInfo.getRequest()).getRequestType();
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
      }
    }

    /**
     * The RequestResponseHandler thread simply runs in a loop polling the OperationController for any
     * requests to be sent, and notifies it about network events.
     */
    @Override
    public void run() {
      try {
        while (isOpen.get()) {
          List<RequestInfo> requestInfoList = pollForRequests();
          List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestInfoList);
          onResponse(responseInfoList);
        }
      } catch (Throwable e) {
        logger.error("Aborting, as requestResponseHandlerThread received an unexpected error: ", e);
      } finally {
        networkClient.close();
        shutDownLatch.countDown();
        // Close the router.
        shutDownOperationControllers();
      }
    }
  }
}

/**
 * An object of this class is passed by the router to the operation managers to use to complete operations.
 */
class OperationCompleteCallback {
  private final AtomicInteger operationsCount;
  private static final Logger logger = LoggerFactory.getLogger(OperationCompleteCallback.class);

  /**
   * Construct an OperationCompleteCallback object
   * @param operationsCount the operationsCount to decrement whenever an operation is completed.
   */
  OperationCompleteCallback(AtomicInteger operationsCount) {
    this.operationsCount = operationsCount;
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
  <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception) {
    operationsCount.decrementAndGet();
    try {
      futureResult.done(operationResult, exception);
      if (callback != null) {
        callback.onCompletion(operationResult, exception);
      }
    } catch (Exception e) {
      logger.error("Exception caught during future and callback completion", e);
    }
  }
}
