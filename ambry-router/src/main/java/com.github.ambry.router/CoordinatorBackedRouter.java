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

import com.codahale.metrics.Histogram;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link Router} that uses a {@link Coordinator} in the background to perform its operations.
 * <p/>
 * The CoordinatorBackedRouter allocates a thread pool of size {@link RouterConfig#routerScalingUnitCount} on
 * instantiation. This thread pool is used to provide non-blocking behavior by executing the blocking operations of the
 * {@link Coordinator} in the background.
 * <p/>
 * If the thread pool is exhausted, the CoordinatorBackedRouter remains non-blocking but latency of operations will
 * spike.
 */
public class CoordinatorBackedRouter implements Router {
  private static RouterException ROUTER_CLOSED_EXCEPTION =
      new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);

  protected final CoordinatorBackedRouterMetrics metrics;
  protected final Coordinator coordinator;

  private final AtomicBoolean routerOpen = new AtomicBoolean(true);
  private final ExecutorService operationPool;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a CoordinatorBackedRouter instance.
   * @param routerConfig the {@link RouterConfig} to refer to.
   * @param metrics the {@link CoordinatorBackedRouterMetrics} instance to use for metrics.
   * @param coordinator the {@link Coordinator} that will back this router.
   * @throws IllegalArgumentException if any of the arguments received are null.
   */
  public CoordinatorBackedRouter(RouterConfig routerConfig, CoordinatorBackedRouterMetrics metrics,
      Coordinator coordinator) {
    if (routerConfig == null || metrics == null || coordinator == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of CoordinatorBackedRouter");
    }
    this.operationPool = Executors.newFixedThreadPool(routerConfig.routerScalingUnitCount);
    this.metrics = metrics;
    this.coordinator = coordinator;
    logger.trace("Instantiated CoordinatorBackedRouter");
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getBlobInfo(blobId, null);
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    long preProcessingStartTime = System.currentTimeMillis();
    try {
      metrics.getBlobInfoRate.mark();
      logger.trace("Beginning getBlobInfo for {}", blobId);
      FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
      CoordinatorOperation operation =
          new CoordinatorOperation(this, futureResult, blobId, callback, CoordinatorOperationType.GetBlobInfo);
      submitOperation(operation, futureResult, callback);
      return futureResult;
    } finally {
      metrics.operationPreProcessingTimeInMs.update(System.currentTimeMillis() - preProcessingStartTime);
    }
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    return getBlob(blobId, null);
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    long preProcessingStartTime = System.currentTimeMillis();
    try {
      metrics.getBlobRate.mark();
      logger.trace("Beginning getBlob for {}", blobId);
      FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
      CoordinatorOperation operation =
          new CoordinatorOperation(this, futureResult, blobId, callback, CoordinatorOperationType.GetBlob);
      submitOperation(operation, futureResult, callback);
      return futureResult;
    } finally {
      metrics.operationPreProcessingTimeInMs.update(System.currentTimeMillis() - preProcessingStartTime);
    }
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel) {
    return putBlob(blobProperties, usermetadata, channel, null);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    long preProcessingStartTime = System.currentTimeMillis();
    try {
      metrics.putBlobRate.mark();
      logger.trace("Beginning putBlob");
      FutureResult<String> futureResult = new FutureResult<String>();
      CoordinatorOperation operation =
          new CoordinatorOperation(this, futureResult, blobProperties, usermetadata, channel, callback);
      submitOperation(operation, futureResult, callback);
      return futureResult;
    } finally {
      metrics.operationPreProcessingTimeInMs.update(System.currentTimeMillis() - preProcessingStartTime);
    }
  }

  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    long preProcessingStartTime = System.currentTimeMillis();
    try {
      metrics.deleteBlobRate.mark();
      logger.trace("Beginning deleteBlob for {}", blobId);
      FutureResult<Void> futureResult = new FutureResult<Void>();
      CoordinatorOperation operation =
          new CoordinatorOperation(this, futureResult, blobId, callback, CoordinatorOperationType.DeleteBlob);
      submitOperation(operation, futureResult, callback);
      return futureResult;
    } finally {
      metrics.operationPreProcessingTimeInMs.update(System.currentTimeMillis() - preProcessingStartTime);
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (routerOpen.compareAndSet(true, false)) {
        logger.info("CoordinatorBackedRouter closing");
        long closeStartTime = System.currentTimeMillis();
        operationPool.shutdown();
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
        coordinator.close();
        metrics.closeTimeInMs.update(System.currentTimeMillis() - closeStartTime);
        logger.info("CoordinatorBackedRouter closed");
      } else {
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
      }
    } catch (InterruptedException e) {
      metrics.closeError.inc();
      logger.error("Error shutting down operationPool in CoordinatorBackedRouter", e);
    }
  }

  /**
   * Submits a {@link CoordinatorOperation} for execution. Immediately completes the operation with an exception if the
   * router is closed or if the task submission fails.
   * @param coordinatorOperation the {@link CoordinatorOperation} that needs to be executed.
   * @param futureResult the {@link FutureResult} that will hold the result of the operation eventually.
   * @param callback the {@link Callback} that will be invoked when the operation completes. Can be null.
   */
  private void submitOperation(CoordinatorOperation coordinatorOperation, FutureResult futureResult,
      Callback callback) {
    if (routerOpen.get()) {
      try {
        metrics.totalOperationsInFlight.incrementAndGet();
        coordinatorOperation.onQueue();
        operationPool.submit(coordinatorOperation);
      } catch (Exception e) {
        completeOperation(futureResult, callback, null, e);
      }
    } else {
      metrics.unavailableError.inc();
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    }
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   */
  protected void completeOperation(FutureResult futureResult, Callback callback, Object operationResult,
      Exception exception) {
    long postProcessingStartTime = System.currentTimeMillis();
    try {
      futureResult.done(operationResult, exception);
      if (callback != null) {
        callback.onCompletion(operationResult, exception);
      }
    } catch (Exception e) {
      metrics.futureCallbackError.inc();
      logger.error("Exception caught during future and callback completion", e);
      if (operationResult instanceof ReadableStreamChannel) {
        try {
          logger.trace("Closing ReadableStreamChannel due to exception");
          ((ReadableStreamChannel) operationResult).close();
        } catch (IOException channelCloseException) {
          logger.error("Ignoring IOException during ReadableStreamChannel close");
        }
      }
    } finally {
      metrics.operationPostProcessingTimeInMs.update(System.currentTimeMillis() - postProcessingStartTime);
      metrics.totalOperationsInFlight.decrementAndGet();
    }
  }
}

/**
 * Specifies the operation required of the {@link Coordinator}.
 */
enum CoordinatorOperationType {
  GetBlob,
  GetBlobInfo,
  PutBlob,
  DeleteBlob
}

/**
 * Thread that performs the required {@link CoordinatorOperationType} using the blocking APIs of a {@link Coordinator}.
 */
class CoordinatorOperation implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Long operationQueueStartTime;

  // general
  private final CoordinatorBackedRouter router;
  private final FutureResult futureResult;
  private final Callback callback;
  private final CoordinatorOperationType opType;

  // getBlob, getBlobInfo and delete arguments.
  private String blobId;

  // put arguments. Might be used to store returns in GetBlob and GetBlobInfo too.
  private BlobProperties blobProperties;
  private byte[] usermetadata;
  private ReadableStreamChannel channel;

  /**
   * Constructor used to invoke {@link Coordinator} equivalent operations for {@link Router#getBlob(String)},
   * {@link Router#getBlobInfo(String)} and {@link Router#deleteBlob(String)} (and their variants).
   * @param router the {@link CoordinatorBackedRouter} instance to use to submit operations.
   * @param futureResult the {@link FutureResult} where the final result has to be loaded.
   * @param blobId the blob id that the operation needs to be performed on.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   * @param opType the {@link CoordinatorOperationType} required. Can only be one of
   *                {@link CoordinatorOperationType#GetBlob}, {@link CoordinatorOperationType#GetBlobInfo} or
   *                {@link CoordinatorOperationType#DeleteBlob}.
   * @throws IllegalArgumentException if {@code opType} is {@link CoordinatorOperationType#PutBlob}.
   */
  public CoordinatorOperation(CoordinatorBackedRouter router, FutureResult futureResult, String blobId,
      Callback callback, CoordinatorOperationType opType) {
    this(router, futureResult, callback, opType);
    if (CoordinatorOperationType.PutBlob.equals(opType)) {
      throw new IllegalArgumentException("This constructor cannot be used for the putBlob operation");
    }
    this.blobId = blobId;
  }

  /**
   * Constructor used to invoke {@link Coordinator} equivalent operations for
   * {@link Router#putBlob(BlobProperties, byte[], ReadableStreamChannel)} and its variant.
   * @param router the {@link CoordinatorBackedRouter} instance to use to submit operations.
   * @param futureResult the {@link FutureResult} where the final result has to be loaded.
   * @param blobProperties the properties of the blob.
   * @param usermetadata user specified metadata as a byte array.
   * @param channel the {@link ReadableStreamChannel} to read the blob data from.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   */
  public CoordinatorOperation(CoordinatorBackedRouter router, FutureResult futureResult, BlobProperties blobProperties,
      byte[] usermetadata, ReadableStreamChannel channel, Callback callback) {
    this(router, futureResult, callback, CoordinatorOperationType.PutBlob);
    this.blobProperties = blobProperties;
    this.usermetadata = usermetadata;
    this.channel = channel;
  }

  private CoordinatorOperation(CoordinatorBackedRouter router, FutureResult futureResult, Callback callback,
      CoordinatorOperationType opType) {
    this.router = router;
    this.futureResult = futureResult;
    this.callback = callback;
    this.opType = opType;
    logger.trace("Instantiated CoordinatorOperation for {} operation", this.opType);
  }

  @Override
  public void run() {
    long operationStartTime = System.currentTimeMillis();
    router.metrics.totalOperationsInExecution.incrementAndGet();
    onDequeue();
    Histogram operationTotalTimeTracker = null;
    Object operationResult = null;
    Exception exception = null;
    try {
      switch (opType) {
        case GetBlob:
          logger.trace("Beginning coordinator getBlob");
          BlobOutput blobOutput = router.coordinator.getBlob(blobId);
          long getBlobEndTime = System.currentTimeMillis();
          router.metrics.getBlobTimeInMs.update(getBlobEndTime - operationStartTime);
          // (int) blobOutput.getSize() will not work for blobs >2GB in size but that is not a concern right now.
          // CoordinatorBackedRouter will be long gone before (if) we support blobs with that size.
          logger.trace("Finished coordinator getBlob");

          byte[] buf = Utils.readBytesFromStream(blobOutput.getStream(), (int) blobOutput.getSize());
          logger.trace("Blob data has completely arrived");
          operationResult = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(buf));
          router.metrics.getBlobResultConstructionTimeInMs.update(System.currentTimeMillis() - getBlobEndTime);
          operationTotalTimeTracker = router.metrics.getBlobTotalTimeInMs;
          break;
        case GetBlobInfo:
          logger.trace("Beginning coordinator getBlobProperties");
          blobProperties = router.coordinator.getBlobProperties(blobId);
          long getBlobPropsEndTime = System.currentTimeMillis();
          router.metrics.getBlobPropertiesTimeInMs.update(getBlobPropsEndTime - operationStartTime);
          logger.trace("Finished coordinator getBlobProperties. Beginning coordinator getBlobUserMetadata");

          ByteBuffer usermetadataBuffer = router.coordinator.getBlobUserMetadata(blobId);
          long getBlobUserMetadataEndTime = System.currentTimeMillis();
          router.metrics.getUserMetadataTimeInMs.update(getBlobUserMetadataEndTime - getBlobPropsEndTime);
          logger.trace("Finished coordinator getBlobUserMetadata.");

          if (usermetadataBuffer.hasArray()) {
            usermetadata = usermetadataBuffer.array();
          } else {
            logger.trace("Usermetadata buffer was not backed by an array. Copying the data");
            usermetadata = new byte[usermetadataBuffer.capacity()];
            usermetadataBuffer.get(usermetadata);
          }
          operationResult = new BlobInfo(blobProperties, usermetadata);
          router.metrics.getBlobInfoResultConstructionTimeInMs
              .update(System.currentTimeMillis() - getBlobUserMetadataEndTime);
          operationTotalTimeTracker = router.metrics.getBlobInfoTotalTimeInMs;
          break;
        case PutBlob:
          logger.trace("Beginning coordinator putBlob");
          operationResult = router.coordinator
              .putBlob(blobProperties, ByteBuffer.wrap(usermetadata), new ReadableStreamChannelInputStream(channel));
          logger.trace("Finished coordinator putBlob");
          operationTotalTimeTracker = router.metrics.putBlobTotalTimeInMs;
          break;
        case DeleteBlob:
          logger.trace("Beginning coordinator deleteBlob");
          router.coordinator.deleteBlob(blobId);
          logger.trace("Finished coordinator deleteBlob");
          operationTotalTimeTracker = router.metrics.deleteBlobTotalTimeInMs;
          break;
        default:
          throw new IllegalStateException("Unsupported CoordinatorOperationType - " + opType);
      }
    } catch (CoordinatorException e) {
      router.metrics.operationError.inc();
      exception = new RouterException(e, RouterErrorCode.convertCoordinatorErrorToRouterErrorCode(e.getErrorCode()));
    } catch (Exception e) {
      router.metrics.operationError.inc();
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      if (operationTotalTimeTracker != null) {
        operationTotalTimeTracker.update(System.currentTimeMillis() - operationStartTime);
      }
      router.completeOperation(futureResult, callback, operationResult, exception);
      router.metrics.totalOperationsInExecution.decrementAndGet();
    }
  }

  /**
   * Tracks metrics on queuing of operation.
   */
  protected void onQueue() {
    router.metrics.operationQueuingRate.mark();
    operationQueueStartTime = System.currentTimeMillis();
  }

  /**
   * Tracks metrics on dequeuing of operation.
   */
  private void onDequeue() {
    if (operationQueueStartTime != null) {
      long queueTime = System.currentTimeMillis() - operationQueueStartTime;
      router.metrics.operationQueuingTimeInMs.update(queueTime);
      logger.trace("Operation spent {} ms in execution queue", queueTime);
      router.metrics.operationDequeuingRate.mark();
    } else {
      router.metrics.queueStartTimeNotRecordedError.inc();
    }
  }
}
