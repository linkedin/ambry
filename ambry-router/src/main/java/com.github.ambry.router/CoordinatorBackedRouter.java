package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
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

  private final AtomicBoolean routerOpen = new AtomicBoolean(true);
  private final Coordinator coordinator;
  private final ExecutorService operationPool;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a CoordinatorBackedRouter instance.
   * @param routerConfig the {@link RouterConfig} to refer to.
   * @param metricRegistry the {@link MetricRegistry} to use for metrics.
   * @param coordinator the {@link Coordinator} that will back this router.
   * @throws IllegalArgumentException if any of the arguments received are null or if
   * {@link RouterConfig#routerScalingUnitCount} is less than or equal to 0.
   */
  public CoordinatorBackedRouter(RouterConfig routerConfig, MetricRegistry metricRegistry, Coordinator coordinator) {
    if (routerConfig == null || metricRegistry == null || coordinator == null) {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of CoordinatorBackedRouter -");
      if (routerConfig == null) {
        errorMessage.append(" [RouterConfig] ");
      }
      if (metricRegistry == null) {
        errorMessage.append(" [MetricRegistry] ");
      }
      if (coordinator == null) {
        errorMessage.append(" [Coordinator] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    if (routerConfig.routerScalingUnitCount > 0) {
      this.operationPool = Executors.newFixedThreadPool(routerConfig.routerScalingUnitCount);
    } else {
      throw new IllegalArgumentException(
          "Router scaling unit count defined in config should be > 0 (is " + routerConfig.routerScalingUnitCount + ")");
    }
    this.coordinator = coordinator;
    logger.trace("Instantiated CoordinatorBackedRouter");
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getBlobInfo(blobId, null);
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    logger.trace("Beginning getBlobInfo for {}", blobId);
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    CoordinatorOperation operation =
        new CoordinatorOperation(coordinator, futureResult, blobId, callback, CoordinatorOperationType.GetBlobInfo);
    submitOperation(operation, futureResult, callback);
    return futureResult;
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    return getBlob(blobId, null);
  }

  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    logger.trace("Beginning getBlob for {}", blobId);
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    CoordinatorOperation operation =
        new CoordinatorOperation(coordinator, futureResult, blobId, callback, CoordinatorOperationType.GetBlob);
    submitOperation(operation, futureResult, callback);
    return futureResult;
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel) {
    return putBlob(blobProperties, usermetadata, channel, null);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    logger.trace("Beginning putBlob");
    FutureResult<String> futureResult = new FutureResult<String>();
    CoordinatorOperation operation =
        new CoordinatorOperation(coordinator, futureResult, blobProperties, usermetadata, channel, callback);
    submitOperation(operation, futureResult, callback);
    return futureResult;
  }

  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    logger.trace("Beginning deleteBlob for {}", blobId);
    FutureResult<Void> futureResult = new FutureResult<Void>();
    CoordinatorOperation operation =
        new CoordinatorOperation(coordinator, futureResult, blobId, callback, CoordinatorOperationType.DeleteBlob);
    submitOperation(operation, futureResult, callback);
    return futureResult;
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (routerOpen.compareAndSet(true, false)) {
        logger.info("CoordinatorBackedRouter closing");
        operationPool.shutdown();
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
        coordinator.close();
        logger.info("CoordinatorBackedRouter closed");
      } else {
        operationPool.awaitTermination(1, TimeUnit.MINUTES);
      }
    } catch (InterruptedException e) {
      // TODO: metrics.
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
        operationPool.submit(coordinatorOperation);
      } catch (Exception e) {
        completeOperation(futureResult, callback, null, e);
      }
    } else {
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
  protected static void completeOperation(FutureResult futureResult, Callback callback, Object operationResult,
      Exception exception) {
    RuntimeException runtimeException = null;
    if (exception != null) {
      runtimeException = new RuntimeException(exception);
    }
    futureResult.done(operationResult, runtimeException);
    if (callback != null) {
      callback.onCompletion(operationResult, exception);
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

  // general
  private final Coordinator coordinator;
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
   * @param coordinator the {@link Coordinator} to use to perform the operation.
   * @param futureResult the {@link FutureResult} where the final result has to be loaded.
   * @param blobId the blob id that the operation needs to be performed on.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   * @param opType the {@link CoordinatorOperationType} required. Can only be one of
   *                {@link CoordinatorOperationType#GetBlob}, {@link CoordinatorOperationType#GetBlobInfo} or
   *                {@link CoordinatorOperationType#DeleteBlob}.
   * @throws IllegalArgumentException if {@code opType} is {@link CoordinatorOperationType#PutBlob}.
   */
  public CoordinatorOperation(Coordinator coordinator, FutureResult futureResult, String blobId, Callback callback,
      CoordinatorOperationType opType) {
    this(coordinator, futureResult, callback, opType);
    if (CoordinatorOperationType.PutBlob.equals(opType)) {
      throw new IllegalArgumentException("This constructor cannot be used for the putBlob operation");
    }
    this.blobId = blobId;
  }

  /**
   * Constructor used to invoke {@link Coordinator} equivalent operations for
   * {@link Router#putBlob(BlobProperties, byte[], ReadableStreamChannel)} and its variant.
   * @param coordinator the {@link Coordinator} to use to perform the operation.
   * @param futureResult the {@link FutureResult} where the final result has to be loaded.
   * @param blobProperties the properties of the blob.
   * @param usermetadata user specified metadata as a byte array.
   * @param channel the {@link ReadableStreamChannel} to read the blob data from.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   */
  public CoordinatorOperation(Coordinator coordinator, FutureResult futureResult, BlobProperties blobProperties,
      byte[] usermetadata, ReadableStreamChannel channel, Callback callback) {
    this(coordinator, futureResult, callback, CoordinatorOperationType.PutBlob);
    this.blobProperties = blobProperties;
    this.usermetadata = usermetadata;
    this.channel = channel;
  }

  private CoordinatorOperation(Coordinator coordinator, FutureResult futureResult, Callback callback,
      CoordinatorOperationType opType) {
    this.coordinator = coordinator;
    this.futureResult = futureResult;
    this.callback = callback;
    this.opType = opType;
    logger.trace("Instantiated CoordinatorOperation for {} operation", this.opType);
  }

  @Override
  public void run() {
    Object operationResult = null;
    Exception exception = null;
    try {
      switch (opType) {
        case GetBlob:
          logger.trace("Beginning coordinator getBlob");
          BlobOutput blobOutput = coordinator.getBlob(blobId);
          // (int) blobOutput.getSize() will not work for blobs >2GB in size but that is not a concern right now.
          // CoordinatorBackedRouter will be long gone before (if) we support blobs with that size.
          logger.trace("Finished coordinator getBlob");
          byte[] buf = Utils.readBytesFromStream(blobOutput.getStream(), (int) blobOutput.getSize());
          logger.trace("Blob data has completely arrived");
          operationResult = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(buf));
          break;
        case GetBlobInfo:
          logger.trace("Beginning coordinator getBlobProperties");
          blobProperties = coordinator.getBlobProperties(blobId);
          logger.trace("Finished coordinator getBlobProperties. Beginning coordinator getBlobUserMetadata");
          ByteBuffer usermetadataBuffer = coordinator.getBlobUserMetadata(blobId);
          logger.trace("Finished coordinator getBlobUserMetadata.");
          if (usermetadataBuffer.hasArray()) {
            usermetadata = usermetadataBuffer.array();
          } else {
            logger.trace("Usermetadata buffer was not backed by an array. Copying the data");
            usermetadata = new byte[usermetadataBuffer.capacity()];
            usermetadataBuffer.get(usermetadata);
          }
          operationResult = new BlobInfo(blobProperties, usermetadata);
          break;
        case PutBlob:
          logger.trace("Beginning coordinator putBlob");
          operationResult = coordinator
              .putBlob(blobProperties, ByteBuffer.wrap(usermetadata), new ReadableStreamChannelInputStream(channel));
          logger.trace("Finished coordinator putBlob");
          break;
        case DeleteBlob:
          logger.trace("Beginning coordinator deleteBlob");
          coordinator.deleteBlob(blobId);
          logger.trace("Finished coordinator deleteBlob");
          break;
        default:
          throw new IllegalStateException("Unsupported CoordinatorOperationType - " + opType);
      }
    } catch (CoordinatorException e) {
      exception = new RouterException(e, RouterErrorCode.convertCoordinatorErrorToRouterErrorCode(e.getErrorCode()));
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      CoordinatorBackedRouter.completeOperation(futureResult, callback, operationResult, exception);
    }
  }
}
