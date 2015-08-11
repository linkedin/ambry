package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
 * The CoordinatorBackedRouter allocates a thread pool of size {@link RouterConfig#routerOperationPoolSize} on
 * instantiation. This thread pool is used to provide non-blocking behaviour by executing the blocking operations of the
 * {@link Coordinator} in the background.
 * <p/>
 * If the thread pool is exhausted, the CoordinatorBackedRouter remains non-blocking but latency of operations will
 * spike.
 */
public class CoordinatorBackedRouter implements Router {
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final Coordinator coordinator;
  private final ExecutorService operationPool;
  private final RouterConfig routerConfig;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a CoordinatorBackedRouter instance.
   * @param verifiableProperties the properties map to refer to.
   * @param metricRegistry the {@link MetricRegistry} to use for metrics.
   * @param coordinator the {@link Coordinator} that will back this router.
   * @throws IllegalArgumentException if any of the arguments received are null or if
   * {@link RouterConfig#routerOperationPoolSize} is less than or equal to 0.
   */
  public CoordinatorBackedRouter(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Coordinator coordinator) {
    if (verifiableProperties == null || metricRegistry == null || coordinator == null) {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of CoordinatorBackedRouter -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (metricRegistry == null) {
        errorMessage.append(" [MetricRegistry] ");
      }
      if (coordinator == null) {
        errorMessage.append(" [Coordinator] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    this.routerConfig = new RouterConfig(verifiableProperties);
    if (routerConfig.routerOperationPoolSize > 0) {
      this.operationPool = Executors.newFixedThreadPool(routerConfig.routerOperationPoolSize);
    } else {
      throw new IllegalArgumentException(
          "Router operation pool size defined in config should be > 0 (is " + routerConfig.routerOperationPoolSize
              + ")");
    }
    this.coordinator = coordinator;
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getBlobInfo(blobId, null);
  }

  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    FutureRouterResult<BlobInfo> futureRouterResult = new FutureRouterResult<BlobInfo>();
    operationPool.submit(new CoordinatorOperation(coordinator, futureRouterResult, blobId, callback,
        CoordinatorOperationType.GetBlobInfo));
    return futureRouterResult;
  }

  @Override
  public Future<BlobOutput> getBlob(String blobId) {
    return getBlob(blobId, null);
  }

  @Override
  public Future<BlobOutput> getBlob(String blobId, Callback<BlobOutput> callback) {
    FutureRouterResult<BlobOutput> futureRouterResult = new FutureRouterResult<BlobOutput>();
    operationPool.submit(
        new CoordinatorOperation(coordinator, futureRouterResult, blobId, callback, CoordinatorOperationType.GetBlob));
    return futureRouterResult;
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel) {
    return putBlob(blobProperties, usermetadata, channel, null);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel,
      Callback<String> callback) {
    FutureRouterResult<String> futureRouterResult = new FutureRouterResult<String>();
    operationPool.submit(
        new CoordinatorOperation(coordinator, futureRouterResult, blobProperties, usermetadata, channel, callback));
    return futureRouterResult;
  }

  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    FutureRouterResult<Void> futureRouterResult = new FutureRouterResult<Void>();
    operationPool.submit(new CoordinatorOperation(coordinator, futureRouterResult, blobId, callback,
        CoordinatorOperationType.DeleteBlob));
    return futureRouterResult;
  }

  @Override
  public void close() {
    if (!shuttingDown.getAndSet(true)) {
      logger.info("CoordinatorBackedRouter closing");
      if (!operationPool.isShutdown()) {
        try {
          operationPool.shutdown();
          operationPool.awaitTermination(1, TimeUnit.MINUTES);
          coordinator.close();
          logger.info("CoordinatorBackedRouter closed");
        } catch (IOException e) {
          logger.error("Error closing Coordinator in CoordinatorBackedRouter", e);
        } catch (Exception e) {
          logger.error("Error shutting down operationPool in CoordinatorBackedRouter", e);
        }
      }
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
  // general
  private final Coordinator coordinator;
  private final FutureRouterResult futureRouterResult;
  private final Callback callback;
  private final CoordinatorOperationType opType;

  // getBlob, getBlobInfo and delete
  private String blobId = null;

  // put
  private BlobProperties blobProperties = null;
  private byte[] usermetadata = null;
  private ReadableByteChannel channel = null;

  /**
   * Constructor used to invoke {@link Coordinator} equivalent operations for {@link Router#getBlob(String)},
   * {@link Router#getBlobInfo(String)} and {@link Router#deleteBlob(String)} (and their variants).
   * @param coordinator the {@link Coordinator} to use to perform the operation.
   * @param futureRouterResult the {@link FutureRouterResult} where the final result has to be loaded.
   * @param blobId the blob id that the operation needs to be performed on.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   * @param opType the {@link CoordinatorOperationType} required. Can only be one of
   *                {@link CoordinatorOperationType#GetBlob}, {@link CoordinatorOperationType#GetBlobInfo} or
   *                {@link CoordinatorOperationType#DeleteBlob}.
   * @throws IllegalArgumentException if {@code opType} is {@link CoordinatorOperationType#PutBlob}.
   */
  public CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult, String blobId,
      Callback callback, CoordinatorOperationType opType) {
    this(coordinator, futureRouterResult, callback, opType);
    if (CoordinatorOperationType.PutBlob.equals(opType)) {
      throw new IllegalArgumentException("This constructor cannot be used for the putBlob operation");
    }
    this.blobId = blobId;
  }

  /**
   * Constructor used to invoke {@link Coordinator} equivalent operations for
   * {@link Router#putBlob(BlobProperties, byte[], ReadableByteChannel)} and its variant.
   * @param coordinator the {@link Coordinator} to use to perform the operation.
   * @param futureRouterResult the {@link FutureRouterResult} where the final result has to be loaded.
   * @param blobProperties the properties of the blob.
   * @param usermetadata user specified metadata as a byte array.
   * @param channel the {@link ReadableByteChannel} to read the blob data from.
   * @param callback the {@link Callback} to invoke once operation is complete (can be null if no callback required).
   */
  public CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult,
      BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel, Callback callback) {
    this(coordinator, futureRouterResult, callback, CoordinatorOperationType.PutBlob);
    this.blobProperties = blobProperties;
    this.usermetadata = usermetadata;
    this.channel = channel;
  }

  private CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult, Callback callback,
      CoordinatorOperationType opType) {
    this.coordinator = coordinator;
    this.futureRouterResult = futureRouterResult;
    this.callback = callback;
    this.opType = opType;
  }

  @Override
  public void run() {
    Object operationResult = null;
    Exception exception = null;
    try {
      switch (opType) {
        case GetBlob:
          operationResult = coordinator.getBlob(blobId).open();
          break;
        case GetBlobInfo:
          BlobProperties rcvdBlobProperties = coordinator.getBlobProperties(blobId);
          ByteBuffer rcvdUserMetadata = coordinator.getBlobUserMetadata(blobId);
          byte[] rcvdUserMetadataArray;
          if (rcvdUserMetadata.hasArray()) {
            rcvdUserMetadataArray = rcvdUserMetadata.array();
          } else {
            rcvdUserMetadataArray = new byte[rcvdUserMetadata.capacity()];
            rcvdUserMetadata.get(rcvdUserMetadataArray);
          }
          operationResult = new BlobInfo(rcvdBlobProperties, rcvdUserMetadataArray);
          break;
        case PutBlob:
          // TODO: Might need rethinking. Will probably have to provide custom implementation of InputStream here.
          // TODO: because it seems like Channels.newInputStream() does not like non-blocking channels. Will do this
          // TODO: while implementing POST.
          operationResult =
              coordinator.putBlob(blobProperties, ByteBuffer.wrap(usermetadata), Channels.newInputStream(channel));
          break;
        case DeleteBlob:
          coordinator.deleteBlob(blobId);
          break;
      }
    } catch (CoordinatorException e) {
      exception = new RouterException(e, RouterErrorCode.convertCoordinatorErrorToRouterErrorCode(e.getErrorCode()));
    } catch (Exception e) {
      exception = new RouterException(e, RouterErrorCode.UnexpectedInternalError);
    } finally {
      RuntimeException runtimeException = null;
      if (exception != null) {
        runtimeException = new RuntimeException(exception);
      }
      futureRouterResult.done(operationResult, runtimeException);
      if (callback != null) {
        callback.onCompletion(operationResult, exception);
      }
    }
  }
}
