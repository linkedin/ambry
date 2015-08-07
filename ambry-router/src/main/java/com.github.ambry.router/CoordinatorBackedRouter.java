package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
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
 * TODO: write description
 */
public class CoordinatorBackedRouter implements Router {
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final Coordinator coordinator;
  private final ExecutorService operationPool;
  private final RouterConfig routerConfig;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public CoordinatorBackedRouter(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      Coordinator coordinator) {
    if (verifiableProperties == null || clusterMap == null || coordinator == null) {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of CoordinatorBackedRouter -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (clusterMap == null) {
        errorMessage.append(" [ClusterMap] ");
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

enum CoordinatorOperationType {
  GetBlob,
  GetBlobInfo,
  PutBlob,
  DeleteBlob
}

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

  public CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult, Callback callback,
      CoordinatorOperationType opType) {
    this.coordinator = coordinator;
    this.futureRouterResult = futureRouterResult;
    this.callback = callback;
    this.opType = opType;
  }

  public CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult, String blobId,
      Callback callback, CoordinatorOperationType opType) {
    this(coordinator, futureRouterResult, callback, opType);
    this.blobId = blobId;
  }

  public CoordinatorOperation(Coordinator coordinator, FutureRouterResult futureRouterResult,
      BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel, Callback callback) {
    this(coordinator, futureRouterResult, callback, CoordinatorOperationType.PutBlob);
    this.blobProperties = blobProperties;
    this.usermetadata = usermetadata;
    this.channel = channel;
  }

  @Override
  public void run() {
    Object operationResult = null;
    Exception exception = null;
    try {
      switch (opType) {
        case GetBlob:
          operationResult = coordinator.getBlob(blobId).openForReading();
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
          // TODO: might need rethinking. Will probably have to provide custom implementation of InputStream here.
          // TODO: because it seems like Channels.newInputStream() does not like non-blocking channels
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
