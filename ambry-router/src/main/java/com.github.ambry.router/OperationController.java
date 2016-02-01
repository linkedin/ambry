package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.RequestResponseHandler;
import com.github.ambry.network.Requestor;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The various operation types supported by the Operation Controller.
 */
enum OperationType {
  GetBlob,
  GetBlobInfo,
  PutBlob,
  DeleteBlob
}

/**
 *  OperationController class used by the {@link NonBlockingRouter} to submit put, get and delete operations.
 *  This is the scaling unit for the router. OperationController internally uses {@link PutManager},
 *  {@link GetManager}, and {@link DeleteManager} to handle the respective operation.
 */

class OperationController implements Requestor {
  private static final Logger logger = LoggerFactory.getLogger(OperationController.class);
  private final NonBlockingRouter router;
  private final PutManager putManager;
  private final GetManager getManager;
  private final DeleteManager deleteManager;
  private final ConnectionManager connectionManager;
  private final RequestResponseHandler requestResponseHandler;
  private final RouterConfig routerConfig;
  private static final AtomicLong operationIdGenerator = new AtomicLong(0);
  private final Time time;
  private final MetricRegistry registry;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private volatile boolean isRunning;

  public OperationController(NonBlockingRouter router, SSLFactory sslFactory, RouterConfig routerConfig,
      MetricRegistry registry,
      NotificationSystem notificationSystem, ClusterMap clusterMap)
      throws IOException {
    this.router = router;
    this.routerConfig = routerConfig;
    this.registry = registry;
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    time = SystemTime.getInstance();
    requestResponseHandler = new RequestResponseHandler(this, registry, sslFactory, time);
    connectionManager = new ConnectionManager(requestResponseHandler, routerConfig);
    putManager = new PutManager(routerConfig.routerMaxChunkSize, connectionManager, routerConfig, clusterMap);
    getManager = new GetManager(connectionManager, clusterMap);
    deleteManager = new DeleteManager(connectionManager, clusterMap);
    isRunning = true;
    requestResponseHandler.start();
  }

  public void getBlobInfo(String blobId, FutureResult<BlobInfo> futureResult, Callback<BlobInfo> callback) {
    if (isRunning) {
      getManager.submitGetBlobInfoOperation(operationIdGenerator.incrementAndGet(), blobId, futureResult, callback);
    } else {
      router.completeOperation(futureResult, callback, null, NonBlockingRouter.ROUTER_CLOSED_EXCEPTION);
    }
  }

  public void getBlob(String blobId, FutureResult<ReadableStreamChannel> futureResult,
      Callback<ReadableStreamChannel> callback) {
    if (isRunning) {
      getManager.submitGetBlobOperation(operationIdGenerator.incrementAndGet(), blobId, futureResult, callback);
    } else {
      router.completeOperation(futureResult, callback, null, NonBlockingRouter.ROUTER_CLOSED_EXCEPTION);
    }
  }

  public void putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      FutureResult<String> futureResult, Callback<String> callback) {
    if (isRunning) {
      putManager.submitPutBlobOperation(operationIdGenerator.incrementAndGet(), blobProperties, usermetadata, channel,
          callback);
    } else {
      router.completeOperation(futureResult, callback, null, NonBlockingRouter.ROUTER_CLOSED_EXCEPTION);
    }
  }

  public void deleteBlob(String blobId, FutureResult<Void> futureResult, Callback<Void> callback) {
    if (isRunning) {
      deleteManager.submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), blobId, callback);
    } else {
      router.completeOperation(futureResult, callback, null, NonBlockingRouter.ROUTER_CLOSED_EXCEPTION);
    }
  }

  public void close()
      throws IOException {
    if (isRunning) {
      isRunning = false;
      try {
        requestResponseHandler.close();
      } catch (InterruptedException e) {
        throw new IOException("requestResponseHandler did not shutdown cleanly");
      }
    }
  }

  @Override
  public List<NetworkSend> poll() {
    // these are ids that were successfully put for an operation that eventually failed
    List<String> idsToDelete = putManager.getIdsToDelete();

    if (idsToDelete != null) {
      // this is a best effort to delete ids for cleanup purposes (these may fail and we will
      // not do anything about it at this time).
      for (String id : idsToDelete) {
        // possibly add a batch api going forward.
        deleteManager.submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), id, null);
      }
    }

    List<NetworkSend> requests = new ArrayList<NetworkSend>();
    putManager.poll(requests);
    getManager.poll(requests);
    deleteManager.poll(requests);
    return requests;
  }

  @Override
  public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
      List<NetworkReceive> completedReceives) {
    for (String conn : connected) {
      connectionManager.addToAvailablePool(conn);
    }

    for (String conn : disconnected) {
      connectionManager.destroyConnection(conn);
    }

    for (NetworkReceive recv : completedReceives) {
      connectionManager.checkInConnection(recv.getConnectionId());
      handleResponsePayload(recv.getReceivedBytes().getPayload());
    }
  }

  @Override
  public void onClose(Exception e) {
    if (e != null) {
      logger.error("RequestResponseHandler is shutting down", e);
      // @todo: add a metric.
    } else {
      logger.info("RequestResponseHandler is shutting down");
    }
    router.onClose(this);
  }

  private void handleResponsePayload(ByteBuffer response) {
    RequestOrResponseType type = RequestOrResponseType.values()[response.getShort()];
    response.rewind();
    switch (type) {
      case PutResponse:
        putManager.handleResponse(response);
        break;
      case GetResponse:
        getManager.handleResponse(response);
        break;
      case DeleteResponse:
        deleteManager.handleResponse(response);
        break;
      default:
        logger.error("Unexpected response type: " + type + " received, discarding");
        // @todo add to a metric. We do not want to throw here.
    }
  }
}
