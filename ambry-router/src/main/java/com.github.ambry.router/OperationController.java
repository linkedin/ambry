package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.RequestResponseHandler;
import com.github.ambry.network.Requestor;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final NonBlockingRouter router;
  private final PutManager putManager;
  private final GetManager getManager;
  private final DeleteManager deleteManager;
  private final ConnectionManager connectionManager;
  private final RequestResponseHandler requestResponseHandler;
  private final RouterConfig routerConfig;
  private final Time time;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private final AtomicBoolean isRunning;
  private static final AtomicLong operationIdGenerator = new AtomicLong(0);
  private static final Logger logger = LoggerFactory.getLogger(OperationController.class);

  public OperationController(NonBlockingRouter router, RouterConfig routerConfig, NetworkConfig networkConfig,
      NetworkMetrics networkMetrics, SSLFactory sslFactory, NotificationSystem notificationSystem,
      ClusterMap clusterMap, Time time)
      throws IOException {
    this.router = router;
    this.routerConfig = routerConfig;
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    this.time = time;
    requestResponseHandler = new RequestResponseHandler(this, networkConfig, networkMetrics, sslFactory, time);
    connectionManager = new ConnectionManager(requestResponseHandler, routerConfig);
    putManager = new PutManager(routerConfig.routerMaxChunkSizeBytes, connectionManager, routerConfig, clusterMap);
    getManager = new GetManager(connectionManager, clusterMap);
    deleteManager = new DeleteManager(connectionManager, clusterMap);
    isRunning = new AtomicBoolean(true);
    requestResponseHandler.start();
  }

  public void getBlobInfo(String blobId, FutureResult<BlobInfo> futureResult, Callback<BlobInfo> callback) {
    getManager.submitGetBlobInfoOperation(operationIdGenerator.incrementAndGet(), blobId, futureResult, callback);
  }

  public void getBlob(String blobId, FutureResult<ReadableStreamChannel> futureResult,
      Callback<ReadableStreamChannel> callback) {
    getManager.submitGetBlobOperation(operationIdGenerator.incrementAndGet(), blobId, futureResult, callback);
  }

  public void putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      FutureResult<String> futureResult, Callback<String> callback) {
    putManager.submitPutBlobOperation(operationIdGenerator.incrementAndGet(), blobProperties, usermetadata, channel,
        futureResult, callback);
  }

  public void deleteBlob(String blobId, FutureResult<Void> futureResult, Callback<Void> callback) {
    deleteManager.submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), blobId, futureResult, callback);
  }

  public void close() {
    if (isRunning.compareAndSet(true, false)) {
      try {
        requestResponseHandler.shutDown();
      } catch (InterruptedException e) {
        logger.error("RequestResponseHandler did not shutdown cleanly");
      } finally {
        router.onOperationControllerClose();
      }
    }
  }

  boolean isRunning() {
    return isRunning.get();
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
        deleteManager
            .submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), id, new FutureResult<Void>(), null);
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
      connectionManager.checkInConnection(conn);
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
  public void onRequestResponseHandlerShutDown(Exception e) {
    if (e != null) {
      logger.error("RequestResponseHandler is shutting down", e);
      // @todo: add a metric.
    } else {
      logger.info("RequestResponseHandler is shutting down");
    }
    close();
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
