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
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
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

public class OperationController implements Requestor {
  private static final Logger logger = LoggerFactory.getLogger(OperationController.class);
  private final PutManager putManager;
  private final GetManager getManager;
  private final DeleteManager deleteManager;
  private final ConnectionManager connectionManager;
  private final RequestResponseHandler requestResponseHandler;
  private final RouterConfig routerConfig;
  // @todo make this part of routerConfig; provide reasoning based on the blob size that a metadata chunk can support.
  private static final int MAX_CHUNK_SIZE = 4 * 1024 * 1024;
  private static final AtomicLong operationIdGenerator = new AtomicLong(0);
  Time time;
  MetricRegistry registry;
  ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;

  public OperationController(RouterConfig routerConfig, MetricRegistry registry, NotificationSystem notificationSystem,
      ClusterMap clusterMap)
      throws IOException {
    this.routerConfig = routerConfig;
    this.registry = registry;
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    time = SystemTime.getInstance();
    // @todo: plug in sslFactory.
    requestResponseHandler = new RequestResponseHandler(this, registry, null, time);
    requestResponseHandler.start();
    connectionManager = new ConnectionManager(requestResponseHandler, routerConfig);
    putManager = new PutManager(MAX_CHUNK_SIZE, connectionManager, routerConfig, clusterMap);
    getManager = new GetManager(connectionManager, clusterMap);
    deleteManager = new DeleteManager(connectionManager, clusterMap);
  }

  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    return getManager.submitGetBlobInfoOperation(operationIdGenerator.incrementAndGet(), blobId, callback);
  }

  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    return getManager.submitGetBlobOperation(operationIdGenerator.incrementAndGet(), blobId, callback);
  }

  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    return putManager
        .submitPutBlobOperation(operationIdGenerator.incrementAndGet(), blobProperties, usermetadata, channel,
            callback);
  }

  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    return deleteManager.submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), blobId, callback);
  }

  public void close()
      throws IOException {
    try {
      requestResponseHandler.shutDown();
    } catch (InterruptedException e) {
      throw new IOException("requestResponseHandler did not shutdown cleanly");
    }
  }

  @Override
  public List<NetworkSend> poll() {
    // these are ids that were successfully put for an operation that eventually failed
    List<String> idsToDelete = putManager.getIdsToDelete();

    // this is a best effort to delete ids for cleanup purposes (these may fail and we will
    // not do anything about it at this time).
    for (String id : idsToDelete) {
      // possibly add a batch api going forward.
      deleteManager.submitDeleteBlobOperation(operationIdGenerator.incrementAndGet(), id, null);
    }

    List<NetworkSend> requests = new ArrayList<NetworkSend>();
    putManager.poll(requests);
    getManager.poll(requests);
    deleteManager.poll(requests);
    return requests;
  }

  // the poll for the respective managers simply iterates over the operations to give them
  // a chance to execute their business logic and get more requests to return.
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
  public void onException(Exception e) {
    logger.error("Exception received", e);
    // @todo not much can be done, but figure out.
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
