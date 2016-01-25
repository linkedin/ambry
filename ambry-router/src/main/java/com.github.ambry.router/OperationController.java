package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.ConnectionManager;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.Requestor;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
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
  // @todo make this part of routerConfig, and provide a reasoning (based on how large a blob we can support
  // @todo with a single metadata chunk.
  private static final int MAX_CHUNK_SIZE = 4 * 1024 * 1024;
  private static final AtomicLong getBlobOperationIdGenerator = new AtomicLong(OperationType.GetBlob.ordinal());
  private static final AtomicLong getBlobInfoOperationIdGenerator = new AtomicLong(OperationType.GetBlobInfo.ordinal());
  private static final AtomicLong putBlobOperationIdGenerator = new AtomicLong(OperationType.PutBlob.ordinal());
  private static final AtomicLong deleteBlobOperationIdGenerator = new AtomicLong(OperationType.DeleteBlob.ordinal());
  private static final int operationTypeCount = OperationType.values().length;
  Time time;

  public OperationController()
      throws IOException {
    // @todo: network metrics, sslFactory...
    Time time = SystemTime.getInstance();
    connectionManager = new ConnectionManager(time, null);
    putManager = new PutManager(MAX_CHUNK_SIZE, connectionManager);
    getManager = new GetManager(connectionManager);
    deleteManager = new DeleteManager(connectionManager);
  }

  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    return getManager
        .submitGetBlobInfoOperation(getBlobInfoOperationIdGenerator.addAndGet(operationTypeCount), blobId, callback);
  }

  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    return getManager
        .submitGetBlobOperation(getBlobOperationIdGenerator.addAndGet(operationTypeCount), blobId, callback);
  }

  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    return putManager
        .submitPutBlobOperation(putBlobOperationIdGenerator.addAndGet(operationTypeCount), blobProperties, usermetadata,
            channel, callback);
  }

  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    return deleteManager
        .submitDeleteBlobOperation(deleteBlobOperationIdGenerator.addAndGet(operationTypeCount), blobId, callback);
  }

  public void close()
      throws IOException {
    // @todo: no-op for now.
  }

  @Override
  public List<NetworkSend> poll() {
    // these are ids that were successfully put for an operation that eventually failed
    List<String> idsToDelete = putManager.getIdsToDelete();

    // this is a best effort to delete ids for cleanup purposes (these may fail and we will
    // not do anything about it at this time).
    for (String id : idsToDelete) {
      // add batch api going forward.
      deleteManager.submitDeleteBlobOperation(deleteBlobOperationIdGenerator.addAndGet(operationTypeCount), id, null);
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
      connectionManager.removeFromAvailablePool(conn);
    }

    // update operations in a clean way - benefit being the operation can immediately get
    // ready for the subsequent action (more requests, slip puts, consider the operation as
    // failed, etc.)

    // on receiving a response, call into the respective operation manager -> operation to
    // handle the response. We get this from the correlation id.
    for (NetworkReceive recv : completedReceives) {
      connectionManager.addToAvailablePool(recv.getConnectionId());
      deserializeResponse(recv.getReceivedBytes());
    }
  }

  @Override
  public void onException(Exception e) {
    logger.error("Exception received", e);
    // @todo
  }

  private void deserializeResponse(BoundedByteBufferReceive recv) {
    // @todo
    // 1. Get the appropriate
  }
}
