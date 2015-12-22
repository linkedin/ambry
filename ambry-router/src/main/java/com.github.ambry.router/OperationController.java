package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.Requestor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Specifies the operation type.
 */
enum OperationType {
  GetBlob,
  GetBlobInfo,
  PutBlob,
  DeleteBlob
}

public class OperationController implements Requestor {
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

  public OperationController() {
    // @todo
    putManager = new PutManager(MAX_CHUNK_SIZE);
    getManager = new GetManager();
    deleteManager = new DeleteManager();
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
    // these are ids that were successfully put for an operation that eventually failed:
    List<String> idsToDelete = putManager.getIdsToDelete();

    // this is a best effort to delete ids for cleanup purposes (these may fail and we will
    // not do anything about it at this time).
    deleteManager.submitDeleteBlob(idsToDelete, null);
    return new ArrayList<NetworkSend>().addAll(putManager.poll(), getManager.poll(), deleteManager.poll());
  }

  // the poll for the respective managers simply iterates over the operations to give them
  // a chance to execute their business logic and get more requests to return.
  @Override
  public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
      List<NetworkReceive> completedReceives) {
    for (String conn : connected) {
      connectionManager.checkIn(conn);
    }

    for (String conn : disconnected) {
      connectionManager.updateDisconnection(conn);
    }

    // update operations in a clean way - benefit being the operation can immediately get
    // ready for the subsequent action (more requests, slip puts, consider the operation as
    // failed, etc.)

    // on receiving a response, call into the respective operation manager -> operation to
    // handle the response
    for (NetworkReceive recv : completedReceives) {
      connectionManager.checkIn(recv.getConnectionId());
      getOperationManagerFromRecv(recv).onResponse(recv);
    }
  }
}
