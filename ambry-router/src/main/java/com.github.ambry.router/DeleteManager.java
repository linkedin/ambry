package com.github.ambry.router;

import com.github.ambry.network.ConnectionManager;
import com.github.ambry.network.NetworkSend;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteManager {
  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);
  private ConcurrentSkipListMap<Long, DeleteOperation> deleteOperations;
  private ConnectionManager connectionManager;

  public DeleteManager(ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  public FutureResult<Void> submitDeleteBlobOperation(long operationId, String blobId, Callback<Void> callback) {
    FutureResult<Void> futureResult = new FutureResult<Void>();
    DeleteOperation deleteOperation =
        new DeleteOperation(operationId, blobId, futureResult, callback);
    deleteOperations.put(operationId, deleteOperation);
    return futureResult;
  }

  public void poll(List<NetworkSend> requests) {
    Iterator<DeleteOperation> iter = deleteOperations.values().iterator();
    while (iter.hasNext()) {
      DeleteOperation op = iter.next();
      op.fetchDeleteRequests(requests);
    }
  }

  class DeleteOperation {
    long operationId;
    String blobId;
    FutureResult<Void> futureResult;
    Callback<Void> callback;
    DeleteOperation(long operationId, String blobId, FutureResult<Void> futureResult, Callback<Void> callback) {
      this.operationId = operationId;
      this.blobId = blobId;
      this.futureResult = futureResult;
      this.callback = callback;
    }

    /**
     * For this operation, populate a list of delete requests (in the form of NetworkSends) to send out.
     * @param requests list to populate with the requests.
     */
    void fetchDeleteRequests(List<NetworkSend> requests) {
      //@todo
    }
  }
}
