package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DeleteManager that handles {@link DeleteOperation}. A {@code DeleteManager} keeps track of all the {@link
 * DeleteOperation} that are assigned to it, and manages their life cycles.
 */
class DeleteManager {
  private final HashMap<Long, DeleteOperation> operationIdTodeleteOperation;
  private final HashMap<DeleteRequest, Long> requestToOperationId;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  // Shared by all DeleteOperations
  final ClusterMap clusterMap;
  final RouterConfig routerConfig;

  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  /**
   * Initialize a {@code DeleteManager}.
   */
  public DeleteManager(RouterConfig routerConfig, ClusterMap clusterMap, NotificationSystem notificationSystem,
      Time time) {
    this.clusterMap = clusterMap;
    this.routerConfig = routerConfig;
    this.notificationSystem = notificationSystem;
    this.time = time;
    operationIdTodeleteOperation = new HashMap<Long, DeleteOperation>();
    requestToOperationId = new HashMap<DeleteRequest, Long>();
  }

  /**
   * Submit a {@link DeleteOperation} to this {@code DeleteManager}.
   * @param operationId A unique id that is associated with the {@code DeleteOperation} to be generated.
   * @param blobIdString The blobId string to be deleted.
   * @param futureResult The {@link java.util.concurrent.Future} that was returned to the caller.
   * @param callback The {@link Callback} that was supplied by the caller.
   */
  void submitDeleteBlobOperation(long operationId, String blobIdString, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    BlobId blobId = null;
    Exception exception = null;
    try {
      blobId = getBlobIdFromString(blobIdString);
    } catch (RouterException e) {
      exception = e;
    }
    if (exception == null) {
      final DeleteOperation deleteOperation =
          new DeleteOperation(this, operationId, blobId, clusterMap, futureResult, callback, time);
      operationIdTodeleteOperation.put(operationId, deleteOperation);
    } else {
      NonBlockingRouter.completeOperation(futureResult, callback, null, exception);
    }
  }

  /**
   * Poll among all {@link DeleteOperation}, and generate request for each of them.
   * @param requestInfos the list of {@link RequestInfo} to fill.
   */
  public void poll(List<RequestInfo> requestInfos) {
    final Iterator<DeleteOperation> iter = operationIdTodeleteOperation.values().iterator();
    while (iter.hasNext()) {
      DeleteOperation deleteOperation = iter.next();
      deleteOperation.fetchRequest(requestInfos);
    }
  }

  /**
   * Handle responses received for each of the {@link DeleteOperation} within this manager.
   * @param responseInfo A response from {@link com.github.ambry.network.NetworkClient}
   */
  void handleResponse(ResponseInfo responseInfo) {
    final DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequest();
    final long operationId = requestToOperationId.get(deleteRequest);
    final DeleteOperation deleteOperation = operationIdTodeleteOperation.get(operationId);
    if (deleteOperation != null) {
      deleteOperation.handleResponse(responseInfo);
    } else {
      // This will happen if the DeleteOperation has already been finished by getting enough responses
      // or failed by some ServerError.
      logger.trace("Received response for a request of an operation that has completed.");
    }
  }

  /**
   * Complete a {@link DeleteOperation}.
   * @param operationId The id for the {@link DeleteOperation} to be completed.
   */
  void onOperationComplete(long operationId) {
    final DeleteOperation deleteOperation = operationIdTodeleteOperation.remove(operationId);
    if (deleteOperation == null) {
      logger.trace("Delete operation has already been completed at previous response.");
    } else {
      if (deleteOperation.getOperationException() == null) {
        notificationSystem.onBlobDeleted(deleteOperation.getBlobId().getID());
      }
      NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(),
          deleteOperation.getOperationResult(), deleteOperation.getOperationException());
    }
  }

  /**
   * Used by a {@link DeleteOperation} to associate a {@link DeleteRequest} to a {@code operationId}.
   */
  void addRequestOperationId(DeleteRequest deleteRequest, Long operationId) {
    if (requestToOperationId.containsKey(deleteRequest)) {
      logger.trace("A DeleteRequest-OperationId pair has been previously associated.");
      // currently do nothing. Should we fail in this case?
      // throw new IllegalStateException("This deleteRequest-operationId pair has been added before");
    } else {
      requestToOperationId.put(deleteRequest, operationId);
    }
  }

  /**
   * Get {@link BlobId} from a blob string.
   * @param blobIdString The string of blobId.
   * @return BlobId
   * @throws RouterException If parsing a string blobId fails.
   */
  private BlobId getBlobIdFromString(String blobIdString)
      throws RouterException {
    if (blobIdString == null || blobIdString.length() == 0) {
      logger.error("BlobIdString argument is null or zero length: {}", blobIdString);
      throw new RouterException("BlobId is empty.", RouterErrorCode.InvalidBlobId);
    }

    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
      logger.trace("BlobId created " + blobId + " with partition " + blobId.getPartition());
    } catch (Exception e) {
      logger.error("Caller passed in invalid BlobId " + blobIdString);
      throw new RouterException("BlobId is invalid " + blobIdString, RouterErrorCode.InvalidBlobId);
    }
    return blobId;
  }

  /**
   * Close the {@code DeleteManager}.
   */
  void close() {
    isOpen.set(false);
    Iterator<DeleteOperation> iter = operationIdTodeleteOperation.values().iterator();
    while (iter.hasNext()) {
      DeleteOperation deleteOperation = iter.next();
      NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(), null,
          new RouterException("Cannot process operation because Router is closed.", RouterErrorCode.RouterClosed));
      iter.remove();
    }
  }
}
