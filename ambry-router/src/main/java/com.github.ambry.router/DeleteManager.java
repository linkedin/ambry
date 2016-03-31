package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DeleteManager that handles {@link DeleteOperation}. A {@code DeleteManager} keeps track of all the {@link
 * DeleteOperation} that are assigned to it, and manages their life cycles.
 */
class DeleteManager {
  private final Set<DeleteOperation> deleteOperations;
  private final HashMap<Integer, DeleteOperation> correlationIdToDeleteOperation;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;

  final ClusterMap clusterMap;
  final RouterConfig routerConfig;

  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  /**
   * Initialize a {@code DeleteManager}.
   */
  public DeleteManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    deleteOperations = new HashSet<DeleteOperation>();
    correlationIdToDeleteOperation = new HashMap<Integer, DeleteOperation>();
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
    try {
      BlobId blobId = RouterUtils.getBlobIdFromString(blobIdString, clusterMap);
      DeleteOperation deleteOperation =
          new DeleteOperation(routerConfig, operationId, blobId, clusterMap, futureResult, callback,
              new DeleteRequestRegistrationCallback(), time);
      deleteOperations.add(deleteOperation);
    } catch (RouterException e) {
      NonBlockingRouter.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Poll among all {@link DeleteOperation}, and generate request for each of them.
   * @param requestInfos the list of {@link RequestInfo} to fill.
   */
  public void poll(List<RequestInfo> requestInfos) {
    Iterator<DeleteOperation> iter = deleteOperations.iterator();
    while (iter.hasNext()) {
      DeleteOperation deleteOperation = iter.next();
      deleteOperation.fetchRequest(requestInfos);
      if(deleteOperation.isOperationCompleted()){
        onOperationComplete(deleteOperation);
      }
    }
  }

  /**
   * Handle responses received for each of the {@link DeleteOperation} within this manager.
   * @param responseInfo A response from {@link com.github.ambry.network.NetworkClient}
   */
  void handleResponse(ResponseInfo responseInfo) {
    DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequest();
    DeleteOperation deleteOperation = correlationIdToDeleteOperation.remove(deleteRequest.getCorrelationId());
    if (deleteOperations.contains(deleteOperation)) {
      deleteOperation.handleResponse(responseInfo);
    } else {
      // This will happen if the DeleteOperation has already been finished by getting enough responses
      // or failed by some ServerError.
      logger.trace("Received response for a request of an operation that has completed.");
    }
  }

  /**
   * Complete a {@link DeleteOperation}.
   * @param deleteOperation The {@lilnk DeleteOperation} to be completed.
   */
  void onOperationComplete(DeleteOperation deleteOperation) {
    if (deleteOperation.getOperationException() == null) {
      notificationSystem.onBlobDeleted(deleteOperation.getBlobId().getID());
    }
    NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(),
        deleteOperation.getOperationResult(), deleteOperation.getOperationException());
    deleteOperations.remove(deleteOperation);
  }

  /**
   * Close the {@code DeleteManager}.
   */
  void close() {
    if (isOpen.compareAndSet(true, false)) {
      Iterator<DeleteOperation> iter = deleteOperations.iterator();
      while (iter.hasNext()) {
        DeleteOperation deleteOperation = iter.next();
        NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(), null,
            new RouterException("Cannot process operation because Router is closed.", RouterErrorCode.RouterClosed));
        iter.remove();
      }
    }
  }

  /**
   * Used by a {@link DeleteOperation} to associate a {@code oCorrelationId} to a {@link DeleteOperation}.
   */
  private class DeleteRequestRegistrationCallback implements RequestRegistrationCallback {
    @Override
    public void registerRequestToSend(DeleteOperation deleteOperation, RequestInfo requestInfo) {
      correlationIdToDeleteOperation
          .put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), deleteOperation);
    }
  }
}
