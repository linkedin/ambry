package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutManager class is responsible for handling putBlob operations. PutManager creates a {@link PutOperation} for each
 * operation submitted to it, and tracks them.
 */
class PutManager {
  private static final Logger logger = LoggerFactory.getLogger(PutManager.class);

  private final Set<PutOperation> putOperations;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final Thread chunkFillerThread;
  // This helps the PutManager quickly find the appropriate PutOperation to hand over the response to.
  // Requests are added before they are sent out and get cleaned up as and when responses come in.
  // Because there is a guaranteed response from the NetworkClient for every request sent out, entries
  // get cleaned up periodically.
  private final Map<Integer, PutOperation> correlationIdToPutOperation;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  // shared by all PutOperations
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;

  private final class PutRequestRegistrationCallbackImpl implements PutRequestRegistrationCallback {
    private List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(PutOperation putOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToPutOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), putOperation);
    }
  }

  ;
  // A single callback as this will never get called concurrently. The list of request to fill will be set as
  // appropriate before the callback is passed on to the PutOperations, every time.
  private final PutRequestRegistrationCallbackImpl requestRegistrationCallback =
      new PutRequestRegistrationCallbackImpl();

  /**
   * Create a PutManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob creations.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param time The {@link Time} instance to use.
   */
  PutManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    putOperations = Collections.newSetFromMap(new ConcurrentHashMap<PutOperation, Boolean>());
    correlationIdToPutOperation = new HashMap<Integer, PutOperation>();
    chunkFillerThread = Utils.newThread("ChunkFillerThread", new ChunkFiller(), false);
    chunkFillerThread.start();
  }

  /**
   * Submit a put blob operation to be processed asynchronously.
   * @param operationId the operation id for the put operation.
   * @param blobProperties the blobProperties associated with the blob being put.
   * @param userMetaData the userMetaData associated with the blob being put.
   * @param channel the {@link ReadableStreamChannel} containing the blob content.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitPutBlobOperation(long operationId, BlobProperties blobProperties, byte[] userMetaData,
      final ReadableStreamChannel channel, FutureResult<String> futureResult, Callback<String> callback) {
    try {
      PutOperation putOperation =
          new PutOperation(routerConfig, clusterMap, responseHandler, operationId, blobProperties, userMetaData,
              channel, futureResult, callback, time);
      putOperations.add(putOperation);
    } catch (RouterException e) {
      logger.error("Error creating PutOperation with the given operation parameters", e);
      NonBlockingRouter.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to data nodes in order to
   * complete put operations. Since this is the only method guaranteed to be called periodically by the
   * RequestResponseHandler thread in the {@link NonBlockingRouter} ({@link #handleResponse} gets called only if a
   * response is received for a put operation), any error handling or operation completion and cleanup also usually
   * gets done in the context of this method.
   * @param requestListToFill list to be filled with the requests created
   * @throws RouterException if an error is encountered during the creation of new requests.
   */
  void poll(List<RequestInfo> requestListToFill) {
    Iterator<PutOperation> putOperationIterator = putOperations.iterator();
    requestRegistrationCallback.requestListToFill = requestListToFill;
    while (putOperationIterator.hasNext()) {
      PutOperation op = putOperationIterator.next();
      if (op.poll(requestRegistrationCallback)) {
        // Operation is done.
        putOperationIterator.remove();
        onComplete(op);
      }
    }
  }

  /**
   * Hands over the response to the associated PutChunk that issued the request.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    int correlationId = ((PutRequest) responseInfo.getRequest()).getCorrelationId();
    // Get the PutOperation that generated the request.
    PutOperation putOperation = correlationIdToPutOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (putOperations.contains(putOperation)) {
      if (putOperation.handleResponse(responseInfo)) {
        putOperations.remove(putOperation);
        onComplete(putOperation);
      }
    } else {
      // Ignore. the operation has completed.
    }
  }

  /**
   * Returns a list of ids of successfully put chunks that were part of unsuccessful put operations.
   * @return list of ids to delete.
   */
  void getIdsToDelete(List<String> idsToDelete) {
    // @todo save and return ids of failed puts.
  }

  /**
   * Called by a {@link PutOperation} when the operation is complete. Any cleanup that the PutManager needs to do
   * with respect to this operation will have to be done here. The PutManager also finishes the operation by
   * performing the callback and notification.
   * @param op the {@link PutOperation} that has completed.
   */
  void onComplete(PutOperation op) {
    if (op.getOperationException() != null) {
      // @todo add blobs in the metadata chunk to ids_to_delete
    } else {
      notificationSystem.onBlobCreated(op.getBlobIdString(), op.getBlobProperties(), op.getUserMetadata());
    }
    NonBlockingRouter
        .completeOperation(op.getFuture(), op.getCallback(), op.getBlobIdString(), op.getOperationException());
  }

  /**
   * Close the PutManager.
   */
  void close() {
    isOpen.set(false);
    try {
      chunkFillerThread.join(NonBlockingRouter.SHUTDOWN_WAIT_MS);
    } catch (InterruptedException e) {
      logger.error("Caught interrupted exception while waiting for chunkFillerThread to finish");
      Thread.currentThread().interrupt();
    }
    Iterator<PutOperation> iter = putOperations.iterator();
    while (iter.hasNext()) {
      PutOperation op = iter.next();
      NonBlockingRouter.completeOperation(op.getFuture(), op.getCallback(), null,
          new RouterException("Aborted operation because Router is closed", RouterErrorCode.RouterClosed));
      iter.remove();
    }
  }

  /**
   * the ChunkFiller thread continuously iterates over all the putOperations submitted, reads from the {@link
   * AsyncWritableChannel} associated with the operation, and fills in chunks. The channel will be populated
   * by the {@link ReadableStreamChannel} associated with the operation.
   */
  private class ChunkFiller implements Runnable {
    private final int sleepTimeWhenIdleMs = 10;

    public void run() {
      while (isOpen.get()) {
        boolean allChunksFillComplete = true;
        Iterator<PutOperation> iter = putOperations.iterator();
        while (iter.hasNext()) {
          PutOperation op = iter.next();
          if (!op.fillChunks()) {
            allChunksFillComplete = false;
          }
        }
        if (allChunksFillComplete) {
          try {
            Thread.sleep(sleepTimeWhenIdleMs);
          } catch (InterruptedException e) {
            logger.info("Caught interrupted exception while sleeping", e);
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }
}
