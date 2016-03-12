package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutManager class is responsible for handling putBlob operations. PutManager creates a {@link PutOperation} for each
 * operation submitted to it, and tracks them.
 */
class PutManager {
  private final ConcurrentSkipListMap<Long, PutOperation> putOperations;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final Thread chunkFillerThread;
  private final ConcurrentSkipListMap<Integer, PutOperation.PutChunk> correlationIdToPutChunk;

  final int maxChunkSize;
  final ClusterMap clusterMap;
  final RouterConfig routerConfig;

  private List<BlobId> idsToDelete;

  private static final Logger logger = LoggerFactory.getLogger(PutManager.class);

  /**
   * @param routerConfig the configs for the router.
   * @param clusterMap the {@link ClusterMap} for the cluster.
   * @param notificationSystem the {@link NotificationSystem} system to use to notify blob creations and deletions.
   * @param time the Time instance to use.
   */
  PutManager(RouterConfig routerConfig, ClusterMap clusterMap, NotificationSystem notificationSystem, Time time) {
    this.clusterMap = clusterMap;
    this.routerConfig = routerConfig;
    this.notificationSystem = notificationSystem;
    this.time = time;
    this.maxChunkSize = routerConfig.routerMaxPutChunkSizeBytes;
    this.putOperations = new ConcurrentSkipListMap<Long, PutOperation>();
    this.correlationIdToPutChunk = new ConcurrentSkipListMap<Integer, PutOperation.PutChunk>();
    chunkFillerThread = Utils.newThread("ChunkFiller thread", new ChunkFiller(), false);
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
    if (channel.getSize() != blobProperties.getBlobSize()) {
      throw new IllegalArgumentException("blob size in blob properties: " + blobProperties.getBlobSize() +
          "is different from the channel size: " + channel.getSize());
    }
    ByteBufferAsyncWritableChannel chunkFillerChannel = new ByteBufferAsyncWritableChannel();
    final PutOperation putOperation =
        new PutOperation(this, operationId, blobProperties, userMetaData, chunkFillerChannel, futureResult, callback,
            time);
    putOperations.put(operationId, putOperation);
    channel.readInto(chunkFillerChannel, new Callback<Long>() {
      @Override
      public void onCompletion(Long result, Exception exception) {
        if (exception != null) {
          putOperation.setOperationException(exception);
        } else if (result != channel.getSize()) {
          putOperation.setOperationException(
              new IllegalStateException("channel read finished successfully, " + "but not all data was read"));
        }
        putOperation.operationComplete = true;
      }
    });
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to datanodes in order to
   * complete put operations. Since this is the only method guaranteed to be called periodically by the chunkFiller
   * thread({@link #handleResponse} gets called only if a response is received for a put operation), any error handling
   * or operation completion and cleanup also gets done in the context of this method.
   * @param requestListToFill list to be filled with the requests created
   * @throws RouterException if an error is encountered during the creation of new requests.
   */
  void poll(List<RequestInfo> requestListToFill) {
    Iterator<PutOperation> iter = putOperations.values().iterator();
    while (iter.hasNext()) {
      PutOperation op = iter.next();
      op.fetchChunkPutRequests(requestListToFill);
    }
  }

  /**
   * Handles a response received for a PutRequest. If the response could not be deserialized or if a corresponding
   * {@link PutOperation.PutChunk} could not be found to handle the response, then the response is ignored.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    int correlationId = ((RequestOrResponse) responseInfo.getRequest()).getCorrelationId();
    PutOperation.PutChunk putChunk = correlationIdToPutChunk.remove(correlationId);
    if (putChunk != null) {
      putChunk.handleResponse(responseInfo);
    } else {
      // @todo: add a metric.
      logger.trace("Received response for a request of an operation that has completed.");
    }
  }

  void associateRequestWithChunk(int correlationId, PutOperation.PutChunk putChunk) {
    correlationIdToPutChunk.put(correlationId, putChunk);
  }

  /**
   * Returns a list of ids of successfully put chunks that were part of unsuccessful put operations.
   * @return list of ids to delete.
   */
  List<BlobId> getIdsToDelete() {
    // @todo save and return ids of failed puts.
    return null;
  }

  /**
   * Called by a {@link PutOperation} when the operation is complete. Any cleanup that the PutManager needs to do
   * with respect to this operation will have to be done here. The PutManager also finishes the operation by
   * performing the callback and notification.
   * @param operationId the operation id of the put operation that has completed.
   */

  void onComplete(long operationId) {
    PutOperation op = putOperations.remove(operationId);
    if (op.operationException != null) {
      // add blobs in the metadata chunk to ids_to_delete
    } else {
      notificationSystem.onBlobCreated(op.blobId.getID(), op.blobProperties, op.userMetadata);
    }
    NonBlockingRouter.completeOperation(op.futureResult, op.callback, op.blobId, op.operationException);
  }

  /**
   * the ChunkFiller thread continuously iterates over all the putOperations submitted, reads from the {@link
   * AsyncWritableChannel} associated with the operation, and fills in chunks. The channel will be populated
   * by the {@link ReadableStreamChannel} associated with the operation.
   */
  class ChunkFiller implements Runnable {
    private final int sleepTimeWhenIdleMs = 1;

    public void run() {
      while (true) {
        boolean workDoneInThisIteration = false;
        Iterator<PutOperation> iter = putOperations.values().iterator();
        while (iter.hasNext()) {
          PutOperation op = iter.next();
          if (op.fillChunks()) {
            workDoneInThisIteration = true;
          }
        }
        if (!workDoneInThisIteration) {
          try {
            Thread.sleep(sleepTimeWhenIdleMs);
          } catch (InterruptedException e) {
            logger.info("Caught interrupted exception while sleeping", e);
          }
        }
      }
    }
  }
}
