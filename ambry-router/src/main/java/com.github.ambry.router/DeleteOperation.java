/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages all internal variables and states during the life cycle of a {@code DeleteOperation}. For
 * simple blobs (blobs that are under the max put chunk size), the delete request will be issued to the actual
 * blob. For composite blobs, each composite blob is chunked to multiple pieces and stored individually with a
 * metadata blob that manages information of all the pieces. The delete request will be issued only to the metadata
 * blob.
 * </p>
 * This class involvs multiple concepts:
 *   {@link DeleteOperation}: the highest level of a "request" to delete a blob based on its blob id.
 *                            This virtual request comprises a number of physical requests (i.e.,
 *                            {@link RequestInfo}).
 *   {@link RequestInfo}:     a wrapper of a request that is sent to a specific replica.
 *   {@link DeleteRequest}:   the actual request sent to a replica. It is a destination-agnostic, router-server
 *                            protocol that requests a store server to make a deletion.
 *   {@link ResponseInfo}:    a wrapper of a response that is received for the {@code DeleteOperation}. A
 *                            {@link DeleteOperation} can be completed after a number of responses have been
 *                            received. ResponseInfo is one-to-one mapped to a RequestInfo.
 */
class DeleteOperation {
  private final Time time;
  private final OperationTracker operationTracker;
  private final HashMap<Integer, InflightRequestInfo> inflightRequestInfos;
  private final ResponseHandler responseHandler;
  private final BlobId blobId;
  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final Void operationResult = null;
  private final RouterConfig routerConfig;

  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  private RouterErrorCode resolvedRouterErrorCode;
  private boolean operationCompleted = false;

  private static final Logger logger = LoggerFactory.getLogger(DeleteOperation.class);
  private static final HashMap<RouterErrorCode, Integer> precedenceLevels = new HashMap<RouterErrorCode, Integer>();

  /**
   * Instantiate a {@link DeleteOperation}.
   * @param routerConfig The {@link RouterConfig} that contains router-level configurations.
   * @param responsehandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param blobId The {@link BlobId} that is to be deleted by this {@code DeleteOperation}.
   * @param futureResult The {@link FutureResult} that is returned to the caller.
   * @param callback The {@link Callback} that is supplied by the caller.
   * @param time A {@link Time} reference.
   */
  DeleteOperation(RouterConfig routerConfig, ResponseHandler responsehandler, BlobId blobId,
      FutureResult<Void> futureResult, Callback<Void> callback, Time time) {
    this.routerConfig = routerConfig;
    this.blobId = blobId;
    this.responseHandler = responsehandler;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.inflightRequestInfos = new HashMap<Integer, InflightRequestInfo>();
    this.operationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, blobId.getPartition(), true,
        routerConfig.routerDeleteSuccessTarget, routerConfig.routerDeleteRequestParallelism, false);
  }

  /**
   * The predefined precedence levels among different {@link RouterErrorCode}. This will help resolve
   * {@link RouterErrorCode} if different {@link RouterErrorCode} are returned by multiple replicas. For
   * example, if one replica causes {@link RouterErrorCode.BlobExpired}, and another replica causes
   * {@link RouterErrorCode.AmbryUnavailable}, the final RouterErrorCode should be {@link RouterErrorCode.BlobExpired}.
   */
  static {
    precedenceLevels.put(RouterErrorCode.BlobDeleted, 1);
    precedenceLevels.put(RouterErrorCode.BlobExpired, 2);
    precedenceLevels.put(RouterErrorCode.AmbryUnavailable, 3);
    precedenceLevels.put(RouterErrorCode.UnexpectedInternalError, 4);
    precedenceLevels.put(RouterErrorCode.OperationTimedOut, 5);
    precedenceLevels.put(RouterErrorCode.BlobDoesNotExist, 6);
  }

  /**
   * Get a list of {@link DeleteRequest} for sending to replicas.
   * @param requestFillCallback the {@link DeleteRequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(DeleteRequestRegistrationCallback requestFillCallback) {
    cleanupExpiredInflightRequests();
    checkAndMaybeComplete();
    if (isOperationComplete()) {
      return;
    }
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      Port port = new Port(replica.getDataNodeId().getPort(), PortType.PLAINTEXT);
      DeleteRequest deleteRequest = createDeleteRequest();
      inflightRequestInfos.put(deleteRequest.getCorrelationId(), new InflightRequestInfo(time.milliseconds(), replica));
      RequestInfo requestInfo = new RequestInfo(hostname, port, deleteRequest);
      requestFillCallback.registerRequestToSend(this, requestInfo);
      replicaIterator.remove();
    }
  }

  /**
   * Create a {@link DeleteRequest} for sending to a replica.
   * @return The DeleteRequest.
   */
  private DeleteRequest createDeleteRequest() {
    return new DeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
        blobId);
  }

  /**
   * Handle a response for a delete operation. It determines whether the request was successful,
   * updates operation tracker, and notifies the response handler for failure detection.
   * can be different cases during handling a response. For the same delete operation, it is possible
   * that different {@link ServerErrorCode} are received from different replicas. These error codes
   * are eventually resolved to a single {@link RouterErrorCode}.
   * @param responseInfo The response to be handled.
   */
  void handleResponse(ResponseInfo responseInfo) {
    DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequest();
    InflightRequestInfo inflightRequestInfo = inflightRequestInfos.remove(deleteRequest.getCorrelationId());
    // inflightRequestInfo can be null if this request was timed out before this response is received.
    if (inflightRequestInfo == null) {
      return;
    }
    ReplicaId replica = inflightRequestInfo.replica;
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      responseHandler.onRequestResponseException(replica, new IOException(("NetworkClient error.")));
      updateOperationStatus(replica, RouterErrorCode.OperationTimedOut);
    } else {
      try {
        DeleteResponse deleteResponse =
            DeleteResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())));
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (deleteResponse.getCorrelationId() != deleteRequest.getCorrelationId()) {
          logger.error("The correlation id in the DeleteResponse " + deleteResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated DeleteRequest: " + deleteRequest
              .getCorrelationId());
          setOperationException(
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
          return;
        } else {
          responseHandler.onRequestResponseError(replica, deleteResponse.getError());
          processServerError(replica, deleteResponse.getError());
        }
      } catch (IOException e) {
        // @todo: Even this should really not happen. But we need a metric.
        logger.error("Unable to recover a deleteResponse from received stream.");
        updateOperationStatus(replica, RouterErrorCode.UnexpectedInternalError);
      }
    }
    checkAndMaybeComplete();
  }

  /**
   * A wrapper class that is used to check if a request has been expired.
   */
  private class InflightRequestInfo {
    private long submissionTime;
    private ReplicaId replica;

    InflightRequestInfo(long submissionTime, ReplicaId replica) {
      this.submissionTime = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Go through the inflight request list of this {@code DeleteOperation} and remove those that
   * have been timed out.
   */
  private void cleanupExpiredInflightRequests() {
    Iterator<Map.Entry<Integer, InflightRequestInfo>> itr = inflightRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      InflightRequestInfo inflightRequestInfo = itr.next().getValue();
      if (time.milliseconds() - inflightRequestInfo.submissionTime > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        resolveRouterError(RouterErrorCode.OperationTimedOut);
        operationTracker.onResponse(inflightRequestInfo.replica, false);
      }
    }
  }

  /**
   * Update the DeleteOperation based on the {@link ServerErrorCode}, and the source {@link ReplicaId}
   * for which the {@link ServerErrorCode} is generated. This method internally maps a {@link ServerErrorCode}
   * to a {@link RouterErrorCode}.
   * @param replica The replica for which the ServerErrorCode was generated.
   * @param serverErrorCode ServerErrorCode that indicates the error for the replica.
   */
  private void processServerError(ReplicaId replica, ServerErrorCode serverErrorCode) {
    if (replica != null && serverErrorCode != null) {
      switch (serverErrorCode) {
        case No_Error:
          logger.trace("The delete request was successful.");
          operationTracker.onResponse(replica, true);
          break;
        case Blob_Deleted:
          logger.trace("Blob has already been deleted.");
          operationTracker.onResponse(replica, true);
          break;
        case Blob_Not_Found:
          resolveRouterError(RouterErrorCode.BlobDoesNotExist);
          operationTracker.onResponse(replica, false);
          break;
        case Blob_Expired:
          updateOperationStatus(replica, RouterErrorCode.BlobExpired);
          break;
        case Disk_Unavailable:
          updateOperationStatus(replica, RouterErrorCode.AmbryUnavailable);
          break;
        case IO_Error:
          updateOperationStatus(replica, RouterErrorCode.UnexpectedInternalError);
          break;
        case Partition_Unknown:
          updateOperationStatus(replica, RouterErrorCode.BlobDoesNotExist);
          break;
        case Partition_ReadOnly:
          updateOperationStatus(replica, RouterErrorCode.UnexpectedInternalError);
          break;
        default:
          logger.trace("Server returned an error: ", serverErrorCode);
          updateOperationStatus(replica, RouterErrorCode.UnexpectedInternalError);
          break;
      }
    } else {
      // This should not happen. If falls in this block, it is an illegal state.
      logger.error("Both Replica and ServerErrorCode cannot be null to handle.");
      updateOperationStatus(replica, RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Update the {@code DeleteOperation} based on the {@link RouterErrorCode}, and the source {@link ReplicaId}
   * for which the {@link RouterErrorCode} is generated.
   * @param replica The replica for which the RouterErrorCode was generated.
   * @param routerErrorCode {@link RouterErrorCode} that indicates the error for the replica.
   */
  private void updateOperationStatus(ReplicaId replica, RouterErrorCode routerErrorCode) {
    if (replica != null && routerErrorCode != null) {
      resolveRouterError(routerErrorCode);
      operationTracker.onResponse(replica, false);
    } else {
      logger.error("Either replica or routerErrorCode is null when updating router error code.");
      resolveRouterError(RouterErrorCode.UnexpectedInternalError);
      operationTracker.onResponse(replica, false);
    }
  }

  /**
   * Complete the {@code DeleteOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    if (operationTracker.isDone()) {
      if (!operationTracker.hasSucceeded()) {
        setOperationException(
            new RouterException("The DeleteOperation could not be completed.", resolvedRouterErrorCode));
      }
      operationCompleted = true;
    }
  }

  /**
   * Get the precedence level for a {@link RouterErrorCode}. A precedence level is a relative priority assigned
   * to a {@link RouterErrorCode}. If a {@link RouterErrorCode} has not been assigned a precedence level, a
   * {@code Integer.MIN_VALUE} will be returned.
   * @param routerErrorCode The {@link RouterErrorCode} for which to get its precedence level.
   * @return The precedence level of the {@link RouterErrorCode}.
   */
  private Integer getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    if (precedenceLevels.containsKey(routerErrorCode)) {
      return precedenceLevels.get(routerErrorCode);
    } else {
      return Integer.MIN_VALUE;
    }
  }

  /**
   * Resolve the {@link RouterErrorCode} depending on two codes' precedence level. An error code with a
   * smaller precedence level overrides an error code with a larger precedence level. If both error codes
   * are not assigned with precedence level, the old code will not be overriden.
   * @param newError The error code to resolve.
   */
  private void resolveRouterError(RouterErrorCode newError) {
    if (resolvedRouterErrorCode == null) {
      resolvedRouterErrorCode = newError;
    } else {
      if (getPrecedenceLevel(newError) < getPrecedenceLevel(resolvedRouterErrorCode)) {
        resolvedRouterErrorCode = newError;
      }
    }
  }

  /**
   * Returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * Get {@link BlobId} of this {@code DeleteOperation}.
   * @return The {@link BlobId}.
   */
  BlobId getBlobId() {
    return blobId;
  }

  /**
   * Get the {@link FutureResult} for this {@code DeleteOperation}.
   * @return The {@link FutureResult}.
   */
  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  /**
   * Get the {@link Callback} for this {@code DeleteOperation}.
   * @return The {@link Callback}.
   */
  Callback<Void> getCallback() {
    return callback;
  }

  /**
   * The exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * Get the result for this {@code DeleteOperation}. In a {@link DeleteOperation}, nothing is returned
   * to the caller as a result of this operation. Including this {@link Void} result is for consistency
   * with other operations.
   * @return Void.
   */
  Void getOperationResult() {
    return operationResult;
  }

  /**
   * Set the exception associated with this operation. When this is called, the operation has failed.
   * @param exception the irrecoverable exception associated with this operation.
   */
  void setOperationException(Exception exception) {
    operationException.set(exception);
    operationCompleted = true;
  }
}
