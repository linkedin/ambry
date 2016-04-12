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

import com.github.ambry.clustermap.ClusterMap;
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
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages all internal variables and states during the life cycle of a {@code DeleteOperation}.
 * There are multiple levels of concept that need to be made clear.
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
  private final ClusterMap clusterMap;
  private final ResponseHandler responseHandler;
  private final BlobId blobId;
  private final FutureResult<Void> futureResult;
  private final DeleteRequestRegistrationCallback deleteRequestRegistrationCallback;
  private final Callback<Void> callback;
  private final Void operationResult = null;
  private final RouterConfig routerConfig;

  private RouterException operationException = null;
  private RouterErrorCode resolvedRouterErrorCode;
  private int blobNotFound = 0;
  private int totalReplicas;
  private boolean onlyBlobNotFoundResponses = true;
  private boolean operationCompleted = false;

  private static final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(DeleteOperation.class);
  private static final HashMap<RouterErrorCode, Integer> precedenceLevels = new HashMap<RouterErrorCode, Integer>();

  /**
   * Instantiate a {@link DeleteOperation}.
   * @param routerConfig The {@link RouterConfig} that contains router-level configurations.
   * @param blobId The {@link BlobId} that is to be deleted by this {@code DeleteOperation}.
   * @param clusterMap A {@link ClusterMap}.
   * @param responsehandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param futureResult The {@link FutureResult} that is returned to the caller.
   * @param callback The {@link Callback} that is supplied by the caller.
   * @param requestRegistrationCallback The callback called to register the request in the {@link DeleteManager}.
   * @param time A {@link Time} reference.
   */
  DeleteOperation(RouterConfig routerConfig, BlobId blobId, ClusterMap clusterMap, ResponseHandler responsehandler,
      FutureResult<Void> futureResult, Callback<Void> callback,
      DeleteRequestRegistrationCallback requestRegistrationCallback, Time time) {
    this.routerConfig = routerConfig;
    this.blobId = blobId;
    this.clusterMap = clusterMap;
    this.responseHandler = responsehandler;
    this.futureResult = futureResult;
    this.callback = callback;
    this.deleteRequestRegistrationCallback = requestRegistrationCallback;
    this.time = time;
    this.totalReplicas = blobId.getPartition().getReplicaIds().size();
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
   * Get {@link DeleteRequest} for sending by {@link com.github.ambry.network.NetworkClient}.
   * @param requestInfos The list of {@link RequestInfo} for which a {@link DeleteRequest} is generated.
   */
  void fetchRequest(List<RequestInfo> requestInfos) {
    if (operationCompleted) {
      return;
    }
    checkExpiredInflightRequests();
    if (completeOperationIfDone()) {
      return;
    }
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      // @todo: getPortToConnectTo() in the ClusterMap interface should simply return the correct port without
      // @todo: taking any parameters. Rather than make the change to plug in sslEnabledDataCenters into
      // @todo: the NonBlockingRouter only to change later, we will hard code it here for now.
      Port port = new Port(replica.getDataNodeId().getPort(), PortType.PLAINTEXT);
      DeleteRequest deleteRequest = createDeleteRequest();
      inflightRequestInfos.put(deleteRequest.getCorrelationId(), new InflightRequestInfo(time.milliseconds(), replica));
      RequestInfo requestInfo = new RequestInfo(hostname, port, deleteRequest);
      requestInfos.add(requestInfo);
      replicaIterator.remove();
      deleteRequestRegistrationCallback.registerRequestToSend(this, requestInfo);
    }
  }

  /**
   * Create a {@link DeleteRequest} for sending.
   * @return The DeleteRequest.
   */
  private DeleteRequest createDeleteRequest() {
    return new DeleteRequest(correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname, blobId);
  }

  /**
   * Handle a response received from the {@link com.github.ambry.network.NetworkClient}. There
   * can be different cases during handling a response.
   * 1. Only one response of {@code ServerErrorCode.Blob_Deleted} will be sufficient to complete
   *    the {@code DeleteOperation}, with {@link RouterErrorCode} = {@code BlobDeleted}. However,
   *    this response will not be used if it is received after the operation is completed.
   * 2. If different {@link ServerErrorCode} received from multiple responses, these error codes
   *    needs to be used together to resolve a reasonable {@link RouterErrorCode}.
   * @param responseInfo The response to be handled.
   */
  void handleResponse(ResponseInfo responseInfo) {
    checkExpiredInflightRequests();
    if (completeOperationIfDone()) {
      return;
    }
    DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequest();
    InflightRequestInfo inflightRequestInfo = inflightRequestInfos.remove(deleteRequest.getCorrelationId());
    // inflightRequestInfo can be null if this request was timed out before this response is received.
    if (inflightRequestInfo != null) {
      ReplicaId replica = inflightRequestInfo.replica;
      // Check the error code from NetworkClient. This is only NetworkClientErrorCode.
      if (responseInfo.getError() != null) {
        responseHandler.onRequestResponseException(replica, new IOException(("NetworkClient error.")));
        updateRouterError(replica, RouterErrorCode.OperationTimedOut);
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
            operationException =
                new RouterException("Received wrong response that is not for the corresponding request.",
                    RouterErrorCode.UnexpectedInternalError);
            operationCompleted = true;
            return;
          } else {
            responseHandler.onRequestResponseError(replica, deleteResponse.getError());
            processServerError(replica, deleteResponse.getError());
          }
        } catch (IOException e) {
          // @todo: Even this should really not happen. But we need a metric.
          logger.error("Unable to recover a deleteResponse from received stream.");
          updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
        } catch (IllegalArgumentException e) {
          logger.error("Incompatible response is received for " + RequestOrResponseType.DeleteRequest);
          updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
        }
      }
      completeOperationIfDone();
    } else {
      // @todo: Add metrics for responses of which the request has already been timed out. This metrics will be
      // @todo: provide some information if we are setting reasonable timeout.
    }
  }

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
  private void checkExpiredInflightRequests() {
    Iterator<Map.Entry<Integer, InflightRequestInfo>> itr = inflightRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      InflightRequestInfo inflightRequestInfo = itr.next().getValue();
      if (time.milliseconds() - inflightRequestInfo.submissionTime > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        onlyBlobNotFoundResponses = false;
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
          onlyBlobNotFoundResponses = false;
          operationTracker.onResponse(replica, true);
          break;
        case Blob_Deleted:
          onlyBlobNotFoundResponses = false;
          logger.trace("Blob has already been deleted.");
          operationException = new RouterException("Blob has already been deleted.", RouterErrorCode.BlobDeleted);
          operationCompleted = true;
          break;
        case Blob_Not_Found:
          blobNotFound++;
          resolveRouterError(RouterErrorCode.BlobDoesNotExist);
          if (onlyBlobNotFoundResponses && blobNotFound == totalReplicas) {
            operationException = new RouterException("Blob does not exist", resolvedRouterErrorCode);
            operationCompleted = true;
            return;
          }
          operationTracker.onResponse(replica, false);
          break;
        case Blob_Expired:
          updateRouterError(replica, RouterErrorCode.BlobExpired);
          break;
        case Disk_Unavailable:
          updateRouterError(replica, RouterErrorCode.AmbryUnavailable);
          break;
        case IO_Error:
          updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
          break;
        case Partition_Unknown:
          updateRouterError(replica, RouterErrorCode.BlobDoesNotExist);
          break;
        case Partition_ReadOnly:
          updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
          break;
        default:
          logger.trace("Server returned an error: ", serverErrorCode);
          updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
          break;
      }
    } else {
      // This should not happen. If falls in this block, it is an illegal state.
      logger.error("Both Replica and ServerErrorCode cannot be null to handle.");
      updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Update the {@code DeleteOperation} based on the {@link RouterErrorCode}, and the source {@link ReplicaId}
   * for which the {@link RouterErrorCode} is generated.
   * @param replica The replica for which the RouterErrorCode was generated.
   * @param routerErrorCode {@link RouterErrorCode} that indicates the error for the replica.
   */
  private void updateRouterError(ReplicaId replica, RouterErrorCode routerErrorCode) {
    if (replica != null && routerErrorCode != null) {
      onlyBlobNotFoundResponses = false;
      resolveRouterError(routerErrorCode);
      operationTracker.onResponse(replica, false);
    } else {
      logger.error("Either replica or routerErrorCode is null when updating router error code.");
      updateRouterError(replica, RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Complete the {@code DeleteOperation} if it is done.
   */
  private boolean completeOperationIfDone() {
    if (operationTracker.isDone()) {
      if (!operationTracker.hasSucceeded()) {
        operationException =
            new RouterException("The DeleteOperation could not be completed.", resolvedRouterErrorCode);
      }
      operationCompleted = true;
    }
    return operationCompleted;
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
   * Resolve the {@link RouterErrorCode} depending on two codes' precedence level. If both old
   * code and new code are not assigned with precedence level, the new code will not override
   * the old code.
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

  boolean isOperationCompleted() {
    return operationCompleted;
  }

  BlobId getBlobId() {
    return blobId;
  }

  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  Callback<Void> getCallback() {
    return callback;
  }

  RouterException getOperationException() {
    return operationException;
  }

  Void getOperationResult() {
    return operationResult;
  }
}
