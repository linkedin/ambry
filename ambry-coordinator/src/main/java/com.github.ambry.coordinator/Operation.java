/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.coordinator;

import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.Response;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs an operation
 */
public abstract class Operation {
  // Operation context
  protected String datacenterName;
  protected ConnectionPool connectionPool;
  protected ExecutorService requesterPool;
  protected OperationContext context;
  protected BlobId blobId;

  // Operation state
  protected OperationPolicy operationPolicy;
  private long operationExpirationMs;
  private final AtomicBoolean operationComplete;

  BlockingQueue<OperationResponse> responseQueue;
  private Set<ReplicaId> requestsInFlight;
  private Logger logger = LoggerFactory.getLogger(getClass());
  protected CoordinatorError currentError;
  protected CoordinatorError resolvedError;

  public Operation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext context, BlobId blobId, long operationTimeoutMs, OperationPolicy operationPolicy) {
    this.datacenterName = datacenterName;
    this.connectionPool = connectionPool;
    this.requesterPool = requesterPool;
    this.context = context;
    this.blobId = blobId;

    this.operationPolicy = operationPolicy;
    this.operationExpirationMs = SystemTime.getInstance().milliseconds() + operationTimeoutMs;
    this.operationComplete = new AtomicBoolean(false);

    this.responseQueue = new ArrayBlockingQueue<OperationResponse>(operationPolicy.getReplicaIdCount());
    this.requestsInFlight = new HashSet<ReplicaId>();
  }

  protected abstract OperationRequest makeOperationRequest(ReplicaId replicaId);

  /**
   * Processes error code in response to determine if request was successful or not.
   *
   * @param replicaId replica that sent response
   * @param response the response to process for any error
   * @return The server error code after processing the response
   * @throws CoordinatorException
   */
  protected abstract ServerErrorCode processResponseError(ReplicaId replicaId, Response response)
      throws CoordinatorException;

  private void sendRequests() {
    logger.trace("{} sendRequests determining whether to send more requests", context);
    while (operationPolicy.sendMoreRequests(requestsInFlight)) {
      ReplicaId replicaIdToSendTo = operationPolicy.getNextReplicaIdForSend();
      logger.trace("{} sendRequests sending request to {}", context, replicaIdToSendTo);
      requestsInFlight.add(replicaIdToSendTo);
      requesterPool.submit(makeOperationRequest(replicaIdToSendTo));
    }
  }

  public void execute()
      throws CoordinatorException {
    logger.trace("{} operation beginning execute", context);
    sendRequests();
    while (true) {
      try {
        OperationResponse operationResponse =
            responseQueue.poll(operationExpirationMs - SystemTime.getInstance().milliseconds(), TimeUnit.MILLISECONDS);
        logger.trace("{} operation processing a response", context);
        logger.trace("Requests in flight {} " + requestsInFlight);
        if (operationResponse == null) {
          logger.error("{} Operation timed out", context);
          throw new CoordinatorException("Operation timed out.", CoordinatorError.OperationTimedOut);
        }

        ReplicaId replicaId = operationResponse.getReplicaId();
        logger.trace("Obtained Response from " + replicaId);
        if (!requestsInFlight.remove(replicaId)) {
          CoordinatorException e = new CoordinatorException("Coordinator received unexpected response",
              CoordinatorError.UnexpectedInternalError);
          logger.error("Response received from replica (" + replicaId + ") to which no request is in flight: ", e);
          throw e;
        }

        if (operationResponse.getError() == RequestResponseError.SUCCESS) {
          ServerErrorCode errorCode = processResponseError(replicaId, operationResponse.getResponse());
          logger.trace("Error code " + errorCode);
          if (errorCode == ServerErrorCode.No_Error) {
            operationPolicy.onSuccessfulResponse(replicaId);
            logger.trace("Success response from this replica. Operation Success ");
          } else {
            logger.trace("Failure response from this replica ");
            if (errorCode == ServerErrorCode.Data_Corrupt) {
              operationPolicy.onCorruptResponse(replicaId);
            } else {
              operationPolicy.onFailedResponse(replicaId);
            }
            resolveCoordinatorError(currentError);
            logger.trace("Resolved error " + currentError);
          }
        } else {
          if (operationResponse.getError() == RequestResponseError.MESSAGE_FORMAT_ERROR) {
            operationPolicy.onCorruptResponse(replicaId);
            logger.trace("Corrupt response ");
          } else {
            operationPolicy.onFailedResponse(replicaId);
            logger.trace("Failed response ");
          }
          setCurrentError(CoordinatorError.UnexpectedInternalError);
          resolveCoordinatorError(currentError);
        }

        if (operationPolicy.isComplete()) {
          operationComplete.set(true);
          logger.trace("{} operation successfully completing execute", context);
          this.onOperationComplete();
          return;
        }
        if (!operationPolicy.mayComplete()) {
          if (operationPolicy.isCorrupt()) {
            logger.error("{} operation is corrupt.", context);
            context.getCoordinatorMetrics().corruptionError.inc();
          }
          String message = getErrorMessage();
          logger.trace("{} {}", context, message);
          throw new CoordinatorException(message, getResolvedError());
        }
        sendRequests();
        logger.trace("Requests in flight after processing an operation response " + requestsInFlight);
      } catch (CoordinatorException e) {
        operationComplete.set(true);
        if (logger.isTraceEnabled() || e.getErrorCode() == CoordinatorError.BlobDoesNotExist
            || e.getErrorCode() == CoordinatorError.BlobExpired) {
          logger.trace(context + " operation threw CoordinatorException during execute.", e);
        } else {
          logger.error(context + " operation threw CoordinatorException during execute: " + e);
        }
        throw e;
      } catch (InterruptedException e) {
        operationComplete.set(true);
        // Slightly abuse the notion of "unexpected" internal error since InterruptedException does not indicate
        // something truly unexpected.
        logger.error(context + " operation interrupted during execute");
        throw new CoordinatorException("Operation interrupted.", e, CoordinatorError.UnexpectedInternalError);
      }
    }
  }

  public void resolveCoordinatorError(CoordinatorError newError) {
    if (this.resolvedError == null) {
      this.resolvedError = newError;
    } else {
      if (getPrecedenceLevel(newError) < getPrecedenceLevel(resolvedError)) {
        this.resolvedError = newError;
      }
    }
  }

  public CoordinatorError getResolvedError() {
    return this.resolvedError;
  }

  abstract Integer getPrecedenceLevel(CoordinatorError coordinatorError);

  public String getErrorMessage() {
    String message = "";
    switch (resolvedError) {
      case AmbryUnavailable:
        message += "Insufficient DataNodes replied to complete operation " + context + ":" + operationPolicy +
            "resulting in AmbryUnavailable";
        break;
      case BlobDoesNotExist:
        message += "BlobDoesNotExist to perform the operation " + context + ":" + operationPolicy;
        break;
      case BlobDeleted:
        message += "Cannot perform the operation " + context + ":" + operationPolicy + " as Blob is deleted ";
        break;
      case BlobExpired:
        message += "Cannot perform the operation " + context + ":" + operationPolicy + " as Blob expired ";
        break;
      default:
        message +=
            "Experienced an unexpected error " + resolvedError + " for operation " + context + ":" + operationPolicy;
    }
    return message;
  }

  public synchronized void setCurrentError(CoordinatorError currentError) {
    this.currentError = currentError;
  }

  public synchronized CoordinatorError getCurrentError() {
    return this.currentError;
  }

  /**
   * Actions to be taken on completion of an operation
   */
  public void onOperationComplete() {
    // only GetOperation should have definition. For rest, its a no-op
  }
}

/**
 * OperationRequest is a single blocking request issued by an operation. OperationRequest drives the single blocking
 * request until the response is received and deserialized. Or, until request-response fails.
 */
abstract class OperationRequest implements Runnable {
  private final ConnectionPool connectionPool;
  private final BlockingQueue<OperationResponse> responseQueue;
  protected final OperationContext context;
  private final BlobId blobId;
  protected final ReplicaId replicaId;
  private final RequestOrResponse request;
  private final long operationQueuingTimeInMs = System.currentTimeMillis();
  private ResponseHandler responseHandler;
  protected boolean sslEnabled;
  private Port port;

  private Logger logger = LoggerFactory.getLogger(getClass());

  protected OperationRequest(ConnectionPool connectionPool, BlockingQueue<OperationResponse> responseQueue,
      OperationContext context, BlobId blobId, ReplicaId replicaId, RequestOrResponse request) {
    this.connectionPool = connectionPool;
    this.responseQueue = responseQueue;
    this.context = context;
    this.blobId = blobId;
    this.replicaId = replicaId;
    this.request = request;
    this.responseHandler = context.getResponseHandler();
    this.port = replicaId.getDataNodeId().getPortToConnectTo(context.getSslEnabledDatacenters());
    this.sslEnabled = port.getPortType() == PortType.SSL;
    context.getCoordinatorMetrics().totalRequestsInFlight.incrementAndGet();
  }

  protected abstract Response getResponse(DataInputStream dataInputStream)
      throws IOException;

  protected abstract void markRequest();

  protected abstract void updateRequest(long durationInMs);

  void deserializeResponsePayload(Response response)
      throws IOException, MessageFormatException {
    // Only Get responses have a payload to be deserialized.
  }

  private Port getPort() {
    return this.port;
  }

  @Override
  public void run() {
    context.getCoordinatorMetrics().totalRequestsInExecution.incrementAndGet();
    ConnectedChannel connectedChannel = null;
    long startTimeInMs = System.currentTimeMillis();
    context.getCoordinatorMetrics().operationRequestQueuingTimeInMs.update(startTimeInMs - operationQueuingTimeInMs);

    try {
      logger.trace("{} {} checking out connection", context, replicaId);
      context.getCoordinatorMetrics().sslConnectionsRequestRate.mark();
      connectedChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), getPort(),
          context.getConnectionPoolCheckoutTimeout());
      logger.trace("{} {} sending request", context, replicaId);
      connectedChannel.send(request);
      logger.trace("{} {} receiving response", context, replicaId);
      InputStream responseStream = connectedChannel.receive().getInputStream();
      logger.trace("{} {} processing response", context, replicaId);
      Response response = getResponse(new DataInputStream(responseStream));

      if (response == null) {
        logger.error(context + " " + replicaId + " Response to request is null. BlobId " + blobId);
        enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.UNEXPECTED_ERROR));
        return;
      }

      logger.trace("{} {} deserializing response payload", context, replicaId);
      deserializeResponsePayload(response);

      logger.trace("{} {} checking in connection", context, replicaId);
      connectionPool.checkInConnection(connectedChannel);
      connectedChannel = null;

      enqueueOperationResponse(new OperationResponse(replicaId, response));
      responseHandler.onRequestResponseError(replicaId, response.getError());
    } catch (IOException e) {
      logger.error(context + " " + replicaId + " Error processing request-response for BlobId " + blobId, e);
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.IO_ERROR));
      countError(RequestResponseError.IO_ERROR);
      responseHandler.onRequestResponseException(replicaId, e);
    } catch (MessageFormatException e) {
      logger.error(context + " " + replicaId + " Error processing request-response for BlobId " + blobId, e);
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.MESSAGE_FORMAT_ERROR));
      countError(e.getErrorCode());
      responseHandler.onRequestResponseException(replicaId, e);
    } catch (ConnectionPoolTimeoutException e) {
      logger.error(context + " " + replicaId + " Error processing request-response for BlobId " + blobId, e);
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.TIMEOUT_ERROR));
      countError(RequestResponseError.TIMEOUT_ERROR);
      responseHandler.onRequestResponseException(replicaId, e);
    } catch (Exception e) {
      logger.error(context + " " + replicaId + " Error processing request-response for BlobId " + blobId, e);
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.UNEXPECTED_ERROR));
      countError(RequestResponseError.UNEXPECTED_ERROR);
      responseHandler.onRequestResponseException(replicaId, e);
    } finally {
      if (connectedChannel != null) {
        logger.trace("{} {} destroying connection", context, replicaId);
        connectionPool.destroyConnection(connectedChannel);
      }
      markRequest();
      updateRequest(System.currentTimeMillis() - startTimeInMs);
      context.getCoordinatorMetrics().totalRequestsInExecution.decrementAndGet();
      context.getCoordinatorMetrics().totalRequestsInFlight.decrementAndGet();
    }
  }

  private void countError(MessageFormatErrorCodes error) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.countError(error, sslEnabled);
    }
  }

  private void countError(RequestResponseError error) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.countError(error, sslEnabled);
    }
  }

  private void enqueueOperationResponse(OperationResponse operationResponse) {
    if (!responseQueue.offer(operationResponse)) {
      logger.error(context + " " + replicaId +
          " responseQueue incorrectly sized since offer() returned false.  BlobId ", blobId);
    }
  }
}

enum RequestResponseError {
  SUCCESS,
  UNEXPECTED_ERROR,
  IO_ERROR,
  MESSAGE_FORMAT_ERROR,
  TIMEOUT_ERROR
}

/**
 * OperationResponse encapsulates a complete response from a specific replica.
 */
class OperationResponse {
  private final ReplicaId replicaId;
  private final Response response;
  private final RequestResponseError error;

  /**
   * Construct successful response for a request.
   *
   * @param replicaId ReplicaId from which response is returned. Cannot be null.
   * @param response Response from replica. May be null. Null indicates unexpected exceptions sending request or
   * processing response.
   */
  public OperationResponse(ReplicaId replicaId, Response response) {
    this.replicaId = replicaId;
    this.response = response;
    this.error = RequestResponseError.SUCCESS;
  }

  /**
   * Construct failed response for a request.
   *
   * @param replicaId ReplicaId from which response is returned. Cannot be null.
   * @param error experienced during request-response
   */
  public OperationResponse(ReplicaId replicaId, RequestResponseError error) {
    this.replicaId = replicaId;
    this.response = null;
    this.error = error;
  }

  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public Response getResponse() {
    return response;
  }

  public RequestResponseError getError() {
    return error;
  }
}

