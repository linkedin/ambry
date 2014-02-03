package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.shared.*;
import com.github.ambry.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private OperationPolicy operationPolicy;
  private long operationExpirationMs;
  private final AtomicBoolean operationComplete;

  BlockingQueue<OperationResponse> responseQueue;
  private Set<ReplicaId> requestsInFlight;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Operation(String datacenterName,
                   ConnectionPool connectionPool,
                   ExecutorService requesterPool,
                   OperationContext context,
                   BlobId blobId,
                   long operationTimeoutMs,
                   OperationPolicy operationPolicy) {
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
   * @param serverErrorCode error code in response
   * @return true if response is successful
   * @throws CoordinatorException
   */
  protected abstract boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws
          CoordinatorException;

  private void sendRequests() {
    logger.debug("{} sendRequests determining whether to send more requests", context);
    while (operationPolicy.sendMoreRequests(requestsInFlight)) {
      ReplicaId replicaIdToSendTo = operationPolicy.getNextReplicaIdForSend();
      logger.debug("{} sendRequests sending request to {}", context, replicaIdToSendTo);
      requestsInFlight.add(replicaIdToSendTo);
      requesterPool.submit(makeOperationRequest(replicaIdToSendTo));
    }
  }

  public void execute() throws CoordinatorException {
    logger.debug("{} operation beginning execute", context);
    sendRequests();
    while (true) {
      try {
        OperationResponse operationResponse =
                responseQueue.poll(operationExpirationMs - SystemTime.getInstance().milliseconds(),
                                   TimeUnit.MILLISECONDS);
        logger.debug("{} operation processing a response", context);
        if (operationResponse == null) {
          throw new CoordinatorException("Operation timed out.", CoordinatorError.OperationTimedOut);
        }

        ReplicaId replicaId = operationResponse.getReplicaId();
        if (!requestsInFlight.remove(replicaId)) {
          CoordinatorException e = new CoordinatorException("Coordinator received unexpected response",
                                                            CoordinatorError.UnexpectedInternalError);
          logger.error("Response received from replica ({}) to which no request is in flight: {}", replicaId, e);
          throw e;
        }

        if (operationResponse.getError() == RequestResponseError.SUCCESS) {
          if (processResponseError(replicaId, operationResponse.getResponse().getError())) {
            operationPolicy.onSuccessfulResponse(replicaId);
          }
          else {
            operationPolicy.onFailedResponse(replicaId);
          }
        }
        else {
          // Currently, no actions taken based upon specific RequestResponseError returned. Possible actions include
          // retrying request, updating soft-state, notifying datanode and so forth. Specific action may depend on
          // operation type.
          // At the least, need metric for each error type here. And, need to make sure that 'data corrupt' tracked
          // on its own since that indicates possibly issues with server-side state. If multiple responses return
          // corrupt, we need to signal a serious alert because we likely have a software bug corrupting state on
          // servers.
          operationPolicy.onFailedResponse(replicaId);
        }

        if (operationPolicy.isComplete()) {
          operationComplete.set(true);
          logger.debug("{} operation successfully completing execute", context);
          return;
        }
        if (!operationPolicy.mayComplete()) {
          throw new CoordinatorException("Insufficient DataNodes replied to complete operation",
                                         CoordinatorError.AmbryUnavailable);
        }
        sendRequests();
      }
      catch (CoordinatorException e) {
        operationComplete.set(true);
        logger.debug("{} operation throwing CoordinatorException during ({}).", context, e.getErrorCode());
        throw e;
      }
      catch (InterruptedException e) {
        operationComplete.set(true);
        // Slightly abuse the notion of "unexpected" internal error since InterruptedException does not indicate
        // something truly unexpected.
        logger.info("{} operation interrupted during execute", context);
        throw new CoordinatorException("Operation interrupted.", CoordinatorError.UnexpectedInternalError);
      }
    }
  }
}

/**
 * OperationRequest is a single blocking request issued by an operation. OperationRequest drives the single blocking
 * request until the response is received and deserialized. Or, until request-response fails.
 */
abstract class OperationRequest implements Runnable {
  private final ConnectionPool connectionPool;
  private final BlockingQueue<OperationResponse> responseQueue;
  private final OperationContext context;
  private final BlobId blobId;
  private final ReplicaId replicaId;
  private final RequestOrResponse request;

  private Logger logger = LoggerFactory.getLogger(getClass());

  protected OperationRequest(ConnectionPool connectionPool,
                             BlockingQueue<OperationResponse> responseQueue,
                             OperationContext context,
                             BlobId blobId,
                             ReplicaId replicaId,
                             RequestOrResponse request) {
    this.connectionPool = connectionPool;
    this.responseQueue = responseQueue;
    this.context = context;
    this.blobId = blobId;
    this.replicaId = replicaId;
    this.request = request;
  }

  protected abstract Response getResponse(DataInputStream dataInputStream) throws IOException;

  void deserializeResponsePayload(Response response) throws IOException, MessageFormatException {
    // Only Get responses have a payload to be deserialized.
  }

  @Override
  public void run() {
    ConnectedChannel connectedChannel = null;
    try {
      logger.debug("{} {} checking out connection", context, replicaId);
      connectedChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(),
                                                           replicaId.getDataNodeId().getPort(),
                                                           context.getConnectionPoolCheckoutTimeout());
      logger.debug("{} {} sending request", context, replicaId);
      connectedChannel.send(request);
      logger.debug("{} {} receiving response", context, replicaId);
      InputStream responseStream = connectedChannel.receive();
      logger.debug("{} {} processing response", context, replicaId);
      Response response = getResponse(new DataInputStream(responseStream));

      if (response == null) {
        logger.error("{} {} Response to request is null. BlobId {}.", context, replicaId, blobId);
        enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.UNEXPECTED_ERROR));
        return;
      }

      logger.debug("{} {} deserializing response payload", context, replicaId);
      deserializeResponsePayload(response);

      logger.debug("{} {} checking in connection", context, replicaId);
      connectionPool.checkInConnection(connectedChannel);
      connectedChannel = null;

      enqueueOperationResponse(new OperationResponse(replicaId, response));
    }
    catch (IOException e) {
      logger.error("{} {} Error processing request-response for BlobId {} : {}.",
                   context, replicaId, blobId, e.getCause());
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.IO_ERROR));
    }
    catch (MessageFormatException e) {
      logger.error("{} {} Error processing request-response for BlobId {} : {} - {}.",
                   context, replicaId, blobId, e.getErrorCode(), e.getCause());
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.MESSAGE_FORMAT_ERROR));
    }
    catch (ConnectionPoolTimeoutException e) {
      logger.error("{} {} Error processing request-response for BlobId {} : connectionpooltimeout - {}.",
                   context, replicaId, blobId, e.getCause());
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.TIMEOUT_ERROR));
    }
    catch (Exception e) {
      logger.error("{} {} Error processing request-response for BlobId {} : unexpected error - {}.",
                   context, replicaId, blobId, e.getCause());
      enqueueOperationResponse(new OperationResponse(replicaId, RequestResponseError.UNEXPECTED_ERROR));
    }
    finally {
      if (connectedChannel != null) {
        logger.debug("{} {} destroying connection", context, replicaId);
        connectionPool.destroyConnection(connectedChannel);
      }
    }
  }

  private void enqueueOperationResponse(OperationResponse operationResponse) {
    if (!responseQueue.offer(operationResponse)) {
      logger.error("{} {} responseQueue incorrectly sized since offer() returned false.  BlobId {}.", context,
                   replicaId, blobId);
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



