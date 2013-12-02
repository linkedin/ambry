package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.BlockingChannelPool;
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
  protected BlockingChannelPool connectionPool;
  protected ExecutorService requesterPool;
  protected OperationContext oc;
  protected BlobId blobId;

  // Operation state
  protected OperationPolicy operationPolicy;
  protected long operationExpirationMs;
  protected final AtomicBoolean operationComplete;

  protected BlockingQueue<OperationResponse> responseQueue;
  protected Set<ReplicaId> requestsInFlight;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Operation(String datacenterName, BlockingChannelPool connectionPool, ExecutorService requesterPool,
                   OperationContext oc, BlobId blobId, long operationTimeoutMs, OperationPolicy operationPolicy) {
    this.datacenterName = datacenterName;
    this.connectionPool = connectionPool;
    this.requesterPool = requesterPool;
    this.oc = oc;
    this.blobId = blobId;

    this.operationPolicy = operationPolicy;
    this.operationExpirationMs = SystemTime.getInstance().milliseconds() + operationTimeoutMs;
    this.operationComplete = new AtomicBoolean(false);

    this.responseQueue = new ArrayBlockingQueue<OperationResponse>(operationPolicy.getReplicaIdCount());
    this.requestsInFlight = new HashSet<ReplicaId>();
  }

  /**
   * OperationRequest is a single blocking request issued by an operation. OperationRequest drives the single blocking
   * request until the response is received and deserialized. Or, until request-response fails.
   */
  protected abstract class OperationRequest implements Runnable {
    private ReplicaId replicaId;
    private RequestOrResponse request;

    protected OperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      this.replicaId = replicaId;
      this.request = request;
    }

    protected abstract Response getResponse(DataInputStream dataInputStream) throws IOException;

    protected void deserializeResponsePayload(Response response) throws CoordinatorException, IOException,
            MessageFormatException {
      // Only Get responses have a payload to be deserialized.
    }

    @Override
    public void run() {
      BlockingChannel blockingChannel = null;
      try {
        logger.debug("{} {} checking out connection", oc, replicaId);
        blockingChannel = connectionPool.checkOutConnection(replicaId.getDataNodeId());
        logger.debug("{} {} sending request", oc, replicaId);
        blockingChannel.send(request);
        logger.debug("{} {} receiving response", oc, replicaId);
        InputStream responseStream = blockingChannel.receive();
        logger.debug("{} {} processing response", oc, replicaId);
        Response response = getResponse(new DataInputStream(responseStream));

        if (response == null) {
          throw new CoordinatorException("Response is null.", CoordinatorError.UnexpectedInternalError);
        }
        if (!response.isSendComplete()) {
          throw new CoordinatorException("Response is not complete.", CoordinatorError.UnexpectedInternalError);
        }

        logger.debug("{} {} deserializing response payload", oc, replicaId);
        deserializeResponsePayload(response);

        logger.debug("{} {} checking in connection", oc, replicaId);
        connectionPool.checkInConnection(replicaId.getDataNodeId(), blockingChannel);
        blockingChannel = null;

        if (!responseQueue.offer(new OperationResponse(replicaId, response))) {
          throw new CoordinatorException("responseQueue incorrectly sized since offer() returned false.",
                                         CoordinatorError.UnexpectedInternalError);
        }
      }
      catch (CoordinatorException e) {
        logger.error("{} {} Error processing request-response for BlobId {} : {}.", oc, replicaId, blobId,
                     e.getCause());
        e.printStackTrace();
      }
      catch (IOException e) {
        logger.error("{} {} Error processing request-response for BlobId {} : {}.", oc, replicaId, blobId,
                     e.getCause());
        e.printStackTrace();
      }
      catch (MessageFormatException e) {
        logger.error("{} {} Error processing request-response for BlobId {} : {}.", oc, replicaId, blobId,
                     e.getCause());
        e.printStackTrace();
      }
      finally {
        if (blockingChannel != null) {
          logger.debug("{} {} destroying connection", oc, replicaId);
          connectionPool.destroyConnection(replicaId.getDataNodeId(), blockingChannel);
          responseQueue.offer(new OperationResponse(replicaId, null));
        }
      }
    }
  }

  protected abstract OperationRequest makeOperationRequest(ReplicaId replicaId);

  /**
   * OperationResponse encapsulates a complete response from a specific replica.
   */
  protected static class OperationResponse {
    private ReplicaId replicaId;
    private Response response;

    /**
     * Construct response for a request.
     *
     * @param replicaId ReplicaId from which response is returned. Cannot be null.
     * @param response Response from replica. May be null. Null indicates unexpected exceptions sending request or processing response.
     */
    public OperationResponse(ReplicaId replicaId, Response response) {
      this.replicaId = replicaId;
      this.response = response;
    }

    public ReplicaId getReplicaId() {
      return replicaId;
    }

    public Response getResponse() {
      return response;
    }
  }

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
    logger.debug("{} sendRequests determining whether to send more requests", oc);
    while (operationPolicy.sendMoreRequests(requestsInFlight)) {
      ReplicaId replicaIdToSendTo = operationPolicy.getNextReplicaIdForSend();
      logger.debug("{} sendRequests sending request to {}", oc, replicaIdToSendTo);
      requestsInFlight.add(replicaIdToSendTo);
      requesterPool.submit(makeOperationRequest(replicaIdToSendTo));
    }
  }

  public void execute() throws CoordinatorException {
    logger.debug("{} operation beginning execute", oc);
    sendRequests();
    while (true) {
      try {
        OperationResponse operationResponse =
                responseQueue.poll(operationExpirationMs - SystemTime.getInstance().milliseconds(),
                                   TimeUnit.MILLISECONDS);
        logger.debug("{} operation processing a response", oc);
        if (operationResponse == null) {
          throw new CoordinatorException("Operation timed out.", CoordinatorError.OperationTimedOut);
        }

        ReplicaId replicaId = operationResponse.getReplicaId();
        Response response = operationResponse.getResponse();

        if (!requestsInFlight.remove(replicaId)) {
          throw new CoordinatorException("Response received from replica to which no request is in flight. ReplicaId:" +
                                         " " + replicaId, CoordinatorError.UnexpectedInternalError);
        }
        if (response != null) {
          if (processResponseError(replicaId, response.getError())) {
            operationPolicy.successfulResponse(replicaId);
          }
          else {
            operationPolicy.failedResponse(replicaId);
          }
        }
        else {
          operationPolicy.failedResponse(replicaId);
        }

        if (operationPolicy.isDone()) {
          operationComplete.set(true);
          logger.debug("{} operation completing execute normally", oc);
          return;
        }
        sendRequests();
      }
      catch (CoordinatorException e) {
        operationComplete.set(true);
        logger.debug("{} operation throwing CoordinatorException to complete execute ({}).", oc, e.getErrorCode());
        throw e;
      }
      catch (InterruptedException e) {
        operationComplete.set(true);
        logger.info("{} operation interrupted during execute.", oc);
        throw new CoordinatorException("Operation interrupted.", CoordinatorError.Interrupted);
      }
    }
  }

}



