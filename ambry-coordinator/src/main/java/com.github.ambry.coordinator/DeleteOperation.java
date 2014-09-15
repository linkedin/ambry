package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs a delete operation by sending and receiving delete requests until operation is complete or has failed.
 */
final public class DeleteOperation extends Operation {
  private Logger logger = LoggerFactory.getLogger(getClass());
  private int blobNotFoundCount;
  // Number of replicas in the partition. This is used to set threshold to determine blob not found (all replicas
  // must reply).
  private final int replicaIdCount;
  private static HashMap<CoordinatorError, Integer> precedenceLevels = new HashMap<CoordinatorError, Integer>();

  public DeleteOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
        new AllInParallelOperationPolicy(datacenterName, blobId.getPartition()));

    this.replicaIdCount = blobId.getPartition().getReplicaIds().size();
    this.blobNotFoundCount = 0;
  }

  static {
    precedenceLevels.put(CoordinatorError.BlobExpired, 1);
    precedenceLevels.put(CoordinatorError.AmbryUnavailable, 2);
    precedenceLevels.put(CoordinatorError.UnexpectedInternalError, 3);
    precedenceLevels.put(CoordinatorError.BlobDoesNotExist, 4);
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    DeleteRequest deleteRequest = new DeleteRequest(context.getCorrelationId(), context.getClientId(), blobId);
    return new DeleteOperationRequest(connectionPool, responseQueue, context, blobId, replicaId, deleteRequest);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode)
      throws CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
      case Blob_Deleted:
        return true;

      // Cannot delete if blob is not found
      case Blob_Not_Found:
        blobNotFoundCount++;
        if (blobNotFoundCount == replicaIdCount) {
          String message =
              context + "DeleteOperation : Blob not found : blobNotFoundCount == replicaIdCount == " + blobNotFoundCount + ".";
          logger.trace(message);
          throw new CoordinatorException(message, CoordinatorError.BlobDoesNotExist);
        }
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        return false;
      case Blob_Expired:
        logger.trace(context + "Server returned Blob Expired error for DeleteOperation");
        setCurrentError(CoordinatorError.BlobExpired);
        return false;
      case Disk_Unavailable:
        logger.trace(context + "Server returned Disk Unavailable error for DeleteOperation");
        setCurrentError(CoordinatorError.AmbryUnavailable);
        return false;
      case IO_Error:
        logger.trace(context + "Server returned IO error for DeleteOperation");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        return false;
      case Partition_Unknown:
        logger.trace(context + "Server returned Partition Unknown error for DeleteOperation");
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        return false;
      default:
        CoordinatorException e = new CoordinatorException("Server returned unexpected error for DeleteOperation.",
            CoordinatorError.UnexpectedInternalError);
        logger.error("{} DeleteResponse for BlobId {} received from ReplicaId {} had unexpected error code {}: {}",
            context, blobId, replicaId, serverErrorCode, e);
        throw e;
    }
  }

  @Override
  public Integer getPrecedenceLevel(CoordinatorError coordinatorError) {
    return precedenceLevels.get(coordinatorError);
  }
}

final class DeleteOperationRequest extends OperationRequest {
  protected DeleteOperationRequest(ConnectionPool connectionPool, BlockingQueue<OperationResponse> responseQueue,
      OperationContext context, BlobId blobId, ReplicaId replicaId, RequestOrResponse request) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
  }

  @Override
  protected void markRequest()
      throws CoordinatorException {
    context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId()).deleteBlobRequestRate.mark();
  }

  @Override
  protected void updateRequest(long durationInMs)
      throws CoordinatorException {
    context.getCoordinatorMetrics().
        getRequestMetrics(replicaId.getDataNodeId()).deleteBlobRequestLatencyInMs.update(durationInMs);
  }

  @Override
  protected Response getResponse(DataInputStream dataInputStream)
      throws IOException {
    return DeleteResponse.readFrom(dataInputStream);
  }
}


