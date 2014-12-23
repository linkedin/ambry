package com.github.ambry.coordinator;

import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.Response;
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
        new AllInParallelOperationPolicy(datacenterName, blobId.getPartition(), oc));

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
  protected ServerErrorCode processResponseError(ReplicaId replicaId, Response response)
      throws CoordinatorException {
    ServerErrorCode errorCodeToReturn = ServerErrorCode.No_Error;
    switch (response.getError()) {
      case No_Error:
      case Blob_Deleted:
        errorCodeToReturn = ServerErrorCode.No_Error;
        break;

      // Cannot delete if blob is not found
      case Blob_Not_Found:
        blobNotFoundCount++;
        if (blobNotFoundCount == replicaIdCount) {
          String message = context + " DeleteOperation : Blob not found : blobNotFoundCount == replicaIdCount == "
              + blobNotFoundCount + ".";
          logger.trace(message);
          throw new CoordinatorException(message, CoordinatorError.BlobDoesNotExist);
        }
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        errorCodeToReturn = ServerErrorCode.Blob_Not_Found;
        break;
      case Blob_Expired:
        logger.trace(context + " Server returned Blob Expired error for DeleteOperation");
        setCurrentError(CoordinatorError.BlobExpired);
        errorCodeToReturn = ServerErrorCode.Blob_Expired;
        break;
      case Disk_Unavailable:
        logger.trace(context + " Server returned Disk Unavailable error for DeleteOperation");
        setCurrentError(CoordinatorError.AmbryUnavailable);
        errorCodeToReturn = ServerErrorCode.Disk_Unavailable;
        break;
      case IO_Error:
        logger.trace(context + " Server returned IO error for DeleteOperation");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        errorCodeToReturn = ServerErrorCode.IO_Error;
        break;
      case Partition_Unknown:
        logger.trace(context + " Server returned Partition Unknown error for DeleteOperation");
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        errorCodeToReturn = ServerErrorCode.Partition_Unknown;
        break;
      default:
        CoordinatorException e = new CoordinatorException("Server returned unexpected error for DeleteOperation.",
            CoordinatorError.UnexpectedInternalError);
        logger.error("{} DeleteResponse for BlobId {} received from ReplicaId {} had unexpected error code {}: {}",
            context, blobId, replicaId, response.getError(), e);
        throw e;
    }
    return errorCodeToReturn;
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

