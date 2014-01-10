package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.shared.BlockingChannelPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Performs a delete operation by sending and receiving delete requests until operation is complete or has failed.
 */
final public class DeleteOperation extends Operation {
  private Logger logger = LoggerFactory.getLogger(getClass());

  public DeleteOperation(String datacenterName,
                         BlockingChannelPool connectionPool,
                         ExecutorService requesterPool,
                         OperationContext oc,
                         BlobId blobId,
                         long operationTimeoutMs) throws CoordinatorException {
    super(datacenterName,
          connectionPool,
          requesterPool,
          oc,
          blobId,
          operationTimeoutMs,
          new AllInParallelOperationPolicy(datacenterName, blobId.getPartition()));
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    DeleteRequest deleteRequest = new DeleteRequest(context.getCorrelationId(), context.getClientId(), blobId);
    return new DeleteOperationRequest(connectionPool, responseQueue, context, blobId, replicaId, deleteRequest);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws
          CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
      case Blob_Deleted:
        return true;
      // Cannot delete if blob is not found
      case Blob_Not_Found:
        return false;
      case Blob_Expired:
      default:
        CoordinatorException e = new CoordinatorException("Server returned unexpected error for DeleteOperation.",
                                                          CoordinatorError.UnexpectedInternalError);
        logger.error("{} DeleteResponse for BlobId {} received from ReplicaId {} had unexpected error code {}: {}",
                     context, blobId, replicaId, serverErrorCode, e);
        throw e;
    }
  }
}

final class DeleteOperationRequest extends OperationRequest {
  protected DeleteOperationRequest(BlockingChannelPool connectionPool,
                                   BlockingQueue<OperationResponse> responseQueue,
                                   OperationContext context,
                                   BlobId blobId,
                                   ReplicaId replicaId,
                                   RequestOrResponse request) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
  }

  protected Response getResponse(DataInputStream dataInputStream) throws IOException {
    return DeleteResponse.readFrom(dataInputStream);
  }
}


