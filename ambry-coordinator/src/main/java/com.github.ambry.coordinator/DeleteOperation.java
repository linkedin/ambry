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
import java.util.concurrent.ExecutorService;

/**
 * Performs a delete operation by sending and receiving delete requests until operation is complete or has failed.
 */
public class DeleteOperation extends Operation {
  private Logger logger = LoggerFactory.getLogger(getClass());

  public DeleteOperation(String datacenterName, BlockingChannelPool connectionPool, ExecutorService requesterPool,
                         OperationContext oc, BlobId blobId, long operationTimeoutMs) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, new DeletePolicy(datacenterName, blobId.getPartition()));
  }

  protected class DeleteOperationRequest extends OperationRequest {
    protected DeleteOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    protected Response getResponse(DataInputStream dataInputStream) throws IOException {
      return DeleteResponse.readFrom(dataInputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    DeleteRequest deleteRequest = new DeleteRequest(oc.getCorrelationId(), oc.getClientId(), blobId);
    return new DeleteOperationRequest(replicaId, deleteRequest);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
      case Blob_Deleted:
        return true;
      // Cannot delete if blob is not found
      case Blob_Not_Found:
        return false;
      // case Blob_Expired: should be possible to delete an expired blob.
      default:
        logger.error("{} DeleteResponse for BlobId {} received from ReplicaId {} had unexpected error code {}",
                     oc, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Unexpected server error code in DeleteResponse.",
                                       CoordinatorError.UnexpectedInternalError);
    }
  }
}

