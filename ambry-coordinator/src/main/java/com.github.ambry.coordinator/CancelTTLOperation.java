package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.shared.TTLRequest;
import com.github.ambry.shared.TTLResponse;
import com.github.ambry.utils.BlockingChannelPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Performs a cancel TTL operation by sending and receiving cancel TTL requests until operation is complete or has
 * failed.
 */
public class CancelTTLOperation extends Operation {
  private Logger logger = LoggerFactory.getLogger(getClass());

  public CancelTTLOperation(String datacenterName, BlockingChannelPool connectionPool, ExecutorService requesterPool,
                            OperationContext oc, BlobId blobId, long operationTimeoutMs) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
          new CancelTTLPolicy(datacenterName, blobId.getPartition()));
  }

  protected class CancelTTLOperationRequest extends OperationRequest {
    protected CancelTTLOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    protected Response getResponse(DataInputStream dataInputStream) throws IOException {
      return TTLResponse.readFrom(dataInputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    TTLRequest ttlRequest = new TTLRequest(oc.getCorrelationId(), oc.getClientId(), blobId, BlobProperties.Infinite_TTL);
    return new CancelTTLOperationRequest(replicaId, ttlRequest);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws
          CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
        return true;
      // Cannot cancelTTL if blob is not found, already expired, or deleted.
      case Blob_Not_Found:
      case Blob_Expired:
      case Blob_Deleted:
        return false;
      default:
        logger.error("{} CancelTTLResponse for BlobId {} received from ReplicaId {} had unexpected error code {}",
                     oc, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Unexpected server error code in CancelTTLResponse.",
                                       CoordinatorError.UnexpectedInternalError);
    }
  }
}
