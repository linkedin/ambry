package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.TTLRequest;
import com.github.ambry.shared.TTLResponse;
import com.github.ambry.shared.ServerErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Performs a cancel TTL operation by sending and receiving cancel TTL requests until operation is complete or has
 * failed.
 */
final public class CancelTTLOperation extends Operation {
  private Logger logger = LoggerFactory.getLogger(getClass());

  public CancelTTLOperation(String datacenterName,
                            ConnectionPool connectionPool,
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
    TTLRequest ttlRequest = new TTLRequest(context.getCorrelationId(), context.getClientId(), blobId,
                                           BlobProperties.Infinite_TTL);
    return new CancelTTLOperationRequest(connectionPool,
                                         responseQueue,
                                         context,
                                         blobId,
                                         replicaId,
                                         ttlRequest);
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
        CoordinatorException e = new CoordinatorException("Server returned unexpected error for CancelTTLOperation.",
                                                          CoordinatorError.UnexpectedInternalError);
        logger.error("{} CancelTTLResponse for BlobId {} received from ReplicaId {} had unexpected error code {}: {}",
                     context, blobId, replicaId, serverErrorCode, e);
        throw e;
    }
  }
}

final class CancelTTLOperationRequest extends OperationRequest {
  protected CancelTTLOperationRequest(ConnectionPool connectionPool,
                                      BlockingQueue<OperationResponse> responseQueue,
                                      OperationContext context,
                                      BlobId blobId,
                                      ReplicaId replicaId,
                                      RequestOrResponse request) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
  }

  @Override
  protected Response getResponse(DataInputStream dataInputStream) throws IOException {
    return TTLResponse.readFrom(dataInputStream);
  }
}

