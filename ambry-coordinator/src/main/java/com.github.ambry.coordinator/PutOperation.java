package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Performs a put operation by sending and receiving put requests until operation is complete or has failed.
 * <p/>
 * Note that a put operation that partially completes may eventually "complete" in the background. However, the BlobId
 * is never returned to the caller and so the blob will never be retrieved or deleted. This could "leak" space and, in
 * the future, lead to customers being "billed" for space consumption of a blob they don't know they stored.
 */
final public class PutOperation extends Operation {
  private final BlobProperties blobProperties;
  private final ByteBuffer userMetadata;
  private final ByteBufferInputStream materializedBlobStream;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public PutOperation(String datacenterName,
                      ConnectionPool connectionPool,
                      ExecutorService requesterPool,
                      OperationContext oc,
                      BlobId blobId,
                      long operationTimeoutMs,
                      BlobProperties blobProperties,
                      ByteBuffer userMetadata,
                      InputStream blobStream,
                      int connectionPoolCheckoutTimeout) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
          new PutPolicy(datacenterName, blobId.getPartition()));
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;

    try {
      this.materializedBlobStream = new ByteBufferInputStream(blobStream, (int)blobProperties.getBlobSize());
    }
    catch (IOException e) {
      CoordinatorException ce = new CoordinatorException("Error processing blob passed into PutOperation.", e,
                                                         CoordinatorError.UnexpectedInternalError);
      logger.error("Could not materialize blob ", ce);
      throw ce;
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    PutRequest putRequest = new PutRequest(context.getCorrelationId(),
                                           context.getClientId(),
                                           blobId,
                                           blobProperties,
                                           userMetadata.duplicate(),
                                           materializedBlobStream.duplicate());
    return new PutOperationRequest(connectionPool,
                                   responseQueue,
                                   context,
                                   blobId,
                                   replicaId,
                                   putRequest);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws
          CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
        return true;
      case IO_Error:
        return false;
      case Blob_Already_Exists:
        CoordinatorException e = new CoordinatorException("BlobId already exists.",
                                                          CoordinatorError.UnexpectedInternalError);
        logger.error(context + " Put issued to BlobId " + blobId + " that already exists on ReplicaId " + replicaId, e);
        throw e;
      /*
      case Replica_Not_Writable:
        logger.error("{} Put issued to BlobId {} on read only partition (ReplicaId {} responded with {}).",
                     context, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Attempt to write to read-only partition.",
        CoordinatorError.UnexpectedInternalError);
       */
      default:
        e = new CoordinatorException("Server returned unexpected error for PutOperation.",
                                     CoordinatorError.UnexpectedInternalError);
        logger.error(context + " PutResponse for BlobId " +
                     blobId + " received from ReplicaId " +
                     replicaId + " had unexpected error code " + serverErrorCode, e);
        throw e;
    }
  }
}

final class PutOperationRequest extends OperationRequest {
  protected PutOperationRequest(ConnectionPool connectionPool,
                                BlockingQueue<OperationResponse> responseQueue,
                                OperationContext context,
                                BlobId blobId,
                                ReplicaId replicaId,
                                RequestOrResponse request) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
  }

  @Override
  protected Response getResponse(DataInputStream dataInputStream) throws IOException {
    return PutResponse.readFrom(dataInputStream);
  }
}


