package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.BlockingChannelPool;
import com.github.ambry.utils.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * Performs a put operation by sending and receiving put requests until operation is complete or has failed.
 */
public class PutOperation extends Operation {
  private final BlobProperties blobProperties;
  private final ByteBuffer userMetadata;
  private final ByteBufferInputStream materializedBlobStream;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public PutOperation(String datacenterName, BlockingChannelPool connectionPool, ExecutorService requesterPool,
                      OperationContext oc, BlobId blobId, long operationTimeoutMs,
                      BlobProperties blobProperties, ByteBuffer userMetadata,
                      InputStream blobStream) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, new PutPolicy(datacenterName, blobId.getPartition()));
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;

    try {
      this.materializedBlobStream = new ByteBufferInputStream(blobStream, (int)blobProperties.getBlobSize());
    }
    catch (IOException e) {
      throw new CoordinatorException("Could not materialize input blob data.",
                                     CoordinatorError.UnexpectedInternalError);
    }
  }

  protected class PutOperationRequest extends OperationRequest {
    protected PutOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    protected Response getResponse(DataInputStream dataInputStream) throws IOException {
      return PutResponse.readFrom(dataInputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    PutRequest putRequest = new PutRequest(oc.getCorrelationId(), oc.getClientId(), blobId, blobProperties, userMetadata,
                                           materializedBlobStream.duplicate());

    return new PutOperationRequest(replicaId, putRequest);
  }


  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
        return true;
      case IO_Error:
        return false;
      case Blob_Already_Exists:
        logger.error("{} Put issued to BlobId {} that already exists on ReplicaId {}.",
                     oc, blobId, replicaId);
        throw new CoordinatorException("BlobId already exists.", CoordinatorError.UnexpectedInternalError);
      /*
      case Replica_Not_Writable:
        logger.error("{} Put issued to BlobId {} on read only partition (ReplicaId {} responded with {}).",
                     oc, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Attempt to write to read-only partition.", CoordinatorError.UnexpectedInternalError);
       */
      default:
        logger.error("{} PutResponse for BlobId {} received from ReplicaId {} had unexpected error code {}",
                     oc, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Unexpected server error code in PutResponse.",
                                       CoordinatorError.UnexpectedInternalError);
    }
  }
}
