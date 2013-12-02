package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.utils.BlockingChannelPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
public class GetBlobUserMetadataOperation extends GetOperation {
  private ByteBuffer userMetadata;

  public GetBlobUserMetadataOperation(String datacenterName, BlockingChannelPool connectionPool,
                                      ExecutorService requesterPool, OperationContext oc, BlobId blobId,
                                      long operationTimeoutMs, ClusterMap clusterMap) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
          MessageFormatFlags.BlobUserMetadata);
    this.userMetadata = null;
  }

  protected class GetBlobUserMetadataOperationRequest extends GetOperationRequest {
    protected GetBlobUserMetadataOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    @Override
    protected void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException {
      userMetadata = MessageFormat.deserializeUserMetadata(inputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobUserMetadataOperationRequest(replicaId, makeGetRequest());
  }

  public ByteBuffer getUserMetadata() throws CoordinatorException {
    if (userMetadata != null) {
      return userMetadata;
    }
    throw new CoordinatorException("usermetadata is null.", CoordinatorError.UnexpectedInternalError);
  }
}

