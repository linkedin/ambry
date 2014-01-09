package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.BlockingChannelPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
public class GetBlobPropertiesOperation extends GetOperation {
  private BlobProperties blobProperties;

  public GetBlobPropertiesOperation(String datacenterName, BlockingChannelPool connectionPool,
                                    ExecutorService requesterPool, OperationContext oc, BlobId blobId,
                                    long operationTimeoutMs, ClusterMap clusterMap) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
          MessageFormatFlags.BlobProperties);
    this.blobProperties = null;
  }

  protected class GetBlobPropertiesOperationRequest extends GetOperationRequest {
    protected GetBlobPropertiesOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    @Override
    protected void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException {
      blobProperties = MessageFormat.deserializeBlobProperties(inputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobPropertiesOperationRequest(replicaId, makeGetRequest());
  }

  public BlobProperties getBlobProperties() throws CoordinatorException {
    if (blobProperties != null) {
      return blobProperties;
    }
    throw new CoordinatorException("blobProperties is null.", CoordinatorError.UnexpectedInternalError);
  }
}

