package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.utils.BlockingChannelPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
public class GetBlobOperation extends GetOperation {
  private BlobOutput blobOutput;

  public GetBlobOperation(String datacenterName, BlockingChannelPool connectionPool,
                          ExecutorService requesterPool, OperationContext oc, BlobId blobId,
                          long operationTimeoutMs, ClusterMap clusterMap) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
          MessageFormatFlags.Blob);
    this.blobOutput = null;
  }

  protected class GetBlobOperationRequest extends GetOperationRequest {
    protected GetBlobOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    @Override
    protected void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException {
      blobOutput = MessageFormat.deserializeData(inputStream);
    }
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobOperationRequest(replicaId, makeGetRequest());
  }

  public BlobOutput getBlobOutput() throws CoordinatorException {
    if (blobOutput != null) {
      return blobOutput;
    }
    throw new CoordinatorException("blobOutput is null.", CoordinatorError.UnexpectedInternalError);
  }
}
