package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannelPool;
import com.github.ambry.shared.RequestOrResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
final public class GetBlobUserMetadataOperation extends GetOperation {
  private ByteBuffer userMetadata;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobUserMetadataOperation(String datacenterName, BlockingChannelPool connectionPool,
                                      ExecutorService requesterPool, OperationContext oc, BlobId blobId,
                                      long operationTimeoutMs, ClusterMap clusterMap) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
          MessageFormatFlags.BlobUserMetadata);
    this.userMetadata = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobUserMetadataOperationRequest(connectionPool, responseQueue, context, blobId, replicaId,
                                                   makeGetRequest(), clusterMap, this);
  }

  public ByteBuffer getUserMetadata() throws CoordinatorException {
    if (userMetadata != null) {
      return userMetadata;
    }
    logger.error("userMetadata is null and should not be.");
    throw new CoordinatorException("GetBlobUserMetadata has invalid return data.",
                                   CoordinatorError.UnexpectedInternalError);
  }

  public synchronized void setUserMetadata(ByteBuffer userMetadata) {
    if (this.userMetadata == null) {
      this.userMetadata = userMetadata;
    }
    else {
      logger.warn("{} BlobUserMetadata attempted to be set after being set.", context);
    }
  }
}

final class GetBlobUserMetadataOperationRequest extends GetOperationRequest {
  private GetBlobUserMetadataOperation getBlobUserMetadataOperation;

  protected GetBlobUserMetadataOperationRequest(BlockingChannelPool connectionPool,
                                                BlockingQueue<OperationResponse> responseQueue,
                                                OperationContext context, BlobId blobId, ReplicaId replicaId,
                                                RequestOrResponse request, ClusterMap clusterMap,
                                                GetBlobUserMetadataOperation getBlobUserMetadataOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobUserMetadataOperation = getBlobUserMetadataOperation;
  }

  @Override
  protected void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException {
    getBlobUserMetadataOperation.setUserMetadata(MessageFormat.deserializeUserMetadata(inputStream));
  }
}

