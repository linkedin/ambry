package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.RequestOrResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;


/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
final public class GetBlobPropertiesOperation extends GetOperation {
  private BlobProperties blobProperties;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobPropertiesOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
        MessageFormatFlags.BlobProperties);
    this.blobProperties = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobPropertiesOperationRequest(connectionPool, responseQueue, context, blobId, replicaId,
        makeGetRequest(), clusterMap, this);
  }

  public BlobProperties getBlobProperties()
      throws CoordinatorException {
    if (blobProperties != null) {
      return blobProperties;
    }
    CoordinatorException e = new CoordinatorException("GetBlobProperties has invalid return data.",
        CoordinatorError.UnexpectedInternalError);
    logger.error("blobProperties is null and should not be: {}", e);
    throw e;
  }

  public synchronized void setBlobProperties(BlobProperties blobProperties) {
    if (this.blobProperties == null) {
      this.blobProperties = blobProperties;
    }
  }
}

final class GetBlobPropertiesOperationRequest extends GetOperationRequest {
  private GetBlobPropertiesOperation getBlobPropertiesOperation;

  protected GetBlobPropertiesOperationRequest(ConnectionPool connectionPool,
      BlockingQueue<OperationResponse> responseQueue, OperationContext context, BlobId blobId, ReplicaId replicaId,
      RequestOrResponse request, ClusterMap clusterMap, GetBlobPropertiesOperation getBlobPropertiesOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobPropertiesOperation = getBlobPropertiesOperation;
  }

  @Override
  protected void markRequest()
      throws CoordinatorException {
    context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId()).getBlobPropertiesRequestRate.mark();
  }

  @Override
  protected void updateRequest(long durationInMs)
      throws CoordinatorException {
    context.getCoordinatorMetrics().
        getRequestMetrics(replicaId.getDataNodeId()).getBlobPropertiesRequestLatencyInMs.update(durationInMs);
  }

  @Override
  protected void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException {
    getBlobPropertiesOperation.setBlobProperties(MessageFormatRecord.deserializeBlobProperties(inputStream));
  }
}

