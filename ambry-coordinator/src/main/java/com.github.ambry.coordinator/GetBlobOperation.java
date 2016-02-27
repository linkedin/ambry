package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.RequestOrResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
final public class GetBlobOperation extends GetOperation {
  private BlobOutput blobOutput;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
        MessageFormatFlags.Blob);
    logger.trace("GetBlob: Datacenter name " + datacenterName + " Connection pool " + (connectionPool != null)
        + ", requester pool " + (requesterPool != null) + ", Operation context " + oc + ", blobId " + blobId
        + ", timeout " + operationTimeoutMs + ", clustermap " + (clusterMap != null));
    this.blobOutput = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobOperationRequest(connectionPool, responseQueue, context, blobId, replicaId, makeGetRequest(),
        clusterMap, this);
  }

  public BlobOutput getBlobOutput()
      throws CoordinatorException {
    if (blobOutput != null) {
      return blobOutput;
    }
    CoordinatorException e =
        new CoordinatorException("GetBlobOperation has invalid return data.", CoordinatorError.UnexpectedInternalError);
    logger.error("blobOutput is null and should not be: {}", e);
    throw e;
  }

  public synchronized void setBlobOutput(BlobOutput blobOutput) {
    if (this.blobOutput == null) {
      this.blobOutput = blobOutput;
    }
  }
}

final class GetBlobOperationRequest extends GetOperationRequest {
  private GetBlobOperation getBlobOperation;
  private Logger logger = LoggerFactory.getLogger(getClass());

  protected GetBlobOperationRequest(ConnectionPool connectionPool, BlockingQueue<OperationResponse> responseQueue,
      OperationContext context, BlobId blobId, ReplicaId replicaId, RequestOrResponse request, ClusterMap clusterMap,
      GetBlobOperation getBlobOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobOperation = getBlobOperation;
    logger.trace("Created GetBlobOperationRequest for " + replicaId);
  }

  @Override
  protected void markRequest() {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.incrementGetBlobRequestRate(sslEnabled);
    }
  }

  @Override
  protected void updateRequest(long durationInMs) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.updateGetBlobRequestLatency(durationInMs, sslEnabled);
    }
  }

  @Override
  protected void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException {
    BlobData blobData = MessageFormatRecord.deserializeBlob(inputStream);
    getBlobOperation.setBlobOutput(new BlobOutput(blobData.getSize(), blobData.getStream()));
  }
}

