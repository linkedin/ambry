package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobOutput;
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
final public class GetBlobOperation extends GetOperation {
  private BlobOutput blobOutput;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobOperation(String datacenterName,
                          ConnectionPool connectionPool,
                          ExecutorService requesterPool,
                          OperationContext oc,
                          BlobId blobId,
                          long operationTimeoutMs,
                          ClusterMap clusterMap) throws CoordinatorException {
    super(datacenterName,
          connectionPool,
          requesterPool,
          oc,
          blobId,
          operationTimeoutMs,
          clusterMap,
          MessageFormatFlags.Blob);
    this.blobOutput = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobOperationRequest(connectionPool,
                                       responseQueue,
                                       context,
                                       blobId,
                                       replicaId,
                                       makeGetRequest(),
                                       clusterMap,
                                       this);
  }

  public BlobOutput getBlobOutput() throws CoordinatorException {
    if (blobOutput != null) {
      return blobOutput;
    }
    CoordinatorException e = new CoordinatorException("GetBlobOperation has invalid return data.",
                                                      CoordinatorError.UnexpectedInternalError);
    logger.error("blobOutput is null and should not be: {}", e);
    throw e;
  }

  public synchronized void setBlobOutput(BlobOutput blobOutput) {
    if (this.blobOutput == null) {
      this.blobOutput = blobOutput;
    }
    else {
      logger.warn("{} BlobOutput attempted to be set after being set.", context);
    }
  }
}

final class GetBlobOperationRequest extends GetOperationRequest {
  private GetBlobOperation getBlobOperation;

  protected GetBlobOperationRequest(ConnectionPool connectionPool,
                                    BlockingQueue<OperationResponse> responseQueue,
                                    OperationContext context,
                                    BlobId blobId,
                                    ReplicaId replicaId,
                                    RequestOrResponse request,
                                    ClusterMap clusterMap,
                                    GetBlobOperation getBlobOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobOperation = getBlobOperation;
  }

  @Override
  protected void markRequest() throws CoordinatorException {
    context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId()).getBlobRequestRate.mark();
  }

  @Override
  protected void updateRequest(long durationInMs) throws CoordinatorException {
    context.getCoordinatorMetrics().
            getRequestMetrics(replicaId.getDataNodeId()).getBlobRequestLatencyInMs.update(durationInMs);
  }

  @Override
  protected void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException {
    getBlobOperation.setBlobOutput(MessageFormatRecord.deserializeBlob(inputStream));
  }
}

