package com.github.ambry.coordinator;

import com.github.ambry.commons.BlobId;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.RequestOrResponse;
import java.util.ArrayList;
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

  public GetBlobUserMetadataOperation(String datacenterName, ConnectionPool connectionPool,
      ExecutorService requesterPool, OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap,
      ArrayList<String> sslEnabledColos)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
        MessageFormatFlags.BlobUserMetadata, sslEnabledColos);
    this.userMetadata = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    if (sslEnabledColos.contains(replicaId.getDataNodeId().getDatacenterName())) {
      return new GetBlobUserMetadataOperationRequest(connectionPool, responseQueue, context, blobId, replicaId,
          makeGetRequest(), clusterMap, this, true);
    } else {
      return new GetBlobUserMetadataOperationRequest(connectionPool, responseQueue, context, blobId, replicaId,
          makeGetRequest(), clusterMap, this, false);
    }
  }

  public ByteBuffer getUserMetadata()
      throws CoordinatorException {
    if (userMetadata != null) {
      return userMetadata;
    }
    CoordinatorException e = new CoordinatorException("GetBlobUserMetadata has invalid return data.",
        CoordinatorError.UnexpectedInternalError);
    logger.error("userMetadata is null and should not be: {}", e);
    throw e;
  }

  public synchronized void setUserMetadata(ByteBuffer userMetadata) {
    if (this.userMetadata == null) {
      this.userMetadata = userMetadata;
    }
  }
}

final class GetBlobUserMetadataOperationRequest extends GetOperationRequest {
  private GetBlobUserMetadataOperation getBlobUserMetadataOperation;
  private Logger logger = LoggerFactory.getLogger(getClass());

  protected GetBlobUserMetadataOperationRequest(ConnectionPool connectionPool,
      BlockingQueue<OperationResponse> responseQueue, OperationContext context, BlobId blobId, ReplicaId replicaId,
      RequestOrResponse request, ClusterMap clusterMap, GetBlobUserMetadataOperation getBlobUserMetadataOperation,
      boolean sslEnabled) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap, sslEnabled);
    this.getBlobUserMetadataOperation = getBlobUserMetadataOperation;
    this.logger.trace("Created GetBlobUserMetadataOperationRequest for " + replicaId);
  }

  @Override
  protected void markRequest() {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.getBlobUserMetadataRequestRate.mark();
    }
  }

  @Override
  protected void updateRequest(long durationInMs) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.getBlobUserMetadataRequestLatencyInMs.update(durationInMs);
    }
  }

  @Override
  protected void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException {
    getBlobUserMetadataOperation.setUserMetadata(MessageFormatRecord.deserializeUserMetadata(inputStream));
  }
}

