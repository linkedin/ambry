/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.RequestOrResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs a get blob data operation by sending and receiving get requests until operation is complete or has failed.
 */
final public class GetBlobUserMetadataOperation extends GetOperation {
  private ByteBuffer userMetadata;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobUserMetadataOperation(String datacenterName, ConnectionPool connectionPool,
      ExecutorService requesterPool, OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
        MessageFormatFlags.BlobUserMetadata);
    logger.trace(
        "GetBlobUserMetadata: Datacenter name " + datacenterName + " Connection pool " + (connectionPool != null)
            + ", requester pool " + (requesterPool != null) + ", Operation context " + oc + ", blobId " + blobId
            + ", timeout " + operationTimeoutMs + ", clustermap " + (clusterMap != null));
    this.userMetadata = null;
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    return new GetBlobUserMetadataOperationRequest(connectionPool, responseQueue, context, blobId, replicaId,
        makeGetRequest(), clusterMap, this);
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
      RequestOrResponse request, ClusterMap clusterMap, GetBlobUserMetadataOperation getBlobUserMetadataOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobUserMetadataOperation = getBlobUserMetadataOperation;
    this.logger.trace("Created GetBlobUserMetadataOperationRequest for " + replicaId);
  }

  @Override
  protected void markRequest() {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.incrementGetBlobUserMetadataRequestRate(sslEnabled);
    }
  }

  @Override
  protected void updateRequest(long durationInMs) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.updateGetBlobUserMetadataRequestLatency(durationInMs, sslEnabled);
    }
  }

  @Override
  protected void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException {
    getBlobUserMetadataOperation.setUserMetadata(MessageFormatRecord.deserializeUserMetadata(inputStream));
  }
}

