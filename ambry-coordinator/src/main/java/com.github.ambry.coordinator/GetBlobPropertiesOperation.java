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
import com.github.ambry.messageformat.BlobProperties;
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
final public class GetBlobPropertiesOperation extends GetOperation {
  private BlobProperties blobProperties;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetBlobPropertiesOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, clusterMap,
        MessageFormatFlags.BlobProperties);
    logger.trace("GetBlobProperties: Datacenter name " + datacenterName + " Connection pool " + (connectionPool != null)
        + ", requester pool " + (requesterPool != null) + ", Operation context " + oc + ", blobId " + blobId
        + ", timeout " + operationTimeoutMs + ", clustermap " + (clusterMap != null));
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
  private Logger logger = LoggerFactory.getLogger(getClass());

  protected GetBlobPropertiesOperationRequest(ConnectionPool connectionPool,
      BlockingQueue<OperationResponse> responseQueue, OperationContext context, BlobId blobId, ReplicaId replicaId,
      RequestOrResponse request, ClusterMap clusterMap, GetBlobPropertiesOperation getBlobPropertiesOperation) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request, clusterMap);
    this.getBlobPropertiesOperation = getBlobPropertiesOperation;
    logger.trace("Created GetBlobPropertiesOperationRequest for " + replicaId);
  }

  @Override
  protected void markRequest() {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.incrementGetBlobPropertiesRequestRate(sslEnabled);
    }
  }

  @Override
  protected void updateRequest(long durationInMs) {
    CoordinatorMetrics.RequestMetrics metric =
        context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId());
    if (metric != null) {
      metric.updateGetBlobPropertiesRequestLatency(durationInMs, sslEnabled);
    }
  }

  @Override
  protected void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException {
    getBlobPropertiesOperation.setBlobProperties(MessageFormatRecord.deserializeBlobProperties(inputStream));
  }
}

