/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer.workflow;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.OperationRetryHandler;
import com.github.ambry.network.*;
import com.github.ambry.protocol.*;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class GetChunkDataWorkflow implements OperationRetryHandler.RetryableOperation {
  private final ConnectionPool connectionPool;
  private final FileCopyInfo fileCopyInfo;
  private final ClusterMap clusterMap;

  private static final Logger logger = LoggerFactory.getLogger(GetChunkDataWorkflow.class);

  public GetChunkDataWorkflow(
      @Nonnull ConnectionPool connectionPool,
      @Nonnull FileCopyInfo fileCopyInfo,
      @Nonnull ClusterMap clusterMap) {
    Objects.requireNonNull(connectionPool, "connectionPool param cannot be null");
    Objects.requireNonNull(fileCopyInfo, "fileCopyInfo param cannot be null");
    Objects.requireNonNull(clusterMap, "clusterMap param cannot be null");

    this.connectionPool = connectionPool;
    this.fileCopyInfo = fileCopyInfo;
    this.clusterMap = clusterMap;
  }

  @Override
  public FileCopyGetChunkResponse execute()
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    final FileCopyGetChunkRequest request = new FileCopyGetChunkRequest(
        FileCopyGetChunkRequest.FILE_CHUNK_REQUEST_VERSION_V_1, fileCopyInfo.getCorrelationId(),
        fileCopyInfo.getClientId(), fileCopyInfo.getSourceReplicaId().getPartitionId(), fileCopyInfo.getFileName(),
        fileCopyInfo.getStartOffset(), fileCopyInfo.getChunkLengthInBytes(), fileCopyInfo.isChunked());

    logger.info("Sending FileCopyGetChunkRequest: {}", request);
    long startTimeMs = System.currentTimeMillis();

    DataNodeId dataNodeId = fileCopyInfo.getTargetReplicaId().getDataNodeId();
    ConnectedChannel connectedChannel = connectionPool.checkOutConnection(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(), 40);

    ChannelOutput channelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetChunkResponse response = FileCopyGetChunkResponse.readFrom(channelOutput.getInputStream(), clusterMap);

    logger.info("Received FileCopyGetChunkResponse in {} ms", System.currentTimeMillis() - startTimeMs);
    return response;
  }
}
