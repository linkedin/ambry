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
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.FileChunkInfo;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.network.*;
import com.github.ambry.protocol.*;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;


/**
 * This class is responsible for sending a FileCopyGetChunkRequest to the target replica and receiving the response.
 */
public class GetChunkDataWorkflow extends BaseWorkFlow implements OperationRetryHandler.RetryableOperation<FileCopyGetChunkResponse> {
  /**
   * The {@link FileCopyInfo} that contains the information required to send the request.
   */
  private final FileCopyInfo fileCopyInfo;

  /**
   * The {@link FileChunkInfo} that contains the file-chunk information required to send the request.
   */
  private final FileChunkInfo fileChunkInfo;

  /**
   * The snapshot id of the partition on serving node.
   * This is returned as part of {@link FileCopyGetMetaDataResponse} and is used to ensure that a bootstrapping request
   * doesn't clash with an ongoing compaction.
   */
  private final String snapshotId;

  /**
   * The {@link ClusterMap} that contains the information about the cluster.
   */
  private final ClusterMap clusterMap;

  public static String GET_CHUNK_OPERATION_NAME = "GetChunkOperation";

  public static String GET_FILE_OPERATION_NAME = "GetFileOperation";

  private static final Logger logger = LoggerFactory.getLogger(GetChunkDataWorkflow.class);

  /**
   * GetChunkDataWorkflow ctor
   * @param connectionPool The connection pool to use to get connections to the target replica.
   * @param fileCopyInfo The {@link FileCopyInfo} that contains the information required to send the request.
   * @param fileChunkInfo The {@link FileChunkInfo} that contains the file-chunk information required to send the request.
   * @param snapshotId The snapshot id of the partition on serving node.
   * @param clusterMap The {@link ClusterMap} that contains the information about the cluster.
   * @param config The {@link FileCopyHandlerConfig} that contains the configuration required to send the request.
   */
  public GetChunkDataWorkflow(
      @Nonnull ConnectionPool connectionPool,
      @Nonnull FileCopyInfo fileCopyInfo,
      @Nonnull FileChunkInfo fileChunkInfo,
      @Nonnull String snapshotId,
      @Nonnull ClusterMap clusterMap,
      @Nonnull FileCopyBasedReplicationConfig config) {
    super(connectionPool, config);

    Objects.requireNonNull(connectionPool, "connectionPool param cannot be null");
    Objects.requireNonNull(fileCopyInfo, "fileCopyInfo param cannot be null");
    Objects.requireNonNull(fileChunkInfo, "fileChunkInfo param cannot be null");
    Objects.requireNonNull(snapshotId, "snapshotId param cannot be null");
    Objects.requireNonNull(clusterMap, "clusterMap param cannot be null");
    Objects.requireNonNull(config, "config param cannot be null");

    this.fileCopyInfo = fileCopyInfo;
    this.fileChunkInfo = fileChunkInfo;
    this.snapshotId = snapshotId;
    this.clusterMap = clusterMap;
  }

  /**
   * The execute method can be used with {@link OperationRetryHandler#executeWithRetry} to send the request to the
   * target replica and honor a retry policy.
   * @return The response received from the target replica of type {@link FileCopyGetChunkResponse}.
   * @throws IOException
   * @throws ConnectionPoolTimeoutException
   * @throws InterruptedException
   */
  @Override
  public FileCopyGetChunkResponse execute() throws Exception {
    final FileCopyGetChunkRequest request = new FileCopyGetChunkRequest(
        FileCopyGetChunkRequest.FILE_CHUNK_REQUEST_VERSION_V_1, fileCopyInfo.getCorrelationId(),
        fileCopyInfo.getClientId(), fileCopyInfo.getSourceReplicaId().getPartitionId(), fileChunkInfo.getFileName(),
        fileCopyInfo.getSourceReplicaId().getDataNodeId().getHostname(), snapshotId, fileChunkInfo.getStartOffset(),
        fileChunkInfo.getChunkLengthInBytes(), fileChunkInfo.isChunked());

    logger.info("Sending FileCopyGetChunkRequest: {}", request);
    long startTimeMs = System.currentTimeMillis();

    ConnectedChannel connectedChannel = getChannel(fileCopyInfo.getTargetReplicaId().getDataNodeId());

    ChannelOutput channelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetChunkResponse response = FileCopyGetChunkResponse.readFrom(channelOutput.getInputStream(), clusterMap);

    logger.info("Received FileCopyGetChunkResponse in {} ms", System.currentTimeMillis() - startTimeMs);
    return response;
  }
}
