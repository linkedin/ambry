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

import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.FileCopyDataVerificationRequest;
import com.github.ambry.protocol.FileCopyDataVerificationResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import java.util.ArrayList;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for sending a FileCopyGetMetaDataRequest to the target replica and receiving the response.
 */
public class GetDataVerificationWorkflow extends BaseWorkFlow implements OperationRetryHandler.RetryableOperation<FileCopyDataVerificationResponse> {
  /**
   * The {@link FileCopyInfo} that contains the information required to send the request.
   */
  private final FileCopyInfo fileCopyInfo;

  public static String GET_DATA_VERIFICATION_OPERATION_NAME = "   * GetDataVerificationWorkflow";

  private static final Logger logger = LoggerFactory.getLogger(GetDataVerificationWorkflow.class);

  /**
   * GetDataVerificationWorkflow ctor
   * @param connectionPool The connection pool to use to get connections to the target replica.
   * @param config The {@link FileCopyBasedReplicationConfig} that contains the configuration required to send the request.
   */
  public GetDataVerificationWorkflow(@Nonnull ConnectionPool connectionPool, @Nonnull FileCopyInfo fileCopyInfo,
      @Nonnull FileCopyBasedReplicationConfig config) {
    super(connectionPool, config);

    Objects.requireNonNull(connectionPool, "connectionPool param cannot be null");
    Objects.requireNonNull(fileCopyInfo, "fileCopyInfo param cannot be null");
    Objects.requireNonNull(config, "config param cannot be null");

    this.fileCopyInfo = fileCopyInfo;
  }

  /**
   * The execute method can be used with {@link OperationRetryHandler#executeWithRetry} to send the request to the
   * target replica and honor a retry policy.
   * @return The response received from the target replica of type {@link FileCopyGetMetaDataResponse}.
   */
  @Override
  public FileCopyDataVerificationResponse execute() throws Exception {
    final FileCopyDataVerificationRequest request = new FileCopyDataVerificationRequest(
        FileCopyDataVerificationRequest.FILE_COPY_DATA_VERIFICATION_REQUEST_VERSION_V_1, fileCopyInfo.getCorrelationId(),
        fileCopyInfo.getClientId(), fileCopyInfo.getSourceReplicaId().getPartitionId(),
        "", new ArrayList<>());

    ConnectedChannel connectedChannel = getChannel(fileCopyInfo.getTargetReplicaId().getDataNodeId());

    logger.info("Sending FileCopyDataVerificationRequest: {}", request);
    long startTimeMs = System.currentTimeMillis();
    ChannelOutput channelOutput = connectedChannel.sendAndReceive(request);
    FileCopyDataVerificationResponse response = FileCopyDataVerificationResponse.readFrom(channelOutput.getInputStream());
    logger.info("Received FileCopyDataVerificationRequest in {} ms", System.currentTimeMillis() - startTimeMs);

    return response;
  }
}

