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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandlerConfig;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Base class for all retryable workflows.
 */
public class BaseWorkFlow {
  /**
   * The {@link OperationRetryHandler} to handle retries for operations that can be retried.
   */
  private final OperationRetryHandler operationRetryHandler;

  /**
   * The {@link ConnectionPool} to use to get connections to the target replica.
   */
  private final ConnectionPool connectionPool;

  /**
   * The {@link FileCopyHandlerConfig} that contains the configuration required to send the request.
   */
  private final FileCopyBasedReplicationConfig config;

  /**
   * Constructor to create BaseWorkFlow
   * @param connectionPool The connection pool to use to get connections to the target replica.
   * @param config The {@link FileCopyHandlerConfig} that contains the configuration required to send the request.
   */
  public BaseWorkFlow(@Nonnull ConnectionPool connectionPool, @Nonnull FileCopyBasedReplicationConfig config) {
    Objects.requireNonNull(connectionPool, "connectionPool param cannot be null");
    Objects.requireNonNull(config, "config param cannot be null");

    this.operationRetryHandler = new OperationRetryHandler(config);
    this.connectionPool = connectionPool;
    this.config = config;
  }

  /**
   * Get a connection to the target replica.
   * @param dataNodeId The {@link DataNodeId} that contains the information required to get the connection.
   * @return The {@link ConnectedChannel} to the target replica.
   * @throws ConnectionPoolTimeoutException
   * @throws IOException
   * @throws InterruptedException
   */
  public ConnectedChannel getChannel(@Nonnull DataNodeId dataNodeId) throws Exception {
    Objects.requireNonNull(dataNodeId, "dataNodeId param cannot be null");

    ConnectedChannel connectedChannel = operationRetryHandler.executeWithRetry(
        () -> connectionPool.checkOutConnection(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(),
            config.fileCopyHandlerConnectionTimeoutMs), "CheckOutConnection");
    if (null == connectedChannel) {
      throw new IllegalStateException("Failed to get a connection to " + dataNodeId.getHostname());
    }
    return connectedChannel;
  }
}
