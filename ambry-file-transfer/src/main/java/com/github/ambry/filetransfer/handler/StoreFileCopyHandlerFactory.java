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
package com.github.ambry.filetransfer.handler;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.filetransfer.handler.StoreFileCopyHandler;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.server.StoreManager;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Factory class to create {@link StoreFileCopyHandler}
 */
public class StoreFileCopyHandlerFactory implements FileCopyHandlerFactory {
  /**
   * The connection pool to use for file copy
   */
  private final ConnectionPool connectionPool;

  /**
   * The store manager to use for file copy
   */
  private final StoreManager storeManager;

  /**
   * The cluster map to use for file copy
   */
  private final ClusterMap clusterMap;

  /**
   * The configuration for the file copy handler.
   */
  private final FileCopyBasedReplicationConfig config;

  public StoreFileCopyHandlerFactory(@Nonnull ConnectionPool connectionPool, @Nonnull StoreManager storeManager,
      @Nonnull ClusterMap clusterMap, @Nonnull FileCopyBasedReplicationConfig config) throws Exception {
    Objects.requireNonNull(connectionPool, "connectionPool cannot be null");
    Objects.requireNonNull(storeManager, "storeManager cannot be null");
    Objects.requireNonNull(clusterMap, "clusterMap cannot be null");
    Objects.requireNonNull(config, "config cannot be null");
    this.connectionPool = connectionPool;
    this.clusterMap = clusterMap;
    this.storeManager = storeManager;
    this.config = config;
  }

  @Override
  public FileCopyHandler getFileCopyHandler() {
    return new StoreFileCopyHandler(connectionPool, storeManager, clusterMap, config);
  }
}
