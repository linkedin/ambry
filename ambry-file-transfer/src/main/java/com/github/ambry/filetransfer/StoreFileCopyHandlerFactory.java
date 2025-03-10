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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.server.StoreManager;


public class StoreFileCopyHandlerFactory implements FileCopyHandlerFactory {
  private final ConnectionPool connectionPool;
  private final StoreManager storeManager;
  private final ClusterMap clusterMap;

  public StoreFileCopyHandlerFactory(ConnectionPool connectionPool, StoreManager storeManager, ClusterMap clusterMap)
      throws Exception {
    this.connectionPool = connectionPool;
    this.clusterMap = clusterMap;
    this.storeManager = storeManager;
  }

  @Override
  public FileCopyHandler getFileCopyHandler() {
    return new StoreFileCopyHandler(connectionPool, storeManager, clusterMap);
  }
}
