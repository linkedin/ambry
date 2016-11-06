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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import java.util.Properties;


/**
 * {@link PutGetHelperFactory} for a Server. This factory can be used to fetch
 * {@link ConcurrencyTestTool.ServerPutGetHelper} on {@link #getPutGetHelper()}
 */
public class ServerPutGetHelperFactory implements PutGetHelperFactory {

  private final Properties properties;
  private final String hostName;
  private final int port;
  private final ClusterMap clusterMap;
  private final int maxBlobSize;
  private final int minBlobSize;
  private final boolean enableVerboseLogging;

  public ServerPutGetHelperFactory(Properties properties, String hostName, Integer port, ClusterMap clusterMap,
      Integer maxBlobSize, Integer minBlobSize, Boolean enableVerboseLogging) {
    this.properties = properties;
    this.hostName = hostName;
    this.port = port;
    this.clusterMap = clusterMap;
    this.maxBlobSize = maxBlobSize;
    this.minBlobSize = minBlobSize;
    this.enableVerboseLogging = enableVerboseLogging;
  }

  /**
   * {@inheritDoc}
   * @return an instance of {@link ConcurrencyTestTool.ServerPutGetHelper}
   * @throws Exception if creation of {@link com.github.ambry.network.BlockingChannelConnectionPool} fails
   */
  public ConcurrencyTestTool.ServerPutGetHelper getPutGetHelper()
      throws Exception {
    return new ConcurrencyTestTool.ServerPutGetHelper(properties, hostName, port, clusterMap, maxBlobSize, minBlobSize,
        enableVerboseLogging);
  }
}
