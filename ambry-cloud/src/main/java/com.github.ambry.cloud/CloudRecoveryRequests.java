/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.server.AmbryRequests;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request implementation class for Vcr. All requests to the vcr server are
 * handled by this class.
 */
public class CloudRecoveryRequests extends AmbryRequests {

  private Logger logger = LoggerFactory.getLogger(getClass());

  public CloudRecoveryRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel,
      ClusterMap clusterMap, DataNodeId currentNode, MetricRegistry registry, ServerMetrics serverMetrics,
      FindTokenHelper findTokenHelper, NotificationSystem notification, ReplicationEngine replicationEngine,
      StoreKeyFactory storageKeyFactory, boolean enableDataPrefetch,
      StoreKeyConverterFactory storeKeyConverterFactory) {
    super(storeManager, requestResponseChannel, clusterMap, currentNode, registry, serverMetrics, findTokenHelper,
        notification, replicationEngine, storageKeyFactory, enableDataPrefetch, storeKeyConverterFactory);
  }

  @Override
  public void handlePutRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  @Override
  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  @Override
  public void handleTtlUpdateRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  //todo fix this
  @Override
  protected ServerErrorCode validateRequest(PartitionId partition, RequestOrResponseType requestType,
      boolean skipPartitionAndDiskAvailableCheck) {
    return ServerErrorCode.No_Error;
  }

  @Override
  protected long getRemoteReplicaLag(Store store, long totalBytesRead) {
    return 1024 * 1024; // todo: fix this
  }
}
