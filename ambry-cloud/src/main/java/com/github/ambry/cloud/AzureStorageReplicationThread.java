/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;


/**
 * Replicates from server to VCR
 */
public class AzureStorageReplicationThread extends ReplicaThread {

  private final AzureStorageTokenWriter tokenWriter;

  public AzureStorageReplicationThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient, ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate, ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, ReplicaTokenPersistor tokenWriter) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    this.tokenWriter = (AzureStorageTokenWriter) tokenWriter;
  }

  /**
   * Advances token to make progress on replication and stores it in Azure
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  @Override
  protected void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ReplicaThread.ExchangeMetadataResponse exchangeMetadataResponse) {
    super.advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
    try {
      tokenWriter.persist(null, Collections.singletonList(new RemoteReplicaInfo.ReplicaTokenInfo(remoteReplicaInfo)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
