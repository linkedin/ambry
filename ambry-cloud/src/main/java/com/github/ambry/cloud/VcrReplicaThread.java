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

import com.azure.data.tables.models.TableEntity;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;


/**
 * Replicates from server to VCR
 */
public class VcrReplicaThread extends ReplicaThread {

  protected AzureCloudConfig azureCloudConfig;
  protected VerifiableProperties properties;
  protected CloudDestination cloudDestination;

  public VcrReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, ReplicaTokenPersistor tokenWriter,
      CloudDestination cloudDestination, VerifiableProperties properties) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    this.cloudDestination = cloudDestination;
    this.properties = properties;
    this.azureCloudConfig = new AzureCloudConfig(properties);
  }

  @Override
  protected void logToExternalTable(List<MessageInfo> messageInfoList, RemoteReplicaInfo remoteReplicaInfo) {
    for (MessageInfo messageInfo: messageInfoList) {
      TableEntity tableEntity = new TableEntity(messageInfo.getStoreKey().getID(),
          remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname());
      tableEntity.addProperty("replicaPath", remoteReplicaInfo.getReplicaId().getReplicaPath());
      cloudDestination.createTableEntity(azureCloudConfig.azureInvalidBlobTableName, tableEntity);
    }
  }
}
