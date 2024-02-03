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
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
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
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Replicates from server to VCR
 */
public class VcrReplicaThread extends ReplicaThread {
  private static final Logger logger = LoggerFactory.getLogger(VcrReplicaThread.class);
  protected VcrReplicationManager replicationManager;
  protected AzureMetrics azureMetrics;

  protected AzureCloudConfig azureCloudConfig;
  protected VerifiableProperties properties;
  protected CloudDestination cloudDestination;

  public VcrReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      NotificationSystem notification, StoreKeyConverter storeKeyConverter, Transformer transformer,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, ReplicaTokenPersistor tokenWriter,
      CloudDestination cloudDestination, VerifiableProperties properties) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient,
        new ReplicationConfig(properties),
        new ReplicationMetrics(clusterMap.getMetricRegistry(), Collections.emptyList()), notification,
        storeKeyConverter, transformer, clusterMap.getMetricRegistry(), replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    this.cloudDestination = cloudDestination;
    this.properties = properties;
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.azureMetrics = new AzureMetrics(clusterMap.getMetricRegistry());
  }

  /**
   * Inserts a row in Azure Table for each message
   * @param messageInfoList List of replicated messages
   * @param remoteReplicaInfo Remote host info
   */
  @Override
  protected void logToExternalTable(List<MessageInfo> messageInfoList, RemoteReplicaInfo remoteReplicaInfo) {
    // Table entity = Table row
    // =========================================
    // | partition-key | row-key | replicaPath |
    // =========================================
    // | blob-id-1     | host1   | replica1    |
    // | blob-id-1     | host2   | replica2    |
    // =========================================
    messageInfoList.forEach(messageInfo ->
        cloudDestination.createTableEntity(azureCloudConfig.azureTableNameCorruptBlobs,
            new TableEntity(messageInfo.getStoreKey().getID(), remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname())
                .addProperty("replicaPath", remoteReplicaInfo.getReplicaId().getReplicaPath())));
  }

  /**
   * Persists token to cloud in each replication cycle
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  @Override
  public void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    // The parent method sets in-memory token
    super.advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
    // Now persist the token in cloud
    StoreFindToken token = (StoreFindToken) remoteReplicaInfo.getToken();
    if (token == null) {
      azureMetrics.absTokenPersistFailureCount.inc();
      logger.error("Null token for replica {}", remoteReplicaInfo);
      return;
    }
    logger.trace("replica = {}, token = {}", remoteReplicaInfo, token);
    // Table entity = Table row
    // =============================================================================================================
    // | partition-key | row-key | tokenType | logSegment | offset | storeKey | replicatedUntilUTC   | binaryToken |
    // =============================================================================================================
    // | partition     | host1   | Journal   | 3_14       | 12     | AAAAZzz  | 2024_Feb_02_13_01_00 | AAASLKJDFX  |
    // | partition     | host2   | Index     | 2_12       | 32     | AAEWsXZ  | 2024_Feb_02_13_01_00 | AAAEWODSDS  |
    // =============================================================================================================
    long lastOpTime = remoteReplicaInfo.getReplicatedUntilUTC();
    String partitionKey = String.valueOf(remoteReplicaInfo.getReplicaId().getPartitionId().getId());
    String rowKey = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname();
    TableEntity entity = new TableEntity(partitionKey, rowKey)
        .addProperty(VcrReplicationManager.BACKUP_NODE, dataNodeId.getHostname())
        .addProperty(VcrReplicationManager.TOKEN_TYPE,
            token.getType().toString())
        .addProperty(VcrReplicationManager.LOG_SEGMENT,
            token.getOffset() == null ? "none" : token.getOffset().getName().toString())
        .addProperty(VcrReplicationManager.OFFSET,
            token.getOffset() == null ? "none" : token.getOffset().getOffset())
        .addProperty(VcrReplicationManager.STORE_KEY,
            token.getStoreKey() == null ? "none" : token.getStoreKey().getID())
        .addProperty(VcrReplicationManager.REPLICATED_UNITL_UTC,
            lastOpTime == Utils.Infinite_Time ? String.valueOf(Utils.Infinite_Time)
                : VcrReplicationManager.DATE_FORMAT.format(lastOpTime))
        .addProperty(VcrReplicationManager.BINARY_TOKEN,
            token.toBytes());
    cloudDestination.upsertTableEntity(azureCloudConfig.azureTableNameReplicaTokens, entity);
  }
}
