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
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenFactory;
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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Replicates from server to VCR
 */
public class VcrReplicaThread extends ReplicaThread {
  private static final Logger logger = LoggerFactory.getLogger(VcrReplicaThread.class);
  protected CloudConfig vcrNodeConfig;
  protected ReplicaComparator comparator;
  protected String azureTableNameReplicaTokens;
  protected AzureMetrics azureMetrics;

  protected AzureCloudConfig azureCloudConfig;
  protected VerifiableProperties properties;
  protected CloudDestination cloudDestination;
  protected int numReplIter;

  public VcrReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      NotificationSystem notification, StoreKeyConverter storeKeyConverter, Transformer transformer,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin,
      CloudDestination cloudDestination, VerifiableProperties properties) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient,
        new ReplicationConfig(properties),
        new ReplicationMetrics(clusterMap.getMetricRegistry(), Collections.emptyList()), notification,
        storeKeyConverter, transformer, clusterMap.getMetricRegistry(), replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    this.cloudDestination = cloudDestination;
    this.properties = properties;
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.vcrNodeConfig = new CloudConfig(properties);
    this.azureTableNameReplicaTokens = this.azureCloudConfig.azureTableNameReplicaTokens;
    this.azureMetrics = new AzureMetrics(clusterMap.getMetricRegistry());
    this.numReplIter = 0;
    comparator = new ReplicaComparator();
  }

  class ReplicaComparator implements Comparator<RemoteReplicaInfo> {
    @Override
    public int compare(RemoteReplicaInfo r1, RemoteReplicaInfo r2) {
      String d1 = r1.getReplicaId().getDataNodeId().getHostname();
      String d2 = r2.getReplicaId().getDataNodeId().getHostname();
      return d1.compareTo(d2);
    }
  }

  @Override
  public void run() {
    try {
      int delay = new Random().nextInt(900) + 1;
      logger.info("Starting replica thread {} in {} seconds", Thread.currentThread().getName(), delay);
      Thread.sleep(TimeUnit.SECONDS.toMillis(delay));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    super.run();
  }

  /**
   * Fetch replica-token from Azure and set it in the thread
   * @param replicaInfo
   */
  void setReplicaToken(RemoteReplicaInfo replicaInfo) {
    String partitionKey = String.valueOf(replicaInfo.getReplicaId().getPartitionId().getId());
    String rowKey = replicaInfo.getReplicaId().getDataNodeId().getHostname();
    FindTokenFactory findTokenFactory = findTokenHelper.getFindTokenFactoryFromReplicaType(ReplicaType.DISK_BACKED);
    try {
      TableEntity row = cloudDestination.getTableEntity(azureTableNameReplicaTokens, partitionKey, rowKey);
      if (row == null) {
        logger.warn("Resetting token for replica {}/{} because no token found", partitionKey, rowKey);
        replicaInfo.setToken(findTokenFactory.getNewFindToken());
      } else {
        logger.info("Loading token for replica {}/{}", partitionKey, rowKey);
        DataInputStream inputStream = new DataInputStream(
            new ByteArrayInputStream((byte[]) row.getProperty(VcrReplicationManager.BINARY_TOKEN)));
        replicaInfo.setToken(findTokenFactory.getFindToken(inputStream));
      }
    } catch (Throwable t) {
      azureMetrics.replicaTokenReadErrorCount.inc();
      logger.error("Resetting token for replica {}/{} due to {}", partitionKey, rowKey, t.toString());
      replicaInfo.setToken(findTokenFactory.getNewFindToken());
    } // try-catch
  }

  /**
   * Selects replicas R1, R2, and R3 of a partition P in distinct iterations of the replication loop.
   * As the loop is continuous, each replica gets its turn.
   * Consequently, in the first iteration, R1 is processed, followed by R2, and then R3.
   * This approach ensures that missing blob B is uploaded from R1 to Azure during the first iteration.
   * Subsequent iterations involving R2 and R3 skip the fetch and upload step for blob B, as it is already present.
   *
   * There sure is a better algorithm to do this but the size of the input is enough for this crude code.
   * @param replicas A map of replicas {host -> {replicas}}
   */
  @Override
  public Map<DataNodeId, List<RemoteReplicaInfo>> selectReplicas(Map<DataNodeId, List<RemoteReplicaInfo>> replicas) {
    HashMap<Long, ArrayList<RemoteReplicaInfo>> partitions = new HashMap<>();
    // Group replicas by partition
    replicas.values().forEach(rlist -> rlist.forEach(replica -> partitions.computeIfAbsent(
        replica.getReplicaId().getPartitionId().getId(), k -> new ArrayList<>()).add(replica)));
    // Pick one replica per partition for this iteration
    Map<DataNodeId, List<RemoteReplicaInfo>> nodes = new HashMap<>();
    partitions.values().forEach(rlist -> {
      rlist.sort(comparator);
      RemoteReplicaInfo replica;
      switch (vcrNodeConfig.replicaSelectionPolicy) {
        case FIXED:
          replica = rlist.get(0);
          logger.trace("FIXED replicaSelectionPolicy picked {} for partition-{}",
              replica.getReplicaId().getDataNodeId().getHostname(), replica.getReplicaId().getPartitionId().getId());
          break;
        case ROUND_ROBIN:
        default:
          replica = rlist.get(numReplIter % rlist.size());
          logger.trace("{} replicaSelectionPolicy picked {} for partition-{}",
              vcrNodeConfig.DEFAULT_REPLICA_SELECTION_POLICY, replica.getReplicaId().getDataNodeId().getHostname(),
              replica.getReplicaId().getPartitionId().getId());
      }
      if (replica.getToken() == null) {
        setReplicaToken(replica);
      }
      // Group by data node
      nodes.computeIfAbsent(replica.getReplicaId().getDataNodeId(), k -> new ArrayList<>()).add(replica);
    });
    numReplIter = (numReplIter % 100) + 1; // Prevent integer overflow
    return nodes;
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

  protected boolean isTtlUpdateNeededAfterPut(MessageInfo messageInfo) {
    return messageInfo.isTtlUpdated() && messageInfo.getExpirationTimeInMs() != Utils.Infinite_Time;
  }

  /**
   * Persists token to cloud in each replication cycle
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  @Override
  public void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    StoreFindToken oldToken = (StoreFindToken) remoteReplicaInfo.getToken();
    // The parent method sets in-memory token
    super.advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
    StoreFindToken token = (StoreFindToken) remoteReplicaInfo.getToken();
    if (token == null) {
      azureMetrics.replicaTokenWriteErrorCount.inc();
      logger.error("Null token for replica {}", remoteReplicaInfo);
      return;
    }
    if (token.equals(oldToken)) {
      logger.trace("Skipping token upload due to unchanged token, oldToken = {}, newToken = {}", oldToken, token);
      return;
    }
    logger.trace("Uploading token = {} for replica = {}", token, remoteReplicaInfo);
    // Table entity = Table row
    // ======================================================================================
    // | partition-key | row-key | tokenType | logSegment | offset | storeKey | binaryToken |
    // ======================================================================================
    // | partition     | host1   | Journal   | 3_14       | 12     | none     | AAASLKJDFX  |
    // | partition     | host2   | Index     | 2_12       | 32     | AAEWsXZ  | AAAEWODSDS  |
    // ======================================================================================
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
        .addProperty(VcrReplicationManager.BINARY_TOKEN,
            token.toBytes());
    // Now persist the token in cloud
    if (cloudDestination.upsertTableEntity(azureTableNameReplicaTokens, entity)) {
      azureMetrics.replicaTokenWriteRate.mark();
    } else {
      azureMetrics.replicaTokenWriteErrorCount.inc();
    }
  }
}
