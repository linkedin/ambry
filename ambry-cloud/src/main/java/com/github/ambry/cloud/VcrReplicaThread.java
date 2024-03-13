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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Replicates from server to VCR
 */
public class VcrReplicaThread extends ReplicaThread {
  private static final Logger logger = LoggerFactory.getLogger(VcrReplicaThread.class);
  protected String azureTableNameReplicaTokens;
  protected AzureMetrics azureMetrics;

  protected AzureCloudConfig azureCloudConfig;
  protected VerifiableProperties properties;
  protected CloudDestination cloudDestination;
  protected HashMap<String, List<Pair<RemoteReplicaInfo, MessageInfo>>> blobToReplicas;
  protected HashMap<RemoteReplicaInfo, HashSet<MessageInfo>> replicaToMessages;

  class LexicographicComparator implements Comparator<Pair<RemoteReplicaInfo, MessageInfo>> {
    @Override
    public int compare(Pair<RemoteReplicaInfo, MessageInfo> a, Pair<RemoteReplicaInfo, MessageInfo> b) {
      String r1 = a.getFirst().getReplicaId().getDataNodeId().toString();
      String r2 = b.getFirst().getReplicaId().getDataNodeId().toString();
      return r1.compareTo(r2);
    }
  }

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
    this.azureTableNameReplicaTokens = this.azureCloudConfig.azureTableNameReplicaTokens;
    this.azureMetrics = new AzureMetrics(clusterMap.getMetricRegistry());
    this.blobToReplicas = new HashMap<>();
    this.replicaToMessages = new HashMap<>();
  }

  /**
   * A callback method to invoke after calling handleReplicaMetadataResponse method for given RemoteReplicaGroup.
   * Subclass can override this method to put more control to each group.
   * @param group
   */
  protected void afterHandleReplicaMetadataResponse(RemoteReplicaGroup group) {
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = group.getExchangeMetadataResponseList();
    List<RemoteReplicaInfo> replicasToReplicatePerNode = group.getRemoteReplicaInfos();
    for (int i = 0; i < replicasToReplicatePerNode.size(); i++) {
      ExchangeMetadataResponse metadata = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo replica = replicasToReplicatePerNode.get(i);
      if (metadata.serverErrorCode != ServerErrorCode.No_Error) {
        // bad replica
        continue;
      }
      /**
       * Create a map of blob-id -> List<replica>
       *   For eg.:
       *
       *   blob-id1 -> {replica-1, replica-2, replica-3} <sorted>
       *   blob-id2 -> {replica-1, replica-2} <sorted>
       *   blob-id3 -> {replica-1, replica-3} <sorted>
       *   blob-id4 -> {replica-4, replica-5} <sorted>
       *   blob-id5 -> {replica-4} <sorted>
       *
       */
      for (MessageInfo messageInfo : metadata.getMissingStoreMessages()) {
        String blobId = messageInfo.getStoreKey().getID();
        List<Pair<RemoteReplicaInfo, MessageInfo>> replicaMessagePairList =
            blobToReplicas.getOrDefault(blobId, new ArrayList<>());
        replicaMessagePairList.add(new Pair<>(replica, messageInfo));
        blobToReplicas.putIfAbsent(blobId, replicaMessagePairList);
      }
    }

    for (Map.Entry<String, List<Pair<RemoteReplicaInfo, MessageInfo>>> entry : blobToReplicas.entrySet()) {
      /**
       * For each blob, sort the replicas in alphabetical order and just pick the first one.
       * We can pick using any scheme, so why not alphabetical ? Easy and consistent.
       * Next, for the chosen replica, collect all the blobs belonging to it.
       */
      List<Pair<RemoteReplicaInfo, MessageInfo>> replicaMessagePairList = entry.getValue();
      replicaMessagePairList.sort(new LexicographicComparator());
      Pair<RemoteReplicaInfo, MessageInfo> replicaMessagePair = replicaMessagePairList.get(0);
      HashSet<MessageInfo> missingMessages =
          replicaToMessages.getOrDefault(replicaMessagePair.getFirst(), new HashSet<>());
      missingMessages.add(replicaMessagePair.getSecond());
      replicaToMessages.putIfAbsent(replicaMessagePair.getFirst(), missingMessages);
    }

    /**
     * Construct the exchangeMetadataResponseList.
     * We have two lists - a metadata list and a replica list. There is one-to-one correspondence between them.
     * i-th metadata is from the i-th replica.
     *
     * Iterate over these lists and construct a new metadata list.
     * If a replica is in an error state, then the new metadata is just the same as the old one aka indicates error.
     * If a replica is the one we chose above, then the new metadata is the list of blobs we want from the replica.
     * If a replica is not the one we chose above, then we just set the list of blobs from it as an empty-set.
     *
     * Finally, we set the metadata list from the replica-group.
     */
    List<ExchangeMetadataResponse> newMetadataResponseList = new ArrayList<>();
    for (int i = 0; i < replicasToReplicatePerNode.size(); i++) {
      ExchangeMetadataResponse oldMetadata = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo replica = replicasToReplicatePerNode.get(i);
      ExchangeMetadataResponse newMetadata;
      if (oldMetadata.serverErrorCode != ServerErrorCode.No_Error) {
        newMetadata = oldMetadata;
      } else if (replicaToMessages.containsKey(replica)) {
        // Copy all fields from old metadata except the list of missing messages. The new list is provided below.
        newMetadata = new ExchangeMetadataResponse(oldMetadata, replicaToMessages.get(replica));
      } else {
        // Copy all fields from old metadata except the list of missing messages. The new list is provided below.
        newMetadata = new ExchangeMetadataResponse(oldMetadata, new HashSet<>());
      }
      newMetadataResponseList.add(newMetadata);
    }

    group.setExchangeMetadataResponseList(newMetadataResponseList);
  }

  /**
   * A callback method to invoke after calling handleGetResponse method for given RemoteReplicaGroup.
   * Subclass can override this method to put more control to each group.
   * @param group
   */
  protected void afterHandleGetResponse(RemoteReplicaGroup group) {
    // noop
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
    logger.trace("replica = {}, token = {}", remoteReplicaInfo, token);
    // Table entity = Table row
    // =============================================================================================================
    // | partition-key | row-key | tokenType | logSegment | offset | storeKey | replicatedUntilUTC   | binaryToken |
    // =============================================================================================================
    // | partition     | host1   | Journal   | 3_14       | 12     | none     | 2024_Feb_11_14_21_00 | AAASLKJDFX  |
    // | partition     | host2   | Index     | 2_12       | 32     | AAEWsXZ  | 2024_Jan_02_13_01_00 | AAAEWODSDS  |
    // =============================================================================================================
    long lastOpTime = remoteReplicaInfo.getReplicatedUntilUTC();
    String partitionKey = String.valueOf(remoteReplicaInfo.getReplicaId().getPartitionId().getId());
    String rowKey = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname();
    DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
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
                : formatter.format(lastOpTime))
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
