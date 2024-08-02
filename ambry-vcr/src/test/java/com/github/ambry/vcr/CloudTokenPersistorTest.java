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
package com.github.ambry.vcr;

import com.azure.data.tables.models.TableEntity;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudTokenPersistor;
import com.github.ambry.cloud.RecoveryTokenFactory;
import com.github.ambry.cloud.VcrReplicaThread;
import com.github.ambry.cloud.VcrReplicationManager;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationFactory;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.LogSegmentName;
import com.github.ambry.store.Offset;
import com.github.ambry.store.PersistentIndex;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreFindTokenFactory;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.github.ambry.cloud.CloudTestUtil.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;


/**
 * Test of the CloudTokenPersistor.
 */
public class CloudTokenPersistorTest {
  private final MockPartitionId mockPartitionId;
  private final MockClusterMap mockClusterMap;
  protected final Properties properties;
  protected final VerifiableProperties verifiableProperties;
  private final AzuriteUtils azuriteUtils;
  private final AzureCloudConfig azureCloudConfig;
  private final StoreFindTokenFactory tokenFactory;
  private final DataNodeId dataNodeId;
  protected String azureTableNameReplicaTokens;
  protected BlobIdFactory storeKeyFactory;
  protected String ambryBackupVersion;
  private RemoteReplicaInfo replica;
  private AzureCloudDestinationSync azuriteClient;
  private short ACCOUNT_ID = 657;
  private short CONTAINER_ID = 908;

  public CloudTokenPersistorTest() throws IOException {
    this.ambryBackupVersion = CloudConfig.AMBRY_BACKUP_VERSION_2; // V1 is completely deprecated
    azuriteUtils = new AzuriteUtils();
    properties = azuriteUtils.getAzuriteConnectionProperties();
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(false));
    properties.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, "1");
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "Partition");
    properties.setProperty(ReplicationConfig.REPLICATION_CLOUD_TOKEN_FACTORY,
        RecoveryTokenFactory.class.getCanonicalName());
    properties.setProperty("replication.metadata.request.version", "2");
    verifiableProperties = new VerifiableProperties(properties);
    // Create test cluster
    mockClusterMap = new MockClusterMap(false, true, 1,
        1, 1, true, false,
        "localhost");
    // Create a test partition
    mockPartitionId =
        (MockPartitionId) mockClusterMap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    dataNodeId = mockPartitionId.getReplicaIds().get(0).getDataNodeId();
    azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    this.azureTableNameReplicaTokens = azureCloudConfig.azureTableNameReplicaTokens;
    storeKeyFactory = new BlobIdFactory(mockClusterMap);
    tokenFactory = new StoreFindTokenFactory(storeKeyFactory);
  }

  @Before
  public void beforeTest() throws ReflectiveOperationException {
    // Assume Azurite is up and running
    assumeTrue(azuriteUtils.connectToAzurite());
    azuriteClient = azuriteUtils.getAzuriteClient(
        properties, mockClusterMap.getMetricRegistry(),    null, null);
    // Create remote-replica info
    replica = new RemoteReplicaInfo(mockPartitionId.getReplicaIds().get(0), null, null,
        null, Long.MAX_VALUE, SystemTime.getInstance(), null);
  }

  protected Pair<TableEntity, StoreFindToken> getTokenFromAzureTable() throws IOException {
    TableEntity rowReturned = azuriteClient.getTableEntity(azureTableNameReplicaTokens,
        String.valueOf(mockPartitionId.getId()), dataNodeId.getHostname());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(
        (byte[]) rowReturned.getProperty(VcrReplicationManager.BINARY_TOKEN));
    return new Pair<>(rowReturned, (StoreFindToken) tokenFactory.getFindToken(new DataInputStream(inputStream)));
  }

  @Test
  public void testPersistToken() throws IOException {
    StoreFindToken token;
    Offset offset = new Offset(new LogSegmentName(3, 14), 36);
    ReplicaThread.ExchangeMetadataResponse response;;

    // Create ReplicaThread
    VcrReplicaThread vcrReplicaThread =
        new VcrReplicaThread("vcrReplicaThreadTest", null, mockClusterMap,
            new AtomicInteger(0), dataNodeId, null,
            null,
            null, null, false,
            "localhost", new ResponseHandler(mockClusterMap), new SystemTime(), null,
            null, null, null, azuriteClient,
            verifiableProperties);

    long lastOpTime = System.currentTimeMillis();
    replica.setReplicatedUntilTime(lastOpTime);

    // test 1: uninitialized token
    token =  new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null,
        true, (short) 3, null, null, (short) -1);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token,
            -1, Collections.emptyMap(), new SystemTime());
    vcrReplicaThread.advanceToken(replica, response);
    assertEquals(token, getTokenFromAzureTable().getSecond());

    // test 2: journal-based token w/o reset key
    token = new StoreFindToken(FindTokenType.JournalBased, offset, null, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, null, null, (short) -1);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token, -1,
        Collections.emptyMap(), new SystemTime());
    vcrReplicaThread.advanceToken(replica, response);
    assertEquals(token, getTokenFromAzureTable().getSecond());

    // test 3: journal-based token w/ reset key
    BlobId resetKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.JournalBased, offset, null, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token, -1,
        Collections.emptyMap(), new SystemTime());
    vcrReplicaThread.advanceToken(replica, response);
    assertEquals(token, getTokenFromAzureTable().getSecond());

    // test 4: index-based token
    BlobId storeKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.IndexBased, offset, storeKey, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token, -1,
        Collections.emptyMap(), new SystemTime());
    vcrReplicaThread.advanceToken(replica, response);
    assertEquals(token, getTokenFromAzureTable().getSecond());
  }

  /**
   * Tests that no token is uploaded to Azure Table if the token is unchanged in a repl-cycle.
   * This save dollars.
   */
  @Test
  public void testPersistTokenUnchanged() {
    StoreFindToken token;
    Offset offset = new Offset(new LogSegmentName(3, 14), 36);
    ReplicaThread.ExchangeMetadataResponse response;;

    // Create ReplicaThread
    VcrReplicaThread vcrReplicaThread =
        new VcrReplicaThread("vcrReplicaThreadTest", null, mockClusterMap,
            new AtomicInteger(0), dataNodeId, null,
            null,
            null, null, false,
            "localhost", new ResponseHandler(mockClusterMap), new SystemTime(), null,
            null, null, null, azuriteClient,
            verifiableProperties);

    // upload a dummy token; this must remain unchange in Azure Table through this test
    String partitionKey = String.valueOf(replica.getReplicaId().getPartitionId().getId());
    String rowKey = replica.getReplicaId().getDataNodeId().getHostname();
    TableEntity oldEntity = new TableEntity(partitionKey, rowKey).addProperty("column", "value");
    azuriteClient.upsertTableEntity(azureTableNameReplicaTokens, oldEntity);
    TableEntity newEntity = null;

    // test 1: uninitialized token
    token =  new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null,
        true, (short) 3, null, null, (short) -1);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token,
        -1, Collections.emptyMap(), new SystemTime());
    replica.setToken(token);
    vcrReplicaThread.advanceToken(replica, response);
    newEntity = azuriteClient.getTableEntity(azureTableNameReplicaTokens,
        partitionKey, rowKey);
    assertEquals(oldEntity.getProperty("column"), newEntity.getProperty("column"));

    // test 2: journal-based token w/ reset key
    BlobId resetKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.JournalBased, offset, null, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token, -1,
        Collections.emptyMap(), new SystemTime());
    replica.setToken(token);
    vcrReplicaThread.advanceToken(replica, response);
    assertEquals(oldEntity.getProperty("column"), newEntity.getProperty("column"));

    // test 3: index-based token
    BlobId storeKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.IndexBased, offset, storeKey, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    response = new ReplicaThread.ExchangeMetadataResponse(Collections.emptySet(), token,
        -1, Collections.emptyMap(), new SystemTime());
    replica.setToken(token);
    vcrReplicaThread.advanceToken(replica, response);
    newEntity = azuriteClient.getTableEntity(azureTableNameReplicaTokens,
        partitionKey, rowKey);
    assertEquals(oldEntity.getProperty("column"), newEntity.getProperty("column"));
  }

  protected void uploadTokenToAzureTable(RemoteReplicaInfo replica, StoreFindToken token, long lastOpTime) {
    String partitionKey = String.valueOf(replica.getReplicaId().getPartitionId().getId());
    String rowKey = replica.getReplicaId().getDataNodeId().getHostname();
    TableEntity entity = new TableEntity(partitionKey, rowKey)
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
    azuriteClient.upsertTableEntity(azureTableNameReplicaTokens, entity);
  }

  @Test
  public void testRetrieveToken() throws ReplicationException {
    StoreFindToken token;
    long lastOpTime;
    Offset offset = new Offset(new LogSegmentName(3, 14), 36);
    VcrClusterParticipant vcrClusterParticipant = mock(VcrClusterParticipant.class);
    when(vcrClusterParticipant.getCurrentDataNodeId()).thenReturn(dataNodeId);
    NetworkClientFactory networkClientFactory = mock(NetworkClientFactory.class);
    VcrReplicationManager vcrReplicationManager =
          new VcrReplicationManager(verifiableProperties, null, storeKeyFactory, mockClusterMap,
              vcrClusterParticipant, azuriteClient, null, networkClientFactory, null,
              null);

    // No one checks if local and peer are the same replica. Hmm ?
    // test 1: uninitialized token
    lastOpTime = Utils.Infinite_Time;
    token =  new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null,
        true, (short) 3, null, null, (short) -1);
    uploadTokenToAzureTable(replica, token, lastOpTime);
    vcrReplicationManager.reloadReplicationTokenIfExists(
        mockPartitionId.getReplicaIds().get(0), Collections.singletonList(replica));
    assertEquals(token, replica.getToken());
    assertEquals(Utils.getTimeInMsToTheNearestSec(lastOpTime), replica.getReplicatedUntilTime());

    // test 2: journal-based token w/o reset key
    lastOpTime = Utils.Infinite_Time;
    token = new StoreFindToken(FindTokenType.JournalBased, offset, null, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, null, null, (short) -1);
    uploadTokenToAzureTable(replica, token, lastOpTime);
    vcrReplicationManager.reloadReplicationTokenIfExists(
        mockPartitionId.getReplicaIds().get(0), Collections.singletonList(replica));
    assertEquals(token, replica.getToken());
    assertEquals(Utils.getTimeInMsToTheNearestSec(lastOpTime), replica.getReplicatedUntilTime());

    // test 3: journal-based token w/ reset key
    lastOpTime = System.currentTimeMillis();
    BlobId resetKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.JournalBased, offset, null, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    uploadTokenToAzureTable(replica, token, lastOpTime);
    vcrReplicationManager.reloadReplicationTokenIfExists(
        mockPartitionId.getReplicaIds().get(0), Collections.singletonList(replica));
    assertEquals(token, replica.getToken());
    assertEquals(Utils.getTimeInMsToTheNearestSec(lastOpTime), replica.getReplicatedUntilTime());

    // test 4: index-based token
    lastOpTime = System.currentTimeMillis();
    BlobId storeKey = getUniqueId(ACCOUNT_ID, CONTAINER_ID, false, mockPartitionId);
    token = new StoreFindToken(FindTokenType.IndexBased, offset, storeKey, UUID.randomUUID(), UUID.randomUUID(),
        false, (short) 3, resetKey, PersistentIndex.IndexEntryType.PUT, (short) 3);
    uploadTokenToAzureTable(replica, token, lastOpTime);
    vcrReplicationManager.reloadReplicationTokenIfExists(
        mockPartitionId.getReplicaIds().get(0), Collections.singletonList(replica));
    assertEquals(token, replica.getToken());
    assertEquals(Utils.getTimeInMsToTheNearestSec(lastOpTime), replica.getReplicatedUntilTime());
  }

  @Test
  @Ignore
  public void basicTest() throws Exception {
    Properties props =
        VcrTestUtil.createVcrProperties("DC1", "vcrClusterName", "zkConnectString", 12310, 12410, 12510, null);
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    ClusterMap clusterMap = new MockClusterMap();
    DataNodeId dataNodeId = new CloudDataNode(cloudConfig, clusterMapConfig);
    Map<String, Set<PartitionInfo>> mountPathToPartitionInfoList = new HashMap<>();
    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    StoreFindTokenFactory factory = new StoreFindTokenFactory(blobIdFactory);
    PartitionId partitionId = clusterMap.getAllPartitionIds(null).get(0);

    ReplicaId cloudReplicaId = new CloudReplica(partitionId, dataNodeId);

    List<? extends ReplicaId> peerReplicas = cloudReplicaId.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
    List<RemoteReplicaInfo.ReplicaTokenInfo> replicaTokenInfos = new ArrayList<>();
    for (ReplicaId remoteReplica : peerReplicas) {
      RemoteReplicaInfo remoteReplicaInfo =
          new RemoteReplicaInfo(remoteReplica, cloudReplicaId, null, factory.getNewFindToken(), 10,
              SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
      remoteReplicas.add(remoteReplicaInfo);
      replicaTokenInfos.add(new RemoteReplicaInfo.ReplicaTokenInfo(remoteReplicaInfo));
    }
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partitionId, null, cloudReplicaId);
    mountPathToPartitionInfoList.computeIfAbsent(cloudReplicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);

    CloudDestination cloudDestination = null;
    MetricRegistry metricRegistry = new MetricRegistry();
    if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1)) {
      cloudDestination =
          new LatchBasedInMemoryCloudDestination(Collections.emptyList(),
              AzureCloudDestinationFactory.getReplicationFeedType(new VerifiableProperties(props)), clusterMap);
    } else if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
      cloudDestination = azuriteUtils.getAzuriteClient(props, metricRegistry, clusterMap, null);
    }

    ReplicationConfig replicationConfig = new ReplicationConfig(new VerifiableProperties(props));
    CloudTokenPersistor cloudTokenPersistor = new CloudTokenPersistor("replicaTokens", mountPathToPartitionInfoList,
        new ReplicationMetrics(metricRegistry, Collections.emptyList()), clusterMap,
        new FindTokenHelper(blobIdFactory, replicationConfig), cloudDestination);
    cloudTokenPersistor.persist(cloudReplicaId.getMountPath(), replicaTokenInfos);
    List<RemoteReplicaInfo.ReplicaTokenInfo> retrievedReplicaTokenInfos =
        cloudTokenPersistor.retrieve(cloudReplicaId.getMountPath());

    Assert.assertEquals("Number of tokens doesn't match.", replicaTokenInfos.size(), retrievedReplicaTokenInfos.size());
    for (int i = 0; i < replicaTokenInfos.size(); i++) {
      Assert.assertArrayEquals("Token is not correct.", replicaTokenInfos.get(i).getReplicaToken().toBytes(),
          retrievedReplicaTokenInfos.get(i).getReplicaToken().toBytes());
    }
  }
}
