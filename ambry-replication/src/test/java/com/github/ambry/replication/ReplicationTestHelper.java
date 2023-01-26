/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.messageformat.TtlUpdateMessageFormatInputStream;
import com.github.ambry.messageformat.UndeleteMessageFormatInputStream;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


public class ReplicationTestHelper {

  protected static int CONSTANT_TIME_MS = 100000;
  protected static long EXPIRY_TIME_MS = SystemTime.getInstance().milliseconds() + TimeUnit.DAYS.toMillis(7);
  protected static long UPDATED_EXPIRY_TIME_MS = Utils.Infinite_Time;
  protected static final short VERSION_2 = 2;
  protected static final short VERSION_5 = 5;
  protected final MockTime time = new MockTime();
  protected Properties properties;
  protected VerifiableProperties verifiableProperties;
  protected ReplicationConfig replicationConfig;
  protected final AccountService accountService;
  protected final boolean shouldUseNetworkClient;

  public ReplicationTestHelper(short requestVersion, short responseVersion, boolean shouldUseNetworkClient) {
    System.out.println("The request version: " + requestVersion + " ResponseVersion: " + responseVersion);
    this.shouldUseNetworkClient = shouldUseNetworkClient;
    List<TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, "DC1", (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    properties = new Properties();
    properties.setProperty("replication.metadata.request.version", Short.toString(requestVersion));
    properties.setProperty("replication.metadataresponse.version", Short.toString(responseVersion));
    properties.setProperty("replication.cloud.token.factory", "com.github.ambry.replication.MockFindTokenFactory");
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    properties.setProperty("clustermap.replica.catchup.acceptable.lag.bytes", Long.toString(100L));
    properties.setProperty("replication.synced.replica.backoff.duration.ms", "3000");
    properties.setProperty("replication.intra.replica.thread.throttle.sleep.duration.ms", "100");
    properties.setProperty("replication.inter.replica.thread.throttle.sleep.duration.ms", "200");
    properties.setProperty("replication.replica.thread.idle.sleep.duration.ms", "1000");
    properties.setProperty("replication.track.local.from.remote.per.partition.lag", "true");
    properties.setProperty("replication.max.partition.count.per.request", Integer.toString(0));
    properties.setProperty(ReplicationConfig.REPLICATION_USING_NONBLOCKING_NETWORK_CLIENT_FOR_REMOTE_COLO,
        String.valueOf(shouldUseNetworkClient));
    properties.put("store.segment.size.in.bytes", Long.toString(MockReplicaId.MOCK_REPLICA_CAPACITY / 2L));
    verifiableProperties = new VerifiableProperties(properties);
    replicationConfig = new ReplicationConfig(verifiableProperties);
    accountService = Mockito.mock(AccountService.class);
    time.setCurrentMilliseconds(System.currentTimeMillis());
  }

  // helpers

  /**
   * Helper function to compare put record buffers.
   */
  protected void assertPutRecord(ByteBuffer putRecordBuffer, ByteBuffer expectedPutRecordBuffer, MessageInfo info) {
    // The second short should be the lifeVersion, and it should match lifeVersion from info.
    putRecordBuffer.position(2);
    short lifeVersion = putRecordBuffer.getShort();
    assertEquals("LifeVersion doesn't match", lifeVersion, info.getLifeVersion());
    putRecordBuffer.position(32); // this is where header crc starts
    long crc = putRecordBuffer.getLong();
    expectedPutRecordBuffer.position(2);
    expectedPutRecordBuffer.putShort(lifeVersion);
    expectedPutRecordBuffer.position(32);
    expectedPutRecordBuffer.putLong(crc);
    assertArrayEquals(putRecordBuffer.array(), expectedPutRecordBuffer.array());
  }

  /**
   * Helper function to test when the local lifeVersion is greater than the remote lifeVersion.
   * @param localTtlUpdated
   * @param remoteTtlUpdated
   * @throws Exception
   */
  protected void lifeVersionLocalGreaterThanRemote_Delete(boolean localTtlUpdated, boolean remoteTtlUpdated)
      throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // add 1 messages to remote host with lifeVersion being 0 and add it local host with lifeVersion being 1.
      StoreKey toDeleteId = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);
      if (remoteTtlUpdated) {
        addTtlUpdateMessagesToReplicasOfPartition(partitionId, toDeleteId, Arrays.asList(remoteHost),
            UPDATED_EXPIRY_TIME_MS);
      }
      addDeleteMessagesToReplicasOfPartition(partitionId, toDeleteId, Collections.singletonList(remoteHost));

      BlobId blobId = (BlobId) toDeleteId;
      short accountId = blobId.getAccountId();
      short containerId = blobId.getContainerId();
      short lifeVersion = 1;
      // first put message has encryption turned on
      boolean toEncrypt = true;

      // create a put message with lifeVersion bigger than 0
      PutMsgInfoAndBuffer msgInfoAndBuffer =
          createPutMessage(toDeleteId, accountId, containerId, toEncrypt, lifeVersion);
      localHost.addMessage(partitionId,
          new MessageInfo(toDeleteId, msgInfoAndBuffer.byteBuffer.remaining(), false, false, false, Utils.Infinite_Time,
              null, accountId, containerId, msgInfoAndBuffer.messageInfo.getOperationTimeMs(), lifeVersion),
          msgInfoAndBuffer.byteBuffer);
      if (localTtlUpdated) {
        addTtlUpdateMessagesToReplicasOfPartition(partitionId, toDeleteId, Collections.singletonList(localHost),
            EXPIRY_TIME_MS, lifeVersion);
      }

      // ensure that the first key is not deleted in the local host
      assertNull(toDeleteId + " should not be deleted in the local host",
          getMessageInfo(toDeleteId, localHost.infosByPartition.get(partitionId), true, false, false));
    }

    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, null, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // Do the replica metadata exchange.
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);

    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      // we don't have any missing key here.
      assertEquals(0, response.get(i).missingStoreMessages.size());
      remoteReplicaInfos.get(i).setToken(response.get(i).remoteToken);
      PartitionId partitionId = partitionIds.get(i);
      StoreKey key = localHost.infosByPartition.get(partitionId).get(0).getStoreKey();
      assertNull(key + " should not be deleted in the local host",
          getMessageInfo(key, localHost.infosByPartition.get(partitionId), true, false, false));
      if (!localTtlUpdated) {
        assertNull(key + " should not be ttlUpdated in the local host",
            getMessageInfo(key, localHost.infosByPartition.get(partitionId), false, false, true));
      }
    }
  }

  /**
   * Verify remote replica info is/isn't present in given {@link PartitionInfo}.
   * @param partitionInfo the {@link PartitionInfo} to check if it contains remote replica info
   * @param remoteReplica remote replica to check
   * @param shouldExist if {@code true}, remote replica info should exist. {@code false} otherwise
   */
  protected void verifyRemoteReplicaInfo(PartitionInfo partitionInfo, ReplicaId remoteReplica, boolean shouldExist) {
    Optional<RemoteReplicaInfo> findResult =
        partitionInfo.getRemoteReplicaInfos().stream().filter(info -> info.getReplicaId() == remoteReplica).findAny();
    if (shouldExist) {
      assertTrue("Expected remote replica info is not found in partition info", findResult.isPresent());
      assertEquals("Node of remote replica is not expected", remoteReplica.getDataNodeId(),
          findResult.get().getReplicaId().getDataNodeId());
    } else {
      assertFalse("Remote replica info should no long exist in partition info", findResult.isPresent());
    }
  }

  protected ReplicaId getNewReplicaToAdd(MockClusterMap clusterMap) {
    DataNodeId currentNode = clusterMap.getDataNodeIds().get(0);
    PartitionId newPartition = clusterMap.createNewPartition(clusterMap.getDataNodes());
    return newPartition.getReplicaIds().stream().filter(r -> r.getDataNodeId() == currentNode).findFirst().get();
  }

  /**
   * Helper method to create storage manager and replication manager
   * @param clusterMap {@link ClusterMap} to use
   * @param clusterMapConfig {@link ClusterMapConfig} to use
   * @param clusterParticipant {@link com.github.ambry.clustermap.ClusterParticipant} for listener registration.
   * @return a pair of storage manager and replication manager
   * @throws Exception
   */
  protected Pair<StorageManager, ReplicationManager> createStorageManagerAndReplicationManager(ClusterMap clusterMap,
      ClusterMapConfig clusterMapConfig, MockHelixParticipant clusterParticipant, ConnectionPool mockConnectionPool)
      throws Exception {
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    DataNodeId dataNodeId = clusterMap.getDataNodeIds().get(0);

    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);

    StorageManager storageManager =
        new StorageManager(storeConfig, new DiskManagerConfig(verifiableProperties), Utils.newScheduler(1, true),
            new MetricRegistry(), null, clusterMap, dataNodeId, null,
            clusterParticipant == null ? null : Collections.singletonList(clusterParticipant), new MockTime(), null,
            new InMemAccountService(false, false));
    storageManager.start();

    MockConnectionPool mockPool = (MockConnectionPool) mockConnectionPool;
    NetworkClientFactory mockNetworkClientFactory =
        shouldUseNetworkClient && mockConnectionPool != null ? new MockNetworkClientFactory(mockPool.getHosts(),
            mockPool.getClusterMap(), mockPool.getMaxEntriesToReturn()) : null;
    MockReplicationManager replicationManager =
        new MockReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, clusterMap,
            dataNodeId, storeKeyConverterFactory, clusterParticipant, mockConnectionPool, mockNetworkClientFactory,
            new MockFindTokenHelper(storeKeyFactory, replicationConfig), BlobIdTransformer.class.getName(),
            storeKeyFactory, time);

    return new Pair<>(storageManager, replicationManager);
  }

  /**
   * Helper method to create storage manager and replication manager
   * @param clusterMap {@link ClusterMap} to use
   * @param clusterMapConfig {@link ClusterMapConfig} to use
   * @param clusterParticipant {@link com.github.ambry.clustermap.ClusterParticipant} for listener registration.
   * @return a pair of storage manager and replication manager
   * @throws Exception
   */
  protected Pair<StorageManager, ReplicationManager> createStorageManagerAndReplicationManager(ClusterMap clusterMap,
      ClusterMapConfig clusterMapConfig, MockHelixParticipant clusterParticipant) throws Exception {

    return createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, clusterParticipant,
        new MockConnectionPool(new HashMap<>(), clusterMap, 0));
  }

  /**
   * Creates and gets the remote replicas that the local host will deal with and the {@link ReplicaThread} to perform
   * replication with.
   * @param batchSize the number of messages to be returned in each iteration of replication
   * @param clusterMap the {@link ClusterMap} to use
   * @param localHost the local {@link MockHost} (the one running the replica thread)
   * @param storeKeyConverter the {@link StoreKeyConverter} to be used in {@link ReplicaThread}
   * @param transformer the {@link Transformer} to be used in {@link ReplicaThread}
   * @param listener the {@link StoreEventListener} to use.
   * @param replicaSyncUpManager the {@link ReplicaSyncUpManager} to help create replica thread
   * @param remoteHosts the list of remote {@link MockHost} (the target of replication)
   * @return a pair whose first element is the set of remote replicas and the second element is the {@link ReplicaThread}
   */
  protected Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> getRemoteReplicasAndReplicaThread(
      int batchSize, ClusterMap clusterMap, MockHost localHost, StoreKeyConverter storeKeyConverter,
      Transformer transformer, StoreEventListener listener, ReplicaSyncUpManager replicaSyncUpManager,
      MockHost... remoteHosts) throws ReflectiveOperationException {
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<>();
    for (MockHost remoteHost : remoteHosts) {
      replicationMetrics.populateSingleColoMetrics(remoteHost.dataNodeId.getDatacenterName());
      List<RemoteReplicaInfo> remoteReplicaInfoList = localHost.getRemoteReplicaInfos(remoteHost, listener);
      replicasToReplicate.put(remoteHost.dataNodeId, remoteReplicaInfoList);
      hosts.put(remoteHost.dataNodeId, remoteHost);
    }
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);
    MockNetworkClient networkClient =
        shouldUseNetworkClient ? new MockNetworkClient(hosts, clusterMap, batchSize) : null;
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", new MockFindTokenHelper(storeKeyFactory, replicationConfig), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, networkClient, replicationConfig,
            replicationMetrics, null, storeKeyConverter, transformer, clusterMap.getMetricRegistry(), false,
            localHost.dataNodeId.getDatacenterName(), new ResponseHandler(clusterMap), time, replicaSyncUpManager, null,
            null);
    for (MockHost remoteHost : remoteHosts) {
      for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicate.get(remoteHost.dataNodeId)) {
        replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
      }
    }
    for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
      replicationMetrics.addLagMetricForPartition(partitionId, true);
    }

    return new Pair<>(replicasToReplicate, replicaThread);
  }

  /**
   * Asserts the number of missing keys between the local and remote replicas and fixes the keys
   * @param expectedIndex initial expected index
   * @param expectedIndexInc increment level for the expected index (how much the findToken index is expected to increment)
   * @param expectedMissingKeysSum the number of missing keys expected
   * @param replicaThread replicaThread that will be performing replication
   * @param replicasToReplicate list of replicas to replicate between
   * @return expectedIndex + expectedIndexInc
   * @throws Exception
   */
  protected int assertMissingKeysAndFixMissingStoreKeys(int expectedIndex, int expectedIndexInc,
      int expectedMissingKeysSum, ReplicaThread replicaThread,
      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    return assertMissingKeysAndFixMissingStoreKeys(expectedIndex, expectedIndex, expectedIndexInc,
        expectedMissingKeysSum, replicaThread, replicasToReplicate);
  }

  /**
   * Asserts the number of missing keys between the local and remote replicas and fixes the keys
   * @param expectedIndex initial expected index for even numbered partitions
   * @param expectedIndexOdd initial expected index for odd numbered partitions
   * @param expectedIndexInc increment level for the expected index (how much the findToken index is expected to increment)
   * @param expectedMissingKeysSum the number of missing keys expected
   * @param replicaThread replicaThread that will be performing replication
   * @param replicasToReplicate list of replicas to replicate between
   * @return expectedIndex + expectedIndexInc
   * @throws Exception
   */
  protected int assertMissingKeysAndFixMissingStoreKeys(int expectedIndex, int expectedIndexOdd, int expectedIndexInc,
      int expectedMissingKeysSum, ReplicaThread replicaThread,
      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    expectedIndex += expectedIndexInc;
    expectedIndexOdd += expectedIndexInc;
    replicaThread.replicate();
    for (DataNodeId dataNodeId : replicasToReplicate.keySet()) {
      List<ReplicaThread.ExchangeMetadataResponse> response =
          replicaThread.getExchangeMetadataResponsesInEachCycle().get(dataNodeId);
      assertEquals("Response should contain a response for each replica", replicasToReplicate.get(dataNodeId).size(),
          response.size());
      for (int i = 0; i < response.size(); i++) {
        assertEquals(expectedMissingKeysSum, response.get(i).missingStoreMessages.size());
        assertEquals(i % 2 == 0 ? expectedIndex : expectedIndexOdd,
            ((MockFindToken) response.get(i).remoteToken).getIndex());
        assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
            replicasToReplicate.get(dataNodeId).get(i).getToken());
      }
    }
    return expectedIndex;
  }

  /**
   * Asserts the number of missing keys between the local and remote replicas
   * @param expectedMissingKeysSum the number of missing keys expected for each replica
   * @param batchSize how large of a batch size the internal MockConnection will use for fixing missing store keys
   * @param replicaThread replicaThread that will be performing replication
   * @param remoteHost the remote host that keys are being pulled from
   * @param replicasToReplicate list of replicas to replicate between
   * @throws Exception
   */
  protected void assertMissingKeys(int[] expectedMissingKeysSum, int batchSize, ReplicaThread replicaThread,
      MockHost remoteHost, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(expectedMissingKeysSum[i], response.get(i).missingStoreMessages.size());
    }
  }

  /**
   * Verifies that there are no more missing keys between the local and remote host and that
   * there are an expected amount of missing buffers between the remote and local host
   * @param remoteHost
   * @param localHost
   * @param replicaThread
   * @param replicasToReplicate
   * @param idsToBeIgnoredByPartition
   * @param storeKeyConverter
   * @param expectedIndex
   * @param expectedIndexOdd
   * @param expectedMissingBuffers
   * @throws Exception
   */
  protected void verifyNoMoreMissingKeysAndExpectedMissingBufferCount(MockHost remoteHost, MockHost localHost,
      ReplicaThread replicaThread, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate,
      Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition, StoreKeyConverter storeKeyConverter,
      int expectedIndex, int expectedIndexOdd, int expectedMissingBuffers) throws Exception {
    // no more missing keys
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(0, response.get(i).missingStoreMessages.size());
      assertEquals(i % 2 == 0 ? expectedIndex : expectedIndexOdd,
          ((MockFindToken) response.get(i).remoteToken).getIndex());
    }

    Map<PartitionId, List<MessageInfo>> missingInfos =
        remoteHost.getMissingInfos(localHost.infosByPartition, storeKeyConverter);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      // test that the first key has been marked deleted
      List<MessageInfo> messageInfos = localHost.infosByPartition.get(entry.getKey());
      StoreKey deletedId = messageInfos.get(0).getStoreKey();
      assertNotNull(deletedId + " should have been deleted",
          getMessageInfo(deletedId, messageInfos, true, false, false));
      Map<StoreKey, Boolean> ignoreState = new HashMap<>();
      for (StoreKey toBeIgnored : idsToBeIgnoredByPartition.get(entry.getKey())) {
        ignoreState.put(toBeIgnored, false);
      }
      for (MessageInfo messageInfo : entry.getValue()) {
        StoreKey id = messageInfo.getStoreKey();
        if (!id.equals(deletedId)) {
          assertTrue("Message should be eligible to be ignored: " + id, ignoreState.containsKey(id));
          ignoreState.put(id, true);
        }
      }
      for (Map.Entry<StoreKey, Boolean> stateInfo : ignoreState.entrySet()) {
        assertTrue(stateInfo.getKey() + " should have been ignored", stateInfo.getValue());
      }
    }
    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals(expectedMissingBuffers, entry.getValue().size());
    }
  }

  /**
   * Replicate between local and remote hosts and verify the results on local host are expected.
   * 1.remote host has different versions of blobIds and local host is empty;
   * 2.remote and local hosts have different conversion maps (StoreKeyConverter).
   * @param testSetup the {@link ReplicationTestSetup} used to provide test environment info.
   * @param expectedStr the string presenting expected sequence of PUT, DELETE messages on local host.
   * @throws Exception
   */
  protected void replicateAndVerify(ReplicationTestSetup testSetup, String expectedStr) throws Exception {
    PartitionId partitionId = testSetup.partitionIds.get(0);
    List<RemoteReplicaInfo> singleReplicaList = testSetup.replicasToReplicate.get(testSetup.remoteHost.dataNodeId)
        .stream()
        .filter(e -> e.getReplicaId().getPartitionId() == partitionId)
        .collect(Collectors.toList());

    // Do the replica metadata exchange.
    List<ReplicaThread.ExchangeMetadataResponse> responses = testSetup.replicaThread.exchangeMetadata(
        new MockConnectionPool.MockConnection(testSetup.remoteHost, 10, testSetup.remoteConversionMap),
        singleReplicaList);
    // Do Get request to fix missing keys
    testSetup.replicaThread.fixMissingStoreKeys(
        new MockConnectionPool.MockConnection(testSetup.remoteHost, 10, testSetup.remoteConversionMap),
        singleReplicaList, responses, false);

    // Verify
    String[] expectedResults = expectedStr.equals("") ? new String[0] : expectedStr.split("\\s");
    int size = testSetup.localHost.infosByPartition.get(partitionId).size();
    assertEquals("Mismatch in number of messages on local host after replication", expectedResults.length, size);
    for (int i = 0; i < size; ++i) {
      String blobIdStr = testSetup.localHost.infosByPartition.get(partitionId).get(i).getStoreKey().toString();
      boolean isDeleteMessage = testSetup.localHost.infosByPartition.get(partitionId).get(i).isDeleted();
      switch (expectedResults[i]) {
        case "OP":
          assertEquals("Mismatch in blodId on local host after replication", testSetup.oldKey.toString(), blobIdStr);
          assertFalse("Mismatch in message type on local host after replication", isDeleteMessage);
          break;
        case "OD":
          assertEquals("Mismatch in blodId on local host after replication", testSetup.oldKey.toString(), blobIdStr);
          assertTrue("Mismatch in message type on local host after replication", isDeleteMessage);
          break;
        case "NP":
          assertEquals("Mismatch in blodId on local host after replication", testSetup.newKey.toString(), blobIdStr);
          assertFalse("Mismatch in message type on local host after replication", isDeleteMessage);
          break;
        case "ND":
          assertEquals("Mismatch in blodId on local host after replication", testSetup.newKey.toString(), blobIdStr);
          assertTrue("Mismatch in message type on local host after replication", isDeleteMessage);
          break;
      }
    }
  }

  /**
   * @return {@link StoreKeyConverter} used in replication.
   */
  protected StoreKeyConverter getStoreKeyConverter() {
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    return storeKeyConverterFactory.getStoreKeyConverter();
  }

  /**
   * Set up different combinations of PUT, DELETE messages on remote host.
   * For simplicity, we use O(old) to denote Version_2 blobId, N(new) to denote Version_5 blobId.
   * For example, OP means Version_2 PUT message, ND means Version_5 DELETE message, etc.
   * @param testSetup the {@link ReplicationTestSetup} used to provide test environment info.
   * @param msgStr the string presenting the sequence of PUT, DELETE messages
   * @throws Exception
   */
  protected void createMixedMessagesOnRemoteHost(ReplicationTestSetup testSetup, String msgStr) throws Exception {
    PartitionId partitionId = testSetup.partitionIds.get(0);
    StoreKey oldKey = testSetup.oldKey;
    StoreKey newKey = testSetup.newKey;
    MockHost remoteHost = testSetup.remoteHost;
    String[] messages = msgStr.split("\\s");
    for (String message : messages) {
      switch (message) {
        case "OP":
          addPutMessagesToReplicasOfPartition(Collections.singletonList(oldKey), Collections.singletonList(remoteHost));
          break;
        case "OD":
          addDeleteMessagesToReplicasOfPartition(partitionId, oldKey, Collections.singletonList(remoteHost));
          break;
        case "NP":
          addPutMessagesToReplicasOfPartition(Collections.singletonList(newKey), Collections.singletonList(remoteHost));
          break;
        case "ND":
          addDeleteMessagesToReplicasOfPartition(partitionId, newKey, Collections.singletonList(remoteHost));
          break;
      }
    }
  }

  /**
   * Get remote leader replicas in remote node whose partitions have leaders in local node as well.
   * @return list of leader replicas
   */
  protected Set<ReplicaId> getRemoteLeaderReplicasWithLeaderPartitionsOnLocalNode(ClusterMap clusterMap,
      DataNodeId localNode, DataNodeId remoteNode) {
    Set<ReplicaId> remoteLeaderReplicas = new HashSet<>();
    List<? extends ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    List<? extends ReplicaId> remoteReplicas = clusterMap.getReplicaIds(remoteNode);
    for (int i = 0; i < localReplicas.size(); i++) {
      MockReplicaId localReplica = (MockReplicaId) localReplicas.get(i);
      MockReplicaId remoteReplica = (MockReplicaId) remoteReplicas.get(i);
      if (localReplica.getReplicaState() == ReplicaState.LEADER
          && remoteReplica.getReplicaState() == ReplicaState.LEADER) {
        remoteLeaderReplicas.add(remoteReplicas.get(i));
      }
    }
    return remoteLeaderReplicas;
  }

  /**
   * Get a pair of data nodes (one from local data center and one from remote data center) which share partitions
   * with local node.
   * @return list of data nodes that share partitions with local node.
   */
  protected List<DataNodeId> getRemoteNodesFromLocalAndRemoteDCs(ClusterMap clusterMap, DataNodeId localNode) {
    List<DataNodeId> remoteNodesFromLocalAndRemoteDCs = new ArrayList<>();
    String currentDataCenter = localNode.getDatacenterName();
    DataNodeId remoteNodeInLocalDC = null;
    DataNodeId remoteNodeInRemoteDC = null;
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(localNode);
    MockPartitionId existingPartition = (MockPartitionId) replicaIds.get(0).getPartitionId();
    for (ReplicaId replicaId : existingPartition.getReplicaIds()) {
      if (!replicaId.getDataNodeId().equals(localNode)) {
        if (replicaId.getDataNodeId().getDatacenterName().equals(currentDataCenter)) {
          if (remoteNodeInLocalDC == null) {
            remoteNodeInLocalDC = replicaId.getDataNodeId();
          }
        } else if (remoteNodeInRemoteDC == null) {
          remoteNodeInRemoteDC = replicaId.getDataNodeId();
        }
      }
      if (remoteNodeInLocalDC != null && remoteNodeInRemoteDC != null) {
        break;
      }
    }

    remoteNodesFromLocalAndRemoteDCs.add(remoteNodeInLocalDC);
    remoteNodesFromLocalAndRemoteDCs.add(remoteNodeInRemoteDC);

    return remoteNodesFromLocalAndRemoteDCs;
  }

  /**
   * Verifies that blob messages across all partitions at local and remote hosts are equal.
   */
  protected void checkBlobMessagesAreEqualInLocalAndRemoteHosts(MockHost localHost, MockHost remoteHost,
      Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition,
      Map<PartitionId, List<StoreKey>> idsToBeTtlUpdatedByPartition) {

    for (Map.Entry<PartitionId, List<MessageInfo>> remoteInfoEntry : remoteHost.infosByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteInfoEntry.getValue();
      List<MessageInfo> localInfos = localHost.infosByPartition.get(remoteInfoEntry.getKey());
      int remoteIndex = 0;

      Set<StoreKey> seen = new HashSet<>();
      for (MessageInfo remoteInfo : remoteInfos) {
        StoreKey id = remoteInfo.getStoreKey();
        if (seen.add(id)) {
          MessageInfo localInfo = getMessageInfo(id, localInfos, false, false, false);
          if (localInfo == null) {
            assertTrue("Should be ignored", idsToBeIgnoredByPartition.get(remoteInfoEntry.getKey()).contains(id));
          } else {
            assertFalse("Should not be ignored",
                idsToBeIgnoredByPartition.get(remoteInfoEntry.getKey()) != null && idsToBeIgnoredByPartition.get(
                    remoteInfoEntry.getKey()).contains(id));
            MessageInfo mergedLocalInfo = getMergedMessageInfo(id, localInfos);
            MessageInfo mergedRemoteInfo = getMergedMessageInfo(id, remoteInfos);
            assertEquals(mergedLocalInfo.isDeleted(), mergedRemoteInfo.isDeleted());
            assertEquals(mergedLocalInfo.isTtlUpdated(), mergedRemoteInfo.isTtlUpdated());
            assertEquals(mergedLocalInfo.isTtlUpdated(),
                idsToBeTtlUpdatedByPartition.get(remoteInfoEntry.getKey()).contains(id));
            assertEquals(mergedLocalInfo.getLifeVersion(), mergedRemoteInfo.getLifeVersion());
            assertEquals(mergedLocalInfo.getAccountId(), mergedRemoteInfo.getAccountId());
            assertEquals(mergedLocalInfo.getContainerId(), mergedRemoteInfo.getContainerId());
            assertEquals("Key " + id, mergedLocalInfo.getExpirationTimeInMs(),
                mergedRemoteInfo.getExpirationTimeInMs());
            ByteBuffer putRecordBuffer = null;
            for (int i = 0; i < localInfos.size(); i++) {
              if (localInfo.equals(localInfos.get(i))) {
                putRecordBuffer = localHost.buffersByPartition.get(remoteInfoEntry.getKey()).get(i).duplicate();
                break;
              }
            }
            assertNotNull(putRecordBuffer);
            // Make sure the put buffer contains the same info as the message Info
            assertPutRecord(putRecordBuffer,
                remoteHost.buffersByPartition.get(remoteInfoEntry.getKey()).get(remoteIndex).duplicate(),
                mergedLocalInfo);
          }
        }
        remoteIndex++;
      }
    }
  }

  // static methods to get local and remote hosts, add put/ttlupdate/delete/undelete messages to partitions

  /**
   * Selects a local and remote host for replication tests that need it.
   * @param clusterMap the {@link MockClusterMap} to use.
   * @return a {@link Pair} with the first entry being the "local" host and the second, the "remote" host.
   */
  public static Pair<MockHost, MockHost> getLocalAndRemoteHosts(MockClusterMap clusterMap) {
    // to make sure we select hosts with the SPECIAL_PARTITION_CLASS, pick hosts from the replicas of that partition
    PartitionId specialPartitionId = clusterMap.getWritablePartitionIds(MockClusterMap.SPECIAL_PARTITION_CLASS).get(0);
    // these hosts have replicas of the "special" partition and all the other partitions.
    MockHost localHost = new MockHost(specialPartitionId.getReplicaIds().get(0).getDataNodeId(), clusterMap);
    MockHost remoteHost = new MockHost(specialPartitionId.getReplicaIds().get(1).getDataNodeId(), clusterMap);
    return new Pair<>(localHost, remoteHost);
  }

  /**
   * For the given partitionId, constructs put messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param hosts the list of {@link MockHost} all of which will be populated with the messages.
   * @param count the number of messages to construct and add.
   * @return the list of blobs ids that were generated.
   * @throws MessageFormatException
   * @throws IOException
   */
  public static List<StoreKey> addPutMessagesToReplicasOfPartition(PartitionId partitionId, List<MockHost> hosts,
      int count) throws MessageFormatException, IOException {
    return addPutMessagesToReplicasOfPartition(partitionId, hosts, (short) 0, count);
  }

  public static List<StoreKey> addPutMessagesToReplicasOfPartition(PartitionId partitionId, List<MockHost> hosts,
      short lifeVersion, int count) throws MessageFormatException, IOException {
    List<StoreKey> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      boolean toEncrypt = i % 2 == 0;
      BlobId id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
              partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      ids.add(id);
      PutMsgInfoAndBuffer msgInfoAndBuffer =
          createPutMessage(id, id.getAccountId(), id.getContainerId(), toEncrypt, lifeVersion);
      for (MockHost host : hosts) {
        host.addMessage(partitionId, msgInfoAndBuffer.messageInfo, msgInfoAndBuffer.byteBuffer.duplicate());
      }
    }
    return ids;
  }

  public static BlobId generateRandomBlobId(PartitionId partitionId) {
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    boolean toEncrypt = Utils.getRandomShort(TestUtils.RANDOM) % 2 == 0;
    return new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
  }

  public static void addPutMessagesToReplicasOfPartition(List<StoreKey> ids, List<MockHost> hosts)
      throws MessageFormatException, IOException {
    List<Transformer> transformerList = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      transformerList.add(null);
    }
    addPutMessagesToReplicasOfPartition(ids, transformerList, hosts);
  }

  public static void addPutMessagesToReplicasOfPartition(List<StoreKey> ids, List<Transformer> transformPerId,
      List<MockHost> hosts) throws MessageFormatException, IOException {
    Iterator<Transformer> transformerIterator = transformPerId.iterator();
    for (StoreKey storeKey : ids) {
      Transformer transformer = transformerIterator.next();
      BlobId id = (BlobId) storeKey;
      PutMsgInfoAndBuffer msgInfoAndBuffer =
          createPutMessage(id, id.getAccountId(), id.getContainerId(), BlobId.isEncrypted(id.toString()));
      MessageInfo msgInfo = msgInfoAndBuffer.messageInfo;
      ByteBuffer byteBuffer = msgInfoAndBuffer.byteBuffer;
      if (transformer != null) {
        Message message = new Message(msgInfo, new ByteBufferInputStream(byteBuffer));
        TransformationOutput output = transformer.transform(message);
        assertNull(output.getException());
        message = output.getMsg();
        byteBuffer =
            ByteBuffer.wrap(Utils.readBytesFromStream(message.getStream(), (int) message.getMessageInfo().getSize()));
        msgInfo = message.getMessageInfo();
      }
      for (MockHost host : hosts) {
        host.addMessage(id.getPartition(), msgInfo, byteBuffer.duplicate());
      }
    }
  }

  /**
   * For the given partitionId, constructs delete messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param id the {@link StoreKey} to create a delete message for.
   * @param hosts the list of {@link MockHost} all of which will be populated with the messages.
   * @throws MessageFormatException
   * @throws IOException
   */
  public static void addDeleteMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id, List<MockHost> hosts)
      throws MessageFormatException, IOException {
    MessageInfo putMsg = getMessageInfo(id, hosts.get(0).infosByPartition.get(partitionId), false, false, false);
    short aid;
    short cid;
    short lifeVersion;
    if (putMsg == null) {
      // the StoreKey must be a BlobId in this case (to get the account and container id)
      aid = ((BlobId) id).getAccountId();
      cid = ((BlobId) id).getContainerId();
      lifeVersion = 0;
    } else {
      aid = putMsg.getAccountId();
      cid = putMsg.getContainerId();
      lifeVersion = putMsg.getLifeVersion();
    }
    ByteBuffer buffer = getDeleteMessage(id, aid, cid, CONSTANT_TIME_MS, lifeVersion);
    for (MockHost host : hosts) {
      // ok to send false for ttlUpdated
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), true, false, false, Utils.Infinite_Time, null, aid, cid,
              CONSTANT_TIME_MS, lifeVersion), buffer.duplicate());
    }
  }

  public static void addDeleteMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id, List<MockHost> hosts,
      short lifeVersion, long expirationTime) throws MessageFormatException, IOException {
    short accountId = ((BlobId) id).getAccountId();
    short containerId = ((BlobId) id).getContainerId();
    ByteBuffer buffer = getDeleteMessage(id, accountId, containerId, CONSTANT_TIME_MS, lifeVersion);
    for (MockHost host : hosts) {
      // ok to send false for ttlUpdated
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), true, false, false, expirationTime, null, accountId, containerId,
              CONSTANT_TIME_MS, lifeVersion), buffer.duplicate());
    }
  }

  /**
   * For the given partitionId, constructs a PUT messages and adds it to the given hosts.
   * @param id the {@link StoreKey} to create a put message for.
   * @param accountId the accountId of this message.
   * @param containerId the containerId of this message.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param hosts the list of {@link MockHost} all of which will be populated with the messages.
   * @param operationTime the operation of this message.
   * @param expirationTime the expirationTime of this message.
   * @throws MessageFormatException
   * @throws IOException
   */
  public static void addPutMessagesToReplicasOfPartition(StoreKey id, short accountId, short containerId,
      PartitionId partitionId, List<MockHost> hosts, long operationTime, long expirationTime)
      throws MessageFormatException, IOException {
    // add a PUT message with expiration time less than VCR threshold to remote host.
    boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
    ReplicationTest.PutMsgInfoAndBuffer msgInfoAndBuffer = createPutMessage(id, accountId, containerId, toEncrypt);
    for (MockHost host : hosts) {
      host.addMessage(partitionId,
          new MessageInfo(id, msgInfoAndBuffer.byteBuffer.remaining(), false, false, false, expirationTime, null,
              accountId, containerId, operationTime, (short) 0), msgInfoAndBuffer.byteBuffer);
    }
  }

  /**
   * For the given partitionId, constructs ttl update messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param id the {@link StoreKey} to create a ttl update message for.
   * @param hosts the list of {@link MockHost} all of which will be populated with the messages.
   * @param expirationTime
   * @throws MessageFormatException
   * @throws IOException
   */
  public static void addTtlUpdateMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id,
      List<MockHost> hosts, long expirationTime) throws MessageFormatException, IOException {
    MessageInfo putMsg = getMessageInfo(id, hosts.get(0).infosByPartition.get(partitionId), false, false, false);
    short aid;
    short cid;
    short lifeVersion;
    if (putMsg == null) {
      // the StoreKey must be a BlobId in this case (to get the account and container id)
      aid = ((BlobId) id).getAccountId();
      cid = ((BlobId) id).getContainerId();
      lifeVersion = 0;
    } else {
      aid = putMsg.getAccountId();
      cid = putMsg.getContainerId();
      lifeVersion = putMsg.getLifeVersion();
    }
    ByteBuffer buffer = getTtlUpdateMessage(id, aid, cid, expirationTime, CONSTANT_TIME_MS);
    for (MockHost host : hosts) {
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), false, true, false, expirationTime, null, aid, cid, CONSTANT_TIME_MS,
              lifeVersion), buffer.duplicate());
    }
  }

  public static void addTtlUpdateMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id,
      List<MockHost> hosts, long expirationTime, short lifeVersion) throws MessageFormatException, IOException {
    short accountId = ((BlobId) id).getAccountId();
    short containerId = ((BlobId) id).getContainerId();
    ByteBuffer buffer = getTtlUpdateMessage(id, accountId, containerId, expirationTime, CONSTANT_TIME_MS);
    for (MockHost host : hosts) {
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), false, true, false, expirationTime, null, accountId, containerId,
              CONSTANT_TIME_MS, lifeVersion), buffer.duplicate());
    }
  }

  public static void addUndeleteMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id,
      List<MockHost> hosts, short lifeVersion) throws MessageFormatException, IOException {
    for (MockHost host : hosts) {
      MessageInfo latestInfo = getMergedMessageInfo(id, host.infosByPartition.get(partitionId));
      short aid;
      short cid;
      long expirationTime;
      if (latestInfo == null) {
        aid = ((BlobId) id).getAccountId();
        cid = ((BlobId) id).getContainerId();
        expirationTime = EXPIRY_TIME_MS;
      } else {
        aid = latestInfo.getAccountId();
        cid = latestInfo.getContainerId();
        expirationTime = latestInfo.getExpirationTimeInMs();
      }
      ByteBuffer buffer = getUndeleteMessage(id, aid, cid, lifeVersion, CONSTANT_TIME_MS);
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), false, false, true, expirationTime, null, aid, cid, CONSTANT_TIME_MS,
              lifeVersion), buffer.duplicate());
    }
  }

  public static PutMsgInfoAndBuffer createPutMessage(StoreKey id, short accountId, short containerId,
      boolean enableEncryption) throws MessageFormatException, IOException {
    return createPutMessage(id, accountId, containerId, enableEncryption, (short) 0);
  }

  /**
   * Constructs an entire message with header, blob properties, user metadata and blob content.
   * @param id id for which the message has to be constructed.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param enableEncryption {@code true} if encryption needs to be enabled. {@code false} otherwise
   * @param lifeVersion lifeVersion for this hich the message has to be constructed.
   * @return a {@link Pair} of {@link ByteBuffer} and {@link MessageInfo} representing the entire message and the
   *         associated {@link MessageInfo}
   * @throws MessageFormatException
   * @throws IOException
   */
  public static PutMsgInfoAndBuffer createPutMessage(StoreKey id, short accountId, short containerId,
      boolean enableEncryption, short lifeVersion) throws MessageFormatException, IOException {
    Random blobIdRandom = new Random(id.getID().hashCode());
    int blobSize = blobIdRandom.nextInt(500) + 501;
    int userMetadataSize = blobIdRandom.nextInt(blobSize / 2);
    int encryptionKeySize = blobIdRandom.nextInt(blobSize / 4);
    byte[] blob = new byte[blobSize];
    byte[] usermetadata = new byte[userMetadataSize];
    byte[] encryptionKey = enableEncryption ? new byte[encryptionKeySize] : null;
    blobIdRandom.nextBytes(blob);
    blobIdRandom.nextBytes(usermetadata);
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "test", null, null, false, EXPIRY_TIME_MS - CONSTANT_TIME_MS, CONSTANT_TIME_MS,
            accountId, containerId, encryptionKey != null, null, null, null);
    MessageFormatInputStream stream =
        new PutMessageFormatInputStream(id, encryptionKey == null ? null : ByteBuffer.wrap(encryptionKey),
            blobProperties, ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), blobSize,
            BlobType.DataBlob, lifeVersion);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return new PutMsgInfoAndBuffer(ByteBuffer.wrap(message),
        new MessageInfo(id, message.length, false, false, false, EXPIRY_TIME_MS, null, accountId, containerId,
            CONSTANT_TIME_MS, lifeVersion));
  }

  /**
   * Returns a delete message for the given {@code id}
   * @param id the id for which a delete message must be constructed.
   * @return {@link ByteBuffer} representing the entire message.
   * @throws MessageFormatException
   * @throws IOException
   */
  public static ByteBuffer getDeleteMessage(StoreKey id, short accountId, short containerId, long deletionTimeMs,
      short lifeVersion) throws MessageFormatException, IOException {
    MessageFormatInputStream stream =
        new DeleteMessageFormatInputStream(id, accountId, containerId, deletionTimeMs, lifeVersion);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return ByteBuffer.wrap(message);
  }

  /**
   * Returns a TTL update message for the given {@code id}
   * @param id the id for which a ttl update message must be constructed.
   * @param accountId the account that the blob is associated with
   * @param containerId the container that the blob is associated with
   * @param expiresAtMs the new expiry time (ms)
   * @param updateTimeMs the time of the update (in ms)
   * @return {@link ByteBuffer} representing the entire message.
   * @throws MessageFormatException
   * @throws IOException
   */
  public static ByteBuffer getTtlUpdateMessage(StoreKey id, short accountId, short containerId, long expiresAtMs,
      long updateTimeMs) throws MessageFormatException, IOException {
    return getTtlUpdateMessage(id, accountId, containerId, expiresAtMs, updateTimeMs, (short) 0);
  }

  public static ByteBuffer getTtlUpdateMessage(StoreKey id, short accountId, short containerId, long expiresAtMs,
      long updateTimeMs, short lifeVersion) throws MessageFormatException, IOException {
    MessageFormatInputStream stream =
        new TtlUpdateMessageFormatInputStream(id, accountId, containerId, expiresAtMs, updateTimeMs, lifeVersion);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return ByteBuffer.wrap(message);
  }

  public static ByteBuffer getUndeleteMessage(StoreKey id, short accountId, short containerId, short lifeVersion,
      long undeleteTimeMs) throws MessageFormatException, IOException {
    MessageFormatInputStream stream =
        new UndeleteMessageFormatInputStream(id, accountId, containerId, undeleteTimeMs, lifeVersion);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return ByteBuffer.wrap(message);
  }

  /**
   * Gets the {@link MessageInfo} for {@code id} if present.
   * @param id the {@link StoreKey} to look for.
   * @param messageInfos the {@link MessageInfo} list.
   * @param deleteMsg {@code true} if delete msg is requested. {@code false} otherwise
   * @param undeleteMsg {@code true} if undelete msg is requested. {@code false} otherwise
   * @param ttlUpdateMsg {@code true} if ttl update msg is requested. {@code false} otherwise
   * @return the delete {@link MessageInfo} if it exists in {@code messageInfos}. {@code null otherwise.}
   */
  public static MessageInfo getMessageInfo(StoreKey id, List<MessageInfo> messageInfos, boolean deleteMsg,
      boolean undeleteMsg, boolean ttlUpdateMsg) {
    MessageInfo toRet = null;
    for (int i = messageInfos.size() - 1; i >= 0; i--) {
      MessageInfo messageInfo = messageInfos.get(i);
      if (messageInfo.getStoreKey().equals(id)) {
        if (deleteMsg && messageInfo.isDeleted()) {
          toRet = messageInfo;
          break;
        } else if (undeleteMsg && messageInfo.isUndeleted()) {
          toRet = messageInfo;
          break;
        } else if (ttlUpdateMsg && !messageInfo.isUndeleted() && !messageInfo.isDeleted()
            && messageInfo.isTtlUpdated()) {
          toRet = messageInfo;
          break;
        } else if (!deleteMsg && !ttlUpdateMsg && !undeleteMsg && !messageInfo.isUndeleted() && !messageInfo.isDeleted()
            && !messageInfo.isTtlUpdated()) {
          toRet = messageInfo;
          break;
        }
      }
    }
    return toRet;
  }

  /**
   * Returns a merged {@link MessageInfo} for {@code key}
   * @param key the {@link StoreKey} to look for
   * @param partitionInfos the {@link MessageInfo}s for the partition
   * @return a merged {@link MessageInfo} for {@code key}
   */
  public static MessageInfo getMergedMessageInfo(StoreKey key, List<MessageInfo> partitionInfos) {
    MessageInfo info = getMessageInfo(key, partitionInfos, true, true, false);
    if (info == null) {
      info = getMessageInfo(key, partitionInfos, false, false, false);
    }
    MessageInfo ttlUpdateInfo = getMessageInfo(key, partitionInfos, false, false, true);
    if (ttlUpdateInfo != null) {
      info = new MessageInfo(info.getStoreKey(), info.getSize(), info.isDeleted(), true, info.isUndeleted(),
          ttlUpdateInfo.getExpirationTimeInMs(), info.getCrc(), info.getAccountId(), info.getContainerId(),
          info.getOperationTimeMs(), info.getLifeVersion());
    }
    return info;
  }

  /**
   * Interface to help perform actions on store events.
   */
  interface StoreEventListener {
    void onPut(InMemoryStore store, List<MessageInfo> messageInfos);
  }

  /**
   * A class holds the all the needed info and configuration for replication test.
   */
  protected class ReplicationTestSetup {
    MockHost localHost;
    MockHost remoteHost;
    List<PartitionId> partitionIds;
    StoreKey oldKey;
    StoreKey newKey;
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate;
    ReplicaThread replicaThread;
    Map<StoreKey, StoreKey> localConversionMap = new HashMap<>();
    Map<StoreKey, StoreKey> remoteConversionMap = new HashMap<>();

    /**
     * ReplicationTestSetup Ctor
     * @param batchSize the number of messages to be returned in each iteration of replication
     * @throws Exception
     */
    ReplicationTestSetup(int batchSize) throws Exception {
      MockClusterMap clusterMap = new MockClusterMap();
      Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
      localHost = localAndRemoteHosts.getFirst();
      remoteHost = localAndRemoteHosts.getSecond();
      StoreKeyConverter storeKeyConverter = getStoreKeyConverter();
      partitionIds = clusterMap.getWritablePartitionIds(null);
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
      oldKey = new BlobId(VERSION_2, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
          partitionIds.get(0), toEncrypt, BlobId.BlobDataType.DATACHUNK);
      newKey = new BlobId(VERSION_5, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
          partitionIds.get(0), toEncrypt, BlobId.BlobDataType.DATACHUNK);
      localConversionMap.put(oldKey, newKey);
      remoteConversionMap.put(newKey, oldKey);
      ((MockStoreKeyConverterFactory.MockStoreKeyConverter) storeKeyConverter).setConversionMap(localConversionMap);
      StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
      // the transformer is on local host, which should convert any Old version to New version (both id and message)
      Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

      Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
          getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null,
              null, remoteHost);
      replicasToReplicate = replicasAndThread.getFirst();
      replicaThread = replicasAndThread.getSecond();
    }
  }

  /**
   * A class holds the results generated by {@link ReplicationTest#createPutMessage(StoreKey, short, short, boolean)}.
   */
  public static class PutMsgInfoAndBuffer {
    ByteBuffer byteBuffer;
    MessageInfo messageInfo;

    PutMsgInfoAndBuffer(ByteBuffer bytebuffer, MessageInfo messageInfo) {
      this.byteBuffer = bytebuffer;
      this.messageInfo = messageInfo;
    }
  }
}
