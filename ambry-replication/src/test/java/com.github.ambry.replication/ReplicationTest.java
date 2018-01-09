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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.Response;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreStats;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for ReplicaThread
 */
public class ReplicationTest {

  /**
   * Tests pausing all partitions and makes sure that the replica thread pauses. Also tests that it resumes when one
   * eligible partition is reenabled and that replication completes successfully.
   * @throws Exception
   */
  @Test
  public void replicationAllPauseTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Host localHost = new Host(clusterMap.getDataNodeIds().get(0), clusterMap);
    Host remoteHost = new Host(clusterMap.getDataNodeIds().get(1), clusterMap);

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds();
    for (PartitionId partitionId : partitionIds) {
      // add  10 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    Properties properties = new Properties();
    properties.put("replication.wait.time.between.replicas.ms", "0");
    ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(properties));
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populatePerColoMetrics(Collections.singleton(remoteHost.dataNodeId.getDatacenterName()));
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);

    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<>();
    CountDownLatch readyToPause = new CountDownLatch(1);
    CountDownLatch readyToProceed = new CountDownLatch(1);
    AtomicReference<CountDownLatch> reachedLimitLatch = new AtomicReference<>(new CountDownLatch(1));
    AtomicReference<Exception> exception = new AtomicReference<>();
    replicasToReplicate.put(remoteHost.dataNodeId,
        localHost.getRemoteReplicaInfos(remoteHost, (store, messageInfos) -> {
          try {
            readyToPause.countDown();
            readyToProceed.await();
            if (store.messageInfos.size() == remoteHost.infosByPartition.get(store.id).size()) {
              reachedLimitLatch.get().countDown();
            }
          } catch (Exception e) {
            exception.set(e);
          }
        }));

    Map<DataNodeId, Host> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    int batchSize = 4;
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);

    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, config, replicationMetrics, null,
            storeKeyFactory, true, clusterMap.getMetricRegistry(), false, localHost.dataNodeId.getDatacenterName(),
            new ResponseHandler(clusterMap));
    Thread thread = Utils.newThread(replicaThread, false);
    thread.start();

    assertEquals("There should be no disabled partitions", 0, replicaThread.getReplicationDisabledPartitions().size());
    // wait to pause replication
    readyToPause.await(10, TimeUnit.SECONDS);
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(), false);
    Set<PartitionId> expectedPaused = new HashSet<>(clusterMap.getAllPartitionIds());
    assertEquals("Disabled partitions sets do not match", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    // signal the replica thread to move forward
    readyToProceed.countDown();
    // wait for the thread to go into waiting state
    assertTrue("Replica thread did not go into waiting state",
        TestUtils.waitUntilExpectedState(thread, Thread.State.WAITING, 10000));
    // unpause one partition
    replicaThread.controlReplicationForPartitions(Collections.singletonList(partitionIds.get(0)), true);
    expectedPaused.remove(partitionIds.get(0));
    assertEquals("Disabled partitions sets do not match", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    // wait for it to catch up
    reachedLimitLatch.get().await(10, TimeUnit.SECONDS);
    // reset limit
    reachedLimitLatch.set(new CountDownLatch(partitionIds.size() - 1));
    // unpause all partitions
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(), true);
    assertEquals("There should be no disabled partitions", 0, replicaThread.getReplicationDisabledPartitions().size());
    // wait until all catch up
    reachedLimitLatch.get().await(10, TimeUnit.SECONDS);
    // shutdown
    replicaThread.shutdown();
    if (exception.get() != null) {
      throw exception.get();
    }
    Map<PartitionId, List<MessageInfo>> missingInfos = remoteHost.getMissingInfos(localHost.infosByPartition);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      assertEquals("No infos should be missing", 0, entry.getValue().size());
    }
    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("No buffers should be missing", 0, entry.getValue().size());
    }
  }

  /**
   * Tests pausing replication for all and individual partitions.
   * @throws Exception
   */
  @Test
  public void replicationPauseTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Host localHost = new Host(clusterMap.getDataNodeIds().get(0), clusterMap);
    Host remoteHost = new Host(clusterMap.getDataNodeIds().get(1), clusterMap);

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds();
    for (PartitionId partitionId : partitionIds) {
      // add  10 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    Properties properties = new Properties();
    properties.put("replication.wait.time.between.replicas.ms", "0");
    ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(properties));
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populatePerColoMetrics(Collections.singleton(remoteHost.dataNodeId.getDatacenterName()));
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<>();
    replicasToReplicate.put(remoteHost.dataNodeId, localHost.getRemoteReplicaInfos(remoteHost, null));
    Map<DataNodeId, Host> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    int batchSize = 4;
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);

    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, config, replicationMetrics, null,
            storeKeyFactory, true, clusterMap.getMetricRegistry(), false, localHost.dataNodeId.getDatacenterName(),
            new ResponseHandler(clusterMap));

    Map<PartitionId, Integer> progressTracker = new HashMap<>();
    PartitionId idToLeaveOut = clusterMap.getAllPartitionIds().get(0);
    boolean allStopped = false;
    boolean onlyOneResumed = false;
    boolean allReenabled = false;
    Set<PartitionId> expectedPaused = new HashSet<>();
    assertEquals("There should be no disabled partitions", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    while (true) {
      replicaThread.replicate(new ArrayList<>(replicasToReplicate.values()));
      boolean replicationDone = true;
      for (RemoteReplicaInfo replicaInfo : replicasToReplicate.get(remoteHost.dataNodeId)) {
        PartitionId id = replicaInfo.getReplicaId().getPartitionId();
        MockFindToken token = (MockFindToken) replicaInfo.getToken();
        int lastProgress = progressTracker.computeIfAbsent(id, id1 -> 0);
        int currentProgress = token.getIndex();
        boolean partDone = currentProgress + 1 == remoteHost.infosByPartition.get(id).size();
        if (allStopped || (onlyOneResumed && !id.equals(idToLeaveOut))) {
          assertEquals("There should have been no progress", lastProgress, currentProgress);
        } else if (!partDone) {
          assertTrue("There has been no progress", currentProgress > lastProgress);
          progressTracker.put(id, currentProgress);
        }
        replicationDone = replicationDone && partDone;
      }
      if (!allStopped && !onlyOneResumed && !allReenabled) {
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(), false);
        expectedPaused.addAll(clusterMap.getAllPartitionIds());
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
        allStopped = true;
      } else if (!onlyOneResumed && !allReenabled) {
        // resume replication for first partition
        replicaThread.controlReplicationForPartitions(Collections.singletonList(partitionIds.get(0)), true);
        expectedPaused.remove(partitionIds.get(0));
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
        allStopped = false;
        onlyOneResumed = true;
      } else if (!allReenabled) {
        // not removing the first partition
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(), true);
        onlyOneResumed = false;
        allReenabled = true;
        expectedPaused.clear();
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
      }
      if (replicationDone) {
        break;
      }
    }

    Map<PartitionId, List<MessageInfo>> missingInfos = remoteHost.getMissingInfos(localHost.infosByPartition);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      assertEquals("No infos should be missing", 0, entry.getValue().size());
    }
    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("No buffers should be missing", 0, entry.getValue().size());
    }
  }

  /**
   * Tests {@link ReplicaThread#exchangeMetadata(ConnectedChannel, List)} and
   * {@link ReplicaThread#fixMissingStoreKeys(ConnectedChannel, List, List)} for valid puts, deletes, expired keys and
   * corrupt blobs.
   * @throws Exception
   */
  @Test
  public void replicaThreadTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Host localHost = new Host(clusterMap.getDataNodeIds().get(0), clusterMap);
    Host remoteHost = new Host(clusterMap.getDataNodeIds().get(1), clusterMap);

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      List<StoreKey> idsToBeIgnored = new ArrayList<>();
      PartitionId partitionId = partitionIds.get(i);
      // add 6 messages to both hosts.
      StoreKey toDeleteId =
          addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(localHost, remoteHost), 6).get(0);

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
              containerId, partitionId, toEncrypt);
      Pair<ByteBuffer, MessageInfo> putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      remoteHost.addMessage(partitionId,
          new MessageInfo(id, putMsgInfo.getFirst().remaining(), 1, accountId, containerId,
              putMsgInfo.getSecond().getOperationTimeMs()), putMsgInfo.getFirst());
      idsToBeIgnored.add(id);

      // add 3 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3);

      accountId = Utils.getRandomShort(TestUtils.RANDOM);
      containerId = Utils.getRandomShort(TestUtils.RANDOM);
      toEncrypt = TestUtils.RANDOM.nextBoolean();
      // add a corrupt message to the remote host only
      id = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
          containerId, partitionId, toEncrypt);
      putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      byte[] data = putMsgInfo.getFirst().array();
      // flip every bit in the array
      for (int j = 0; j < data.length; j++) {
        data[j] ^= 0xFF;
      }
      remoteHost.addMessage(partitionId, putMsgInfo.getSecond(), putMsgInfo.getFirst());
      idsToBeIgnored.add(id);

      // add 3 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3);

      // add delete record for the very first blob in the remote host only
      addDeleteMessagesToReplicasOfPartition(partitionId, toDeleteId, Collections.singletonList(remoteHost));
      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost));
      idsToBeIgnored.add(id);

      // add 2 or 3 messages (depending on whether partition is even-numbered or odd-numbered) to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), i % 2 == 0 ? 2 : 3);
      idsToBeIgnoredByPartition.put(partitionId, idsToBeIgnored);

      // ensure that the first key is not deleted in the local host
      assertNull(toDeleteId + " should not be deleted in the local host",
          getMessageInfo(toDeleteId, localHost.infosByPartition.get(partitionId), true));
    }

    Properties properties = new Properties();
    properties.put("replication.wait.time.between.replicas.ms", "0");
    ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(properties));
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populatePerColoMetrics(Collections.singleton(remoteHost.dataNodeId.getDatacenterName()));
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<>();
    replicasToReplicate.put(remoteHost.dataNodeId, localHost.getRemoteReplicaInfos(remoteHost, null));
    Map<DataNodeId, Host> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    int batchSize = 4;
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);

    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, config, replicationMetrics, null,
            storeKeyFactory, true, clusterMap.getMetricRegistry(), false, localHost.dataNodeId.getDatacenterName(),
            new ResponseHandler(clusterMap));

    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      if (partitionIds.indexOf(entry.getKey()) % 2 == 0) {
        assertEquals("Missing buffers count mismatch", 13, entry.getValue().size());
      } else {
        assertEquals("Missing buffers count mismatch", 14, entry.getValue().size());
      }
    }

    // 1st and 2nd iterations - no keys missing because all data is in both hosts
    // 3rd iteration - 3 missing keys (one expired)
    // 4th iteration - 3 missing keys (one expired) - the corrupt key also shows up as missing but is ignored later
    // 5th iteration - 1 missing key (1 key from prev cycle, 1 deleted key, 1 never present key but deleted in remote)
    // 6th iteration - 2 missing keys (2 entries i.e put,delete of never present key)
    int[] missingKeysCounts = {0, 0, 3, 3, 1, 2};
    int[] missingBuffersCount = {12, 12, 9, 7, 6, 4};
    int expectedIndex = 0;
    int missingBuffersIndex = 0;

    for (int missingKeysCount : missingKeysCounts) {
      expectedIndex += (batchSize - 1);
      List<ReplicaThread.ExchangeMetadataResponse> response =
          replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
              replicasToReplicate.get(remoteHost.dataNodeId));
      assertEquals("Response should contain a response for each replica",
          replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
      for (int i = 0; i < response.size(); i++) {
        assertEquals(missingKeysCount, response.get(i).missingStoreKeys.size());
        assertEquals(expectedIndex, ((MockFindToken) response.get(i).remoteToken).getIndex());
        replicasToReplicate.get(remoteHost.dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }
      replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize),
          replicasToReplicate.get(remoteHost.dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
            replicasToReplicate.get(remoteHost.dataNodeId).get(i).getToken());
      }
      missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
        if (partitionIds.indexOf(entry.getKey()) % 2 == 0) {
          assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
              missingBuffersCount[missingBuffersIndex], entry.getValue().size());
        } else {
          assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
              missingBuffersCount[missingBuffersIndex] + 1, entry.getValue().size());
        }
      }
      missingBuffersIndex++;
    }

    // Test the case where some partitions have missing keys, but not all.
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      if (i % 2 == 0) {
        assertEquals(0, response.get(i).missingStoreKeys.size());
        assertEquals(expectedIndex, ((MockFindToken) response.get(i).remoteToken).getIndex());
      } else {
        assertEquals(1, response.get(i).missingStoreKeys.size());
        assertEquals(expectedIndex + 1, ((MockFindToken) response.get(i).remoteToken).getIndex());
      }
    }
    replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize),
        replicasToReplicate.get(remoteHost.dataNodeId), response);
    for (int i = 0; i < response.size(); i++) {
      assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
          replicasToReplicate.get(remoteHost.dataNodeId).get(i).getToken());
    }

    // no more missing keys
    response = replicaThread.exchangeMetadata(new MockConnection(remoteHost, 4),
        replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(0, response.get(i).missingStoreKeys.size());
      assertEquals(i % 2 == 0 ? expectedIndex : expectedIndex + 1,
          ((MockFindToken) response.get(i).remoteToken).getIndex());
    }

    Map<PartitionId, List<MessageInfo>> missingInfos = remoteHost.getMissingInfos(localHost.infosByPartition);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      // test that the first key has been marked deleted
      List<MessageInfo> messageInfos = localHost.infosByPartition.get(entry.getKey());
      StoreKey deletedId = messageInfos.get(0).getStoreKey();
      assertNotNull(deletedId + " should have been deleted", getMessageInfo(deletedId, messageInfos, true));
      Map<StoreKey, Boolean> ignoreState = new HashMap<>();
      for (StoreKey toBeIgnored : idsToBeIgnoredByPartition.get(entry.getKey())) {
        ignoreState.put(toBeIgnored, false);
      }
      for (MessageInfo messageInfo : entry.getValue()) {
        StoreKey id = messageInfo.getStoreKey();
        if (!messageInfo.getStoreKey().equals(deletedId)) {
          assertTrue("Message should be eligible to be ignored", ignoreState.containsKey(id));
          ignoreState.put(id, true);
        }
      }
      for (Map.Entry<StoreKey, Boolean> stateInfo : ignoreState.entrySet()) {
        assertTrue(stateInfo.getKey() + " should have been ignored", stateInfo.getValue());
      }
    }
    missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      // 1 expired + 1 corrupt + 1 put (never present) + 1 deleted (never present)
      assertEquals(4, entry.getValue().size());
    }
  }

  /**
   * Tests that replica tokens are set correctly and go through different stages correctly.
   * @throws InterruptedException
   */
  @Test
  public void replicaTokenTest() throws InterruptedException {
    final long tokenPersistInterval = 100;
    Time time = new MockTime();
    MockFindToken token1 = new MockFindToken(0, 0);
    RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(new MockReplicaId(), new MockReplicaId(),
        new MockStore(null, Collections.EMPTY_LIST, Collections.EMPTY_LIST, null), token1, tokenPersistInterval, time,
        new Port(5000, PortType.PLAINTEXT));

    // The equality check is for the reference, which is fine.
    // Initially, the current token and the token to persist are the same.
    assertEquals(token1, remoteReplicaInfo.getToken());
    assertEquals(token1, remoteReplicaInfo.getTokenToPersist());
    MockFindToken token2 = new MockFindToken(100, 100);

    remoteReplicaInfo.initializeTokens(token2);
    // Both tokens should be the newly initialized token.
    assertEquals(token2, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token3 = new MockFindToken(200, 200);

    remoteReplicaInfo.setToken(token3);
    // Token to persist should still be the old token.
    assertEquals(token3, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    // Sleep for shorter than token persist interval.
    time.sleep(tokenPersistInterval - 1);
    // Token to persist should still be the old token.
    assertEquals(token3, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token4 = new MockFindToken(200, 200);
    remoteReplicaInfo.setToken(token4);

    time.sleep(2);
    // Token to persist should be the most recent token as of currentTime - tokenToPersistInterval
    // which is token3 at this time.
    assertEquals(token4, remoteReplicaInfo.getToken());
    assertEquals(token3, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    time.sleep(tokenPersistInterval + 1);
    // The most recently set token as of currentTime - tokenToPersistInterval is token4
    assertEquals(token4, remoteReplicaInfo.getToken());
    assertEquals(token4, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();
  }

  /**
   * For the given partitionId, constructs put messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param hosts the list of {@link Host} all of which will be populated with the messages.
   * @param count the number of messages to construct and add.
   * @return the list of blobs ids that were generated.
   * @throws MessageFormatException
   * @throws IOException
   */
  private List<StoreKey> addPutMessagesToReplicasOfPartition(PartitionId partitionId, List<Host> hosts, int count)
      throws MessageFormatException, IOException {
    List<StoreKey> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      boolean toEncrypt = i % 2 == 0;
      BlobId id = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
          containerId, partitionId, toEncrypt);
      ids.add(id);
      Pair<ByteBuffer, MessageInfo> putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      for (Host host : hosts) {
        host.addMessage(partitionId, putMsgInfo.getSecond(), putMsgInfo.getFirst().duplicate());
      }
    }
    return ids;
  }

  /**
   * For the given partitionId, constructs delete messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param id the {@link StoreKey} to create a delete message for.
   * @param hosts the list of {@link Host} all of which will be populated with the messages.
   * @throws MessageFormatException
   * @throws IOException
   */
  private void addDeleteMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id, List<Host> hosts)
      throws MessageFormatException, IOException {
    MessageInfo putMsg = getMessageInfo(id, hosts.get(0).infosByPartition.get(partitionId), false);
    long deletionTimeMs = System.currentTimeMillis();
    ByteBuffer buffer = getDeleteMessage(id, putMsg.getAccountId(), putMsg.getContainerId(), deletionTimeMs);
    for (Host host : hosts) {
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), true, putMsg.getAccountId(), putMsg.getContainerId(), deletionTimeMs),
          buffer.duplicate());
    }
  }

  /**
   * Constructs an entire message with header, blob properties, user metadata and blob content.
   * @param id id for which the message has to be constructed.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param enableEncryption {@code true} if encryption needs to be enabled. {@code false} otherwise
   * @return a {@link Pair} of {@link ByteBuffer} and {@link MessageInfo} representing the entire message and the
   *         associated {@link MessageInfo}
   * @throws MessageFormatException
   * @throws IOException
   */
  private Pair<ByteBuffer, MessageInfo> getPutMessage(StoreKey id, short accountId, short containerId,
      boolean enableEncryption) throws MessageFormatException, IOException {
    int blobSize = TestUtils.RANDOM.nextInt(500) + 501;
    int userMetadataSize = TestUtils.RANDOM.nextInt(blobSize / 2);
    int encryptionKeySize = TestUtils.RANDOM.nextInt(blobSize / 4);
    byte[] blob = new byte[blobSize];
    byte[] usermetadata = new byte[userMetadataSize];
    byte[] encryptionKey = enableEncryption ? new byte[encryptionKeySize] : null;
    TestUtils.RANDOM.nextBytes(blob);
    TestUtils.RANDOM.nextBytes(usermetadata);
    BlobProperties blobProperties = new BlobProperties(blobSize, "test", accountId, containerId, encryptionKey != null);

    MessageFormatInputStream stream =
        new PutMessageFormatInputStream(id, encryptionKey == null ? null : ByteBuffer.wrap(encryptionKey),
            blobProperties, ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), blobSize);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return new Pair<>(ByteBuffer.wrap(message),
        new MessageInfo(id, message.length, Utils.Infinite_Time, accountId, containerId,
            blobProperties.getCreationTimeInMs()));
  }

  /**
   * Returns a delete message for the given {@code id}
   * @param id the id for which a delete message must be constructed.
   * @return {@link ByteBuffer} representing the entire message.
   * @throws MessageFormatException
   * @throws IOException
   */
  private ByteBuffer getDeleteMessage(StoreKey id, short accountId, short containerId, long deletionTimeMs)
      throws MessageFormatException, IOException {
    MessageFormatInputStream stream = new DeleteMessageFormatInputStream(id, accountId, containerId, deletionTimeMs);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return ByteBuffer.wrap(message);
  }

  /**
   * Gets the {@link MessageInfo} for {@code id} if present.
   * @param id the {@link StoreKey} to look for.
   * @param messageInfos the {@link MessageInfo} list.
   * @param deleteMsg {@code true} if delete msg if requested. {@code false} otherwise
   * @return the delete {@link MessageInfo} if it exists in {@code messageInfos}. {@code null otherwise.}
   */
  private static MessageInfo getMessageInfo(StoreKey id, List<MessageInfo> messageInfos, boolean deleteMsg) {
    MessageInfo toRet = null;
    for (MessageInfo messageInfo : messageInfos) {
      if (messageInfo.getStoreKey().equals(id)) {
        if (deleteMsg == messageInfo.isDeleted()) {
          toRet = messageInfo;
          break;
        }
      }
    }
    return toRet;
  }

  /**
   * We can have duplicate entries in the message entries since updates can happen to the same key. For example,
   * insert a key followed by a delete. This would create two entries in the journal or the index. A single findInfo
   * could read both the entries. The findInfo should return as clean information as possible. This method removes
   * the oldest duplicate in the list.
   * @param messageEntries The message entry list where duplicates need to be removed
   */
  private static void eliminateDuplicates(List<MessageInfo> messageEntries) {
    Set<StoreKey> setToFindDuplicate = new HashSet<StoreKey>();
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator(messageEntries.size());
    while (messageEntriesIterator.hasPrevious()) {
      MessageInfo messageInfo = messageEntriesIterator.previous();
      if (setToFindDuplicate.contains(messageInfo.getStoreKey())) {
        messageEntriesIterator.remove();
      } else {
        setToFindDuplicate.add(messageInfo.getStoreKey());
      }
    }
  }

  /**
   * Interface to help perform actions on store events.
   */
  interface StoreEventListener {
    void onPut(MockStore store, List<MessageInfo> messageInfos);
  }

  /**
   * Representation of a host. Contains all the data for all partitions.
   */
  class Host {
    private final ClusterMap clusterMap;
    private final Map<PartitionId, MockStore> storesByPartition = new HashMap<>();

    final DataNodeId dataNodeId;
    final Map<PartitionId, List<MessageInfo>> infosByPartition = new HashMap<>();
    final Map<PartitionId, List<ByteBuffer>> buffersByPartition = new HashMap<>();

    Host(DataNodeId dataNodeId, ClusterMap clusterMap) {
      this.dataNodeId = dataNodeId;
      this.clusterMap = clusterMap;
    }

    /**
     * Adds a message to {@code id} with the provided details.
     * @param id the {@link PartitionId} to add the message to.
     * @param messageInfo the {@link MessageInfo} of the message.
     * @param buffer the data accompanying the message.
     */
    void addMessage(PartitionId id, MessageInfo messageInfo, ByteBuffer buffer) {
      List<MessageInfo> existingInfos = infosByPartition.computeIfAbsent(id, id1 -> new ArrayList<>());
      existingInfos.add(messageInfo);
      List<ByteBuffer> existingBuffers = buffersByPartition.computeIfAbsent(id, id1 -> new ArrayList<>());
      existingBuffers.add(buffer);
    }

    /**
     * Gets the list of {@link RemoteReplicaInfo} from this host to the given {@code remoteHost}
     * @param remoteHost the host whose replica info is required.
     * @param listener the {@link StoreEventListener} to use.
     * @return the list of {@link RemoteReplicaInfo} from this host to the given {@code remoteHost}
     */
    List<RemoteReplicaInfo> getRemoteReplicaInfos(Host remoteHost, StoreEventListener listener) {
      List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
      for (ReplicaId replicaId : replicaIds) {
        for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
          if (peerReplicaId.getDataNodeId().equals(remoteHost.dataNodeId)) {
            PartitionId partitionId = replicaId.getPartitionId();
            MockStore store = storesByPartition.computeIfAbsent(partitionId, partitionId1 -> new MockStore(partitionId,
                infosByPartition.computeIfAbsent(partitionId1,
                    (Function<PartitionId, List<MessageInfo>>) partitionId2 -> new ArrayList<>()),
                buffersByPartition.computeIfAbsent(partitionId1,
                    (Function<PartitionId, List<ByteBuffer>>) partitionId22 -> new ArrayList<>()), listener));
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(peerReplicaId, replicaId, store, new MockFindToken(0, 0), Long.MAX_VALUE,
                    SystemTime.getInstance(), new Port(peerReplicaId.getDataNodeId().getPort(), PortType.PLAINTEXT));
            remoteReplicaInfos.add(remoteReplicaInfo);
          }
        }
      }
      return remoteReplicaInfos;
    }

    /**
     * Gets the message infos that are present in this host but missing in {@code other}.
     * @param other the list of {@link MessageInfo} to check against.
     * @return the message infos that are present in this host but missing in {@code other}.
     */
    Map<PartitionId, List<MessageInfo>> getMissingInfos(Map<PartitionId, List<MessageInfo>> other) {
      Map<PartitionId, List<MessageInfo>> missingInfos = new HashMap<>();
      for (Map.Entry<PartitionId, List<MessageInfo>> entry : infosByPartition.entrySet()) {
        PartitionId partitionId = entry.getKey();
        for (MessageInfo messageInfo : entry.getValue()) {
          boolean found = false;
          for (MessageInfo otherInfo : other.get(partitionId)) {
            if (messageInfo.getStoreKey().equals(otherInfo.getStoreKey())
                && messageInfo.isDeleted() == otherInfo.isDeleted()) {
              found = true;
              break;
            }
          }
          if (!found) {
            missingInfos.computeIfAbsent(partitionId, partitionId1 -> new ArrayList<>()).add(messageInfo);
          }
        }
      }
      return missingInfos;
    }

    /**
     * Gets the buffers that are present in this host but missing in {@code other}.
     * @param other the list of {@link ByteBuffer} to check against.
     * @return the buffers that are present in this host but missing in {@code other}.
     */
    Map<PartitionId, List<ByteBuffer>> getMissingBuffers(Map<PartitionId, List<ByteBuffer>> other) {
      Map<PartitionId, List<ByteBuffer>> missingBuffers = new HashMap<>();
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : buffersByPartition.entrySet()) {
        PartitionId partitionId = entry.getKey();
        for (ByteBuffer buf : entry.getValue()) {
          boolean found = false;
          for (ByteBuffer bufActual : other.get(partitionId)) {
            if (Arrays.equals(buf.array(), bufActual.array())) {
              found = true;
              break;
            }
          }
          if (!found) {
            missingBuffers.computeIfAbsent(partitionId, partitionId1 -> new ArrayList<>()).add(buf);
          }
        }
      }
      return missingBuffers;
    }
  }

  /**
   * A mock implementation of {@link Store} that store all details in memory.
   */
  class MockStore implements Store {

    class MockMessageReadSet implements MessageReadSet {

      // NOTE: all the functions in MockMessageReadSet are currently not used.

      private final List<ByteBuffer> buffers;
      private final List<StoreKey> storeKeys;

      MockMessageReadSet(List<ByteBuffer> buffers, List<StoreKey> storeKeys) {
        this.buffers = buffers;
        this.storeKeys = storeKeys;
      }

      @Override
      public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize)
          throws IOException {
        ByteBuffer bufferToWrite = buffers.get(index);
        int savedPos = bufferToWrite.position();
        int savedLimit = bufferToWrite.limit();
        bufferToWrite.position((int) relativeOffset);
        bufferToWrite.limit((int) Math.min(maxSize + relativeOffset, bufferToWrite.capacity()));
        int sizeToWrite = bufferToWrite.remaining();
        while (bufferToWrite.hasRemaining()) {
          channel.write(bufferToWrite);
        }
        bufferToWrite.position(savedPos);
        bufferToWrite.limit(savedLimit);
        return sizeToWrite;
      }

      @Override
      public int count() {
        return buffers.size();
      }

      @Override
      public long sizeInBytes(int index) {
        return buffers.get(index).limit();
      }

      @Override
      public StoreKey getKeyAt(int index) {
        return storeKeys.get(index);
      }
    }

    /**
     * Log that stores all data in memory.
     */
    class DummyLog implements Write {
      private final List<ByteBuffer> blobs;
      private long endOffSet;

      DummyLog(List<ByteBuffer> initialBlobs) {
        this.blobs = initialBlobs;
      }

      @Override
      public int appendFrom(ByteBuffer buffer) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(buffer.remaining());
        buf.put(buffer);
        buf.flip();
        storeBuf(buf);
        return buf.capacity();
      }

      @Override
      public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate((int) size);
        int sizeRead = 0;
        while (sizeRead < size) {
          sizeRead += channel.read(buf);
        }
        buf.flip();
        storeBuf(buf);
      }

      ByteBuffer getData(int index) {
        return blobs.get(index).duplicate();
      }

      long getEndOffSet() {
        return endOffSet;
      }

      private void storeBuf(ByteBuffer buffer) {
        blobs.add(buffer);
        endOffSet += buffer.capacity();
      }
    }

    private final StoreEventListener listener;
    private final DummyLog log;
    final List<MessageInfo> messageInfos;
    final PartitionId id;

    MockStore(PartitionId id, List<MessageInfo> messageInfos, List<ByteBuffer> buffers, StoreEventListener listener) {
      if (messageInfos.size() != buffers.size()) {
        throw new IllegalArgumentException("message info size and buffer size does not match");
      }
      this.messageInfos = messageInfos;
      log = new DummyLog(buffers);
      this.listener = listener;
      this.id = id;
    }

    @Override
    public void start() throws StoreException {
    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> getOptions) throws StoreException {
      // unused function
      List<MessageInfo> infos = new ArrayList<>();
      List<ByteBuffer> buffers = new ArrayList<>();
      List<StoreKey> keys = new ArrayList<>();
      for (StoreKey id : ids) {
        for (int i = 0; i < messageInfos.size(); i++) {
          MessageInfo info = messageInfos.get(i);
          if (info.getStoreKey().equals(id)) {
            infos.add(info);
            buffers.add(log.getData(i));
            keys.add(info.getStoreKey());
          }
        }
      }
      return new StoreInfo(new MockMessageReadSet(buffers, keys), infos);
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
      List<MessageInfo> newInfos = messageSetToWrite.getMessageSetInfo();
      try {
        messageSetToWrite.writeTo(log);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      messageInfos.addAll(newInfos);
      if (listener != null) {
        listener.onPut(this, newInfos);
      }
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
      for (MessageInfo deleteInfo : messageSetToDelete.getMessageSetInfo()) {
        int index = 0;
        MessageInfo messageInfoFound = null;
        for (MessageInfo messageInfo : messageInfos) {
          if (messageInfo.getStoreKey().equals(deleteInfo.getStoreKey())) {
            messageInfoFound = messageInfo;
            break;
          }
          index++;
        }
        if (index >= messageInfos.size()) {
          throw new StoreException("Did not find " + deleteInfo.getStoreKey(), StoreErrorCodes.ID_Not_Found);
        }
        try {
          messageSetToDelete.writeTo(log);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        messageInfos.add(new MessageInfo(deleteInfo.getStoreKey(), deleteInfo.getSize(), true,
            messageInfoFound.getExpirationTimeInMs(), messageInfoFound.getAccountId(),
            messageInfoFound.getContainerId(), System.currentTimeMillis()));
      }
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries) throws StoreException {
      // unused function
      MockFindToken mockToken = (MockFindToken) token;
      List<MessageInfo> entriesToReturn = new ArrayList<>();
      long currentSizeOfEntriesInBytes = 0;
      int index = mockToken.getIndex();
      while (currentSizeOfEntriesInBytes < maxSizeOfEntries && index < messageInfos.size()) {
        MessageInfo messageInfo = messageInfos.get(index);
        MessageInfo deleteInfo = getMessageInfo(messageInfo.getStoreKey(), messageInfos, true);
        entriesToReturn.add(deleteInfo == null ? messageInfo : deleteInfo);
        // still use the size of the put (if the original picked up is the put.
        currentSizeOfEntriesInBytes += messageInfos.get(index).getSize();
        index++;
      }

      int startIndex = mockToken.getIndex();
      int totalSizeRead = 0;
      for (int i = 0; i < startIndex; i++) {
        totalSizeRead += messageInfos.get(i).getSize();
      }
      totalSizeRead += currentSizeOfEntriesInBytes;
      eliminateDuplicates(entriesToReturn);
      return new FindInfo(entriesToReturn,
          new MockFindToken(mockToken.getIndex() + entriesToReturn.size(), totalSizeRead));
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
      Set<StoreKey> keysMissing = new HashSet<>();
      for (StoreKey key : keys) {
        boolean found = false;
        for (MessageInfo messageInfo : messageInfos) {
          if (messageInfo.getStoreKey().equals(key)) {
            found = true;
            break;
          }
        }
        if (!found) {
          keysMissing.add(key);
        }
      }
      return keysMissing;
    }

    @Override
    public StoreStats getStoreStats() {
      return null;
    }

    @Override
    public boolean isKeyDeleted(StoreKey key) throws StoreException {
      return getMessageInfo(key, messageInfos, true) != null;
    }

    @Override
    public long getSizeInBytes() {
      return log.getEndOffSet();
    }

    @Override
    public void shutdown() throws StoreException {
    }
  }

  /**
   * Implementation of {@link ConnectedChannel} that fetches message infos or blobs based on the type of request.
   */
  class MockConnection implements ConnectedChannel {

    class MockSend implements Send {

      private final List<ByteBuffer> buffers;
      private final int size;
      private int index;

      MockSend(List<ByteBuffer> buffers) {
        this.buffers = buffers;
        index = 0;
        int runningSize = 0;
        for (ByteBuffer buffer : buffers) {
          runningSize += buffer.remaining();
        }
        size = runningSize;
      }

      @Override
      public long writeTo(WritableByteChannel channel) throws IOException {
        ByteBuffer bufferToWrite = buffers.get(index);
        index++;
        int savedPos = bufferToWrite.position();
        long written = channel.write(bufferToWrite);
        bufferToWrite.position(savedPos);
        return written;
      }

      @Override
      public boolean isSendComplete() {
        return buffers.size() == index;
      }

      @Override
      public long sizeInBytes() {
        return size;
      }
    }

    private final Host host;
    private final int maxSizeToReturn;

    private List<ByteBuffer> buffersToReturn;
    private Map<PartitionId, List<MessageInfo>> infosToReturn;
    private Map<PartitionId, List<MessageMetadata>> messageMetadatasToReturn;
    private ReplicaMetadataRequest metadataRequest;
    private GetRequest getRequest;

    MockConnection(Host host, int maxSizeToReturn) {
      this.host = host;
      this.maxSizeToReturn = maxSizeToReturn;
    }

    @Override
    public void send(Send request) throws IOException {
      if (request instanceof ReplicaMetadataRequest) {
        metadataRequest = (ReplicaMetadataRequest) request;
      } else if (request instanceof GetRequest) {
        getRequest = (GetRequest) request;
        buffersToReturn = new ArrayList<>();
        infosToReturn = new HashMap<>();
        messageMetadatasToReturn = new HashMap<>();
        boolean requestIsEmpty = true;
        for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
          PartitionId partitionId = partitionRequestInfo.getPartition();
          List<ByteBuffer> bufferList = host.buffersByPartition.get(partitionId);
          List<MessageInfo> messageInfoList = host.infosByPartition.get(partitionId);
          infosToReturn.put(partitionId, new ArrayList<>());
          messageMetadatasToReturn.put(partitionId, new ArrayList<>());
          for (StoreKey key : partitionRequestInfo.getBlobIds()) {
            requestIsEmpty = false;
            int index = 0;
            for (MessageInfo info : messageInfoList) {
              if (key.equals(info.getStoreKey())) {
                infosToReturn.get(partitionId).add(info);
                messageMetadatasToReturn.get(partitionId).add(null);
                buffersToReturn.add(bufferList.get(index));
              }
              index++;
            }
          }
        }
        assertFalse("Replication should not make empty GetRequests", requestIsEmpty);
      }
    }

    @Override
    public ChannelOutput receive() throws IOException {
      Response response;
      if (metadataRequest != null) {
        List<ReplicaMetadataResponseInfo> responseInfoList = new ArrayList<>();
        for (ReplicaMetadataRequestInfo requestInfo : metadataRequest.getReplicaMetadataRequestInfoList()) {
          List<MessageInfo> messageInfosToReturn = new ArrayList<>();
          List<MessageInfo> partitionInfos = host.infosByPartition.get(requestInfo.getPartitionId());
          int startIndex = ((MockFindToken) (requestInfo.getToken())).getIndex();
          int endIndex = Math.min(partitionInfos.size(), startIndex + maxSizeToReturn);
          int indexRequested = 0;
          for (int i = startIndex; i < endIndex; i++) {
            MessageInfo messageInfo = partitionInfos.get(i);
            MessageInfo deleteInfo = getMessageInfo(messageInfo.getStoreKey(), partitionInfos, true);
            messageInfosToReturn.add(deleteInfo == null ? messageInfo : deleteInfo);
            indexRequested = i;
          }
          eliminateDuplicates(messageInfosToReturn);
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(requestInfo.getPartitionId(),
                  new MockFindToken(indexRequested, requestInfo.getToken().getBytesRead()), messageInfosToReturn, 0);
          responseInfoList.add(replicaMetadataResponseInfo);
        }
        response = new ReplicaMetadataResponse(1, "replicametadata", ServerErrorCode.No_Error, responseInfoList);
        metadataRequest = null;
      } else {
        List<PartitionResponseInfo> responseInfoList = new ArrayList<>();
        for (PartitionRequestInfo requestInfo : getRequest.getPartitionInfoList()) {
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(requestInfo.getPartition(), infosToReturn.get(requestInfo.getPartition()),
                  messageMetadatasToReturn.get(requestInfo.getPartition()));
          responseInfoList.add(partitionResponseInfo);
        }
        response = new GetResponse(1, "replication", responseInfoList, new MockSend(buffersToReturn),
            ServerErrorCode.No_Error);
        getRequest = null;
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) response.sizeInBytes());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
      WritableByteChannel channel = Channels.newChannel(stream);
      while (!response.isSendComplete()) {
        response.writeTo(channel);
      }
      buffer.flip();
      buffer.getLong();
      return new ChannelOutput(new ByteBufferInputStream(buffer), buffer.remaining());
    }

    @Override
    public String getRemoteHost() {
      return host.dataNodeId.getHostname();
    }

    @Override
    public int getRemotePort() {
      return host.dataNodeId.getPort();
    }
  }

  /**
   * Implementation of {@link ConnectionPool} that hands out {@link MockConnection}s.
   */
  class MockConnectionPool implements ConnectionPool {
    private final Map<DataNodeId, Host> hosts;
    private final ClusterMap clusterMap;
    private final int maxEntriesToReturn;

    MockConnectionPool(Map<DataNodeId, Host> hosts, ClusterMap clusterMap, int maxEntriesToReturn) {
      this.hosts = hosts;
      this.clusterMap = clusterMap;
      this.maxEntriesToReturn = maxEntriesToReturn;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public ConnectedChannel checkOutConnection(String host, Port port, long timeout)
        throws IOException, InterruptedException, ConnectionPoolTimeoutException {
      DataNodeId dataNodeId = clusterMap.getDataNodeId(host, port.getPort());
      Host hostObj = hosts.get(dataNodeId);
      return new MockConnection(hostObj, maxEntriesToReturn);
    }

    @Override
    public void checkInConnection(ConnectedChannel connectedChannel) {
    }

    @Override
    public void destroyConnection(ConnectedChannel connectedChannel) {
    }
  }
}
