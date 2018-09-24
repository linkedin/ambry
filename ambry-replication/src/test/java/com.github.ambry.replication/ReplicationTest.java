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
import com.github.ambry.commons.BlobIdFactory;
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
import com.github.ambry.messageformat.TtlUpdateMessageFormatInputStream;
import com.github.ambry.messageformat.ValidatingTransformer;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.GetOption;
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
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreStats;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for ReplicaThread
 */
public class ReplicationTest {

  private static int CONSTANT_TIME_MS = 100000;
  private static long EXPIRY_TIME_MS = SystemTime.getInstance().milliseconds() + TimeUnit.DAYS.toMillis(7);
  private static long UPDATED_EXPIRY_TIME_MS = SystemTime.getInstance().milliseconds() + TimeUnit.DAYS.toMillis(14);
  private final MockTime time = new MockTime();
  private ReplicationConfig config;

  /**
   * Constructor to set the configs
   */
  public ReplicationTest() {
    Properties properties = new Properties();
    properties.setProperty("replication.synced.replica.backoff.duration.ms", "3000");
    properties.setProperty("replication.intra.replica.thread.throttle.sleep.duration.ms", "100");
    properties.setProperty("replication.inter.replica.thread.throttle.sleep.duration.ms", "200");
    properties.setProperty("replication.replica.thread.idle.sleep.duration.ms", "1000");
    config = new ReplicationConfig(new VerifiableProperties(properties));
  }

  /**
   * Tests pausing all partitions and makes sure that the replica thread pauses. Also tests that it resumes when one
   * eligible partition is reenabled and that replication completes successfully.
   * @throws Exception
   */
  @Test
  public void replicationAllPauseTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add  10 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    MockStoreKeyConverterFactory mockStoreKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    mockStoreKeyConverterFactory.setReturnInputIfAbsent(true);
    mockStoreKeyConverterFactory.setConversionMap(new HashMap<>());

    int batchSize = 4;
    StoreKeyConverter storeKeyConverter = mockStoreKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);
    CountDownLatch readyToPause = new CountDownLatch(1);
    CountDownLatch readyToProceed = new CountDownLatch(1);
    AtomicReference<CountDownLatch> reachedLimitLatch = new AtomicReference<>(new CountDownLatch(1));
    AtomicReference<Exception> exception = new AtomicReference<>();
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            (store, messageInfos) -> {
              try {
                readyToPause.countDown();
                readyToProceed.await();
                if (store.messageInfos.size() == remoteHost.infosByPartition.get(store.id).size()) {
                  reachedLimitLatch.get().countDown();
                }
              } catch (Exception e) {
                exception.set(e);
              }
            });
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();
    Thread thread = Utils.newThread(replicaThread, false);
    thread.start();

    assertEquals("There should be no disabled partitions", 0, replicaThread.getReplicationDisabledPartitions().size());
    // wait to pause replication
    readyToPause.await(10, TimeUnit.SECONDS);
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), false);
    Set<PartitionId> expectedPaused = new HashSet<>(clusterMap.getAllPartitionIds(null));
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
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), true);
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
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add  10 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    Map<PartitionId, Integer> progressTracker = new HashMap<>();
    PartitionId idToLeaveOut = clusterMap.getAllPartitionIds(null).get(0);
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
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), false);
        expectedPaused.addAll(clusterMap.getAllPartitionIds(null));
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
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), true);
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
   * Tests that replication between a local and remote server who have different
   * blob IDs for the same blobs (via StoreKeyConverter)
   * @throws Exception
   */
  @Test
  public void replicaThreadTestConverter() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();
    Host expectedLocalHost = new Host(localHost.dataNodeId, clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();


        /*
        STORE KEY CONVERTER MAPPING
        Key     Value
        B0      B0'
        B1      B1'
        B2      null

        BEFORE
        Local   Remote
        B0'     B0
                B1
                B2

        AFTER
        Local   Remote
        B0'     B0
        B1'     B1
                B2
        B0 is B0' for local,
        B1 is B1' for local,
        B2 is null for local,
        so it already has B0/B0'
        B1 is transferred to B1'
        and B2 is invalid for L
        so it does not count as missing
        Missing Keys: 1

     */
    Map<PartitionId, List<BlobId>> partitionIdToDeleteBlobId = new HashMap<>();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    Map<PartitionId, BlobId> expectedPartitionIdToDeleteBlobId = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<BlobId> deleteBlobIds = new ArrayList<>();
      partitionIdToDeleteBlobId.put(partitionId, deleteBlobIds);
      BlobId b0 = generateRandomBlobId(partitionId);
      deleteBlobIds.add(b0);
      BlobId b0p = generateRandomBlobId(partitionId);
      expectedPartitionIdToDeleteBlobId.put(partitionId, b0p);
      BlobId b1 = generateRandomBlobId(partitionId);
      BlobId b1p = generateRandomBlobId(partitionId);
      BlobId b2 = generateRandomBlobId(partitionId);
      deleteBlobIds.add(b2);
      conversionMap.put(b0, b0p);
      conversionMap.put(b1, b1p);
      conversionMap.put(b2, null);
      //Convert current conversion map so that BlobIdTransformer can
      //create b1p in expectedLocalHost
      storeKeyConverter.setConversionMap(conversionMap);
      storeKeyConverter.convert(conversionMap.keySet());
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p), Arrays.asList(localHost));
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0, b1, b2), Arrays.asList(remoteHost));
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p, b1), Arrays.asList(null, transformer),
          Arrays.asList(expectedLocalHost));

      //Check that expected local host contains the correct blob ids
      Set<BlobId> expectedLocalHostBlobIds = new HashSet<>();
      expectedLocalHostBlobIds.add(b0p);
      expectedLocalHostBlobIds.add(b1p);
      for (MessageInfo messageInfo : expectedLocalHost.infosByPartition.get(partitionId)) {
        assertTrue("Remove should never fail", expectedLocalHostBlobIds.remove(messageInfo.getStoreKey()));
      }
      assertTrue("expectedLocalHostBlobIds should now be empty", expectedLocalHostBlobIds.isEmpty());
    }
    storeKeyConverter.setConversionMap(conversionMap);

    int expectedIndex =
        assertMissingKeysAndFixMissingStoreKeys(0, 2, batchSize, 1, replicaThread, remoteHost, replicasToReplicate);

    //Check that there are no missing buffers between expectedLocalHost and LocalHost
    Map<PartitionId, List<ByteBuffer>> missingBuffers =
        expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());
    missingBuffers = localHost.getMissingBuffers(expectedLocalHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());

    /*
        BEFORE
        Local   Remote
        B0'     B0
        B1'     B1
                B2
                dB0 (delete B0)
                dB2

        AFTER
        Local   Remote
        B0'     B0
        B1'     B1
        dB0'    B2
                dB0
                dB2
        delete B0 gets converted
        to delete B0' in Local
        Missing Keys: 0

     */
    //delete blob
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<BlobId> deleteBlobIds = partitionIdToDeleteBlobId.get(partitionId);
      for (BlobId deleteBlobId : deleteBlobIds) {
        addDeleteMessagesToReplicasOfPartition(partitionId, deleteBlobId, Arrays.asList(remoteHost));
      }
      addDeleteMessagesToReplicasOfPartition(partitionId, expectedPartitionIdToDeleteBlobId.get(partitionId),
          Arrays.asList(expectedLocalHost));
    }

    expectedIndex = assertMissingKeysAndFixMissingStoreKeys(expectedIndex, 2, batchSize, 0, replicaThread, remoteHost,
        replicasToReplicate);

    //Check that there are no missing buffers between expectedLocalHost and LocalHost
    missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());
    missingBuffers = localHost.getMissingBuffers(expectedLocalHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());

    // 3 unconverted + 2 unconverted deleted expected missing buffers
    verifyNoMoreMissingKeysAndExpectedMissingBufferCount(remoteHost, localHost, replicaThread, replicasToReplicate,
        idsToBeIgnoredByPartition, storeKeyConverter, expectedIndex, expectedIndex, 5);
  }

  /**
   * Tests replication of TTL updates
   * @throws Exception
   */
  @Test
  public void ttlUpdateReplicationTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();
    Host expectedLocalHost = new Host(localHost.dataNodeId, clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    int numMessagesInEachPart = 0;
    Map<PartitionId, StoreKey> idsDeletedLocallyByPartition = new HashMap<>();
    List<Host> remoteHostOnly = Collections.singletonList(remoteHost);
    List<Host> expectedLocalHostOnly = Collections.singletonList(expectedLocalHost);
    List<Host> localHostAndExpectedLocalHost = Arrays.asList(localHost, expectedLocalHost);
    List<Host> remoteHostAndExpectedLocalHost = Arrays.asList(remoteHost, expectedLocalHost);
    List<Host> allHosts = Arrays.asList(localHost, expectedLocalHost, remoteHost);
    for (PartitionId pid : partitionIds) {
      // add 3 put messages to both hosts (also add to expectedLocal)
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(pid, allHosts, 3);
      // delete 1 of the messages in the local host only
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(0), localHostAndExpectedLocalHost);
      idsDeletedLocallyByPartition.put(pid, ids.get(0));
      // ttl update 1 of the messages in the local host only
      addTtlUpdateMessagesToReplicasOfPartition(pid, ids.get(1), localHostAndExpectedLocalHost);

      // remote host only
      // add 2 put messages
      ids.addAll(addPutMessagesToReplicasOfPartition(pid, remoteHostOnly, 1));
      ids.addAll(addPutMessagesToReplicasOfPartition(pid, remoteHostAndExpectedLocalHost, 1));
      // ttl update all 5 put messages
      for (int i = ids.size() - 1; i >= 0; i--) {
        List<Host> hostList = remoteHostOnly;
        if (i == 2 || i == 4) {
          hostList = remoteHostAndExpectedLocalHost;
        }
        // doing it in reverse order so that a put and ttl update arrive in the same batch
        addTtlUpdateMessagesToReplicasOfPartition(pid, ids.get(i), hostList);
      }
      // delete one of the keys that has put and ttl update on local host
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(1), remoteHostAndExpectedLocalHost);
      // delete one of the keys that has put and ttl update on remote only
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(3), remoteHostOnly);

      // add a TTL update and delete message without a put msg (compaction can create such a situation)
      BlobId id = generateRandomBlobId(pid);
      addTtlUpdateMessagesToReplicasOfPartition(pid, id, remoteHostOnly);
      addDeleteMessagesToReplicasOfPartition(pid, id, remoteHostOnly);

      // message transformation test cases
      // a blob ID with PUT and TTL update in both remote and local
      BlobId b0 = generateRandomBlobId(pid);
      BlobId b0p = generateRandomBlobId(pid);
      // a blob ID with a PUT in the local and PUT and TTL update in remote (with mapping)
      BlobId b1 = generateRandomBlobId(pid);
      BlobId b1p = generateRandomBlobId(pid);
      // a blob ID with PUT and TTL update in remote only (with mapping)
      BlobId b2 = generateRandomBlobId(pid);
      BlobId b2p = generateRandomBlobId(pid);
      // a blob ID with PUT and TTL update in remote (no mapping)
      BlobId b3 = generateRandomBlobId(pid);
      conversionMap.put(b0, b0p);
      conversionMap.put(b1, b1p);
      conversionMap.put(b2, b2p);
      conversionMap.put(b3, null);

      storeKeyConverter.convert(conversionMap.keySet());
      // add as required on local, remote and expected local
      // only PUT of b0p and b1p on local
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p, b1p), localHostAndExpectedLocalHost);
      // PUT of b0,b1,b2,b3 on remote
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0, b1, b2, b3), remoteHostOnly);
      // PUT of b0, b1, b2 expected in local at the end
      addPutMessagesToReplicasOfPartition(Collections.singletonList(b2), Collections.singletonList(transformer),
          expectedLocalHostOnly);

      // TTL update of b0 on all hosts
      addTtlUpdateMessagesToReplicasOfPartition(pid, b0p, localHostAndExpectedLocalHost);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b0, remoteHostOnly);
      // TTL update on b1, b2 and b3 on remote
      addTtlUpdateMessagesToReplicasOfPartition(pid, b1, remoteHostOnly);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b1p, expectedLocalHostOnly);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b2, remoteHostOnly);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b2p, expectedLocalHostOnly);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b3, remoteHostOnly);

      numMessagesInEachPart = remoteHost.infosByPartition.get(pid).size();
    }

    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    Map<PartitionId, List<ByteBuffer>> missingBuffers =
        expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("Missing buffers count mismatch", 7, entry.getValue().size());
    }

    // 1st iteration - 0 missing keys (3 puts already present, one put missing but del in remote, 1 ttl update will be
    // applied, 1 delete will be applied)
    // 2nd iteration - 1 missing key, 1 of which will also be ttl updated (one key with put + ttl update missing but
    // del in remote, one put and ttl update replicated)
    // 3rd iteration - 0 missing keys (1 ttl update missing but del in remote, 1 already ttl updated in iter 1, 1 key
    // already ttl updated in local, 1 key del local)
    // 4th iteration - 0 missing keys (1 key del local, 1 key already deleted, 1 key missing but del in remote, 1 key
    // with ttl update missing but del remote)
    // 5th iteration - 0 missing keys (1 key - two records - missing but del remote, 2 puts already present but TTL
    // update of one of them is applied)
    // 6th iteration - 1 missing key (put + ttl update for a key, 1 deprecated id ignored, 1 TTL update already applied)
    // 7th iteration - 0 missing keys (2 TTL updates already applied, 1 TTL update of a deprecated ID ignored)
    int[] missingKeysCounts = {0, 1, 0, 0, 0, 1, 0};
    int[] missingBuffersCount = {5, 3, 3, 3, 2, 0, 0};
    int expectedIndex = 0;
    int missingBuffersIndex = 0;

    for (int missingKeysCount : missingKeysCounts) {
      expectedIndex = Math.min(expectedIndex + batchSize, numMessagesInEachPart) - 1;
      List<ReplicaThread.ExchangeMetadataResponse> response =
          replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize), remoteReplicaInfos);
      assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
      for (int i = 0; i < response.size(); i++) {
        assertEquals(missingKeysCount, response.get(i).missingStoreKeys.size());
        assertEquals(expectedIndex, ((MockFindToken) response.get(i).remoteToken).getIndex());
        remoteReplicaInfos.get(i).setToken(response.get(i).remoteToken);
      }
      replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize), remoteReplicaInfos, response);
      for (int i = 0; i < response.size(); i++) {
        assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
            remoteReplicaInfos.get(i).getToken());
      }
      missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
        assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
            missingBuffersCount[missingBuffersIndex], entry.getValue().size());
      }
      missingBuffersIndex++;
    }

    // no more missing keys
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize), remoteReplicaInfos);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (ReplicaThread.ExchangeMetadataResponse metadata : response) {
      assertEquals(0, metadata.missingStoreKeys.size());
      assertEquals(expectedIndex, ((MockFindToken) metadata.remoteToken).getIndex());
    }

    missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertEquals("There should be no missing buffers", 0, missingBuffers.size());

    // validate everything
    for (Map.Entry<PartitionId, List<MessageInfo>> remoteInfoEntry : remoteHost.infosByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteInfoEntry.getValue();
      List<MessageInfo> localInfos = localHost.infosByPartition.get(remoteInfoEntry.getKey());
      Set<StoreKey> seen = new HashSet<>();
      for (MessageInfo remoteInfo : remoteInfos) {
        StoreKey remoteId = remoteInfo.getStoreKey();
        if (seen.add(remoteId)) {
          StoreKey localId = storeKeyConverter.convert(Collections.singleton(remoteId)).get(remoteId);
          MessageInfo localInfo = getMessageInfo(localId, localInfos, false, false);
          if (localId == null) {
            // this is a deprecated ID. There should be no messages locally
            assertNull(remoteId + " is deprecated and should have no entries", localInfo);
          } else {
            MessageInfo mergedRemoteInfo = getMergedMessageInfo(remoteId, remoteInfos);
            if (localInfo == null) {
              // local has no put, must be deleted on remote
              assertTrue(localId + ":" + remoteId + " not replicated", mergedRemoteInfo.isDeleted());
            } else {
              // local has a put and must be either at or beyond the state of the remote (based on ops above)
              MessageInfo mergedLocalInfo = getMergedMessageInfo(localId, localInfos);
              if (mergedRemoteInfo.isDeleted()) {
                // delete on remote, should be deleted locally too
                assertTrue(localId + ":" + remoteId + " is deleted on remote but not locally",
                    mergedLocalInfo.isDeleted());
              } else if (mergedRemoteInfo.isTtlUpdated() && !idsDeletedLocallyByPartition.get(remoteInfoEntry.getKey())
                  .equals(localId)) {
                // ttl updated on remote, should be ttl updated locally too
                assertTrue(localId + ":" + remoteId + " is updated on remote but not locally",
                    mergedLocalInfo.isTtlUpdated());
              } else if (!idsDeletedLocallyByPartition.get(remoteInfoEntry.getKey()).equals(localId)) {
                // should not be updated or deleted locally
                assertFalse(localId + ":" + remoteId + " has been updated", mergedLocalInfo.isTtlUpdated());
                assertFalse(localId + ":" + remoteId + " has been deleted", mergedLocalInfo.isDeleted());
              }
            }
          }
        }
      }
    }
  }

  /**
   * Test the case where a blob gets deleted after a replication metadata exchange completes and identifies the blob as
   * a candidate. The subsequent GetRequest should succeed as Replication makes a Include_All call, and
   * fixMissingStoreKeys() should succeed without exceptions. The blob should not be put locally.
   */
  @Test
  public void testDeletionAfterMetadataExchange() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, Set<StoreKey>> idsToExpectByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);

      // add 5 messages to remote host only.
      Set<StoreKey> expectedIds =
          new HashSet<>(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 5));

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();

      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
              containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      Pair<ByteBuffer, MessageInfo> putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      remoteHost.addMessage(partitionId,
          new MessageInfo(id, putMsgInfo.getFirst().remaining(), 1, accountId, containerId,
              putMsgInfo.getSecond().getOperationTimeMs()), putMsgInfo.getFirst());

      // add 3 messages to the remote host only
      expectedIds.addAll(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3));

      // delete the very first blob in the remote host only (and delete it from expected list)
      Iterator<StoreKey> iter = expectedIds.iterator();
      addDeleteMessagesToReplicasOfPartition(partitionId, iter.next(), Collections.singletonList(remoteHost));
      iter.remove();

      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost));

      idsToExpectByPartition.put(partitionId, expectedIds);
    }

    // do the exchange metadata.

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 400;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // Do the replica metadata exchange.
    List<ReplicaThread.ExchangeMetadataResponse> responses =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));

    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        responses.stream().map(k -> k.missingStoreKeys).flatMap(Collection::stream).collect(Collectors.toSet()));

    // Now delete a message in the remote before doing the Get requests (for every partition). Remove these keys from
    // expected key set. Even though they are requested, they should not go into the local store. However, this cycle
    // of replication must be successful.
    for (PartitionId partitionId : partitionIds) {
      Iterator<StoreKey> iter = idsToExpectByPartition.get(partitionId).iterator();
      iter.next();
      StoreKey keyToDelete = iter.next();
      addDeleteMessagesToReplicasOfPartition(partitionId, keyToDelete, Collections.singletonList(remoteHost));
      iter.remove();
    }

    replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize),
        replicasToReplicate.get(remoteHost.dataNodeId), responses);

    Assert.assertEquals(idsToExpectByPartition.keySet(), localHost.infosByPartition.keySet());
    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        localHost.infosByPartition.values()
            .stream()
            .flatMap(Collection::stream)
            .map(MessageInfo::getStoreKey)
            .collect(Collectors.toSet()));
  }

  /**
   * Test the case where a blob expires after a replication metadata exchange completes and identifies the blob as
   * a candidate. The subsequent GetRequest should succeed as Replication makes a Include_All call, and
   * fixMissingStoreKeys() should succeed without exceptions. The blob should not be put locally.
   */
  @Test
  public void testExpiryAfterMetadataExchange() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, Set<StoreKey>> idsToExpectByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);

      // add 5 messages to remote host only.
      Set<StoreKey> expectedIds =
          new HashSet<>(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 5));

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();

      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
              containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      Pair<ByteBuffer, MessageInfo> putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      remoteHost.addMessage(partitionId,
          new MessageInfo(id, putMsgInfo.getFirst().remaining(), 1, accountId, containerId,
              putMsgInfo.getSecond().getOperationTimeMs()), putMsgInfo.getFirst());

      // add 3 messages to the remote host only
      expectedIds.addAll(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3));

      // delete the very first blob in the remote host only (and delete it from expected list)
      Iterator<StoreKey> iter = expectedIds.iterator();
      addDeleteMessagesToReplicasOfPartition(partitionId, iter.next(), Collections.singletonList(remoteHost));
      iter.remove();

      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost));

      idsToExpectByPartition.put(partitionId, expectedIds);
    }

    // do the exchange metadata.

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 400;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // Do the replica metadata exchange.
    List<ReplicaThread.ExchangeMetadataResponse> responses =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));

    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        responses.stream().map(k -> k.missingStoreKeys).flatMap(Collection::stream).collect(Collectors.toSet()));

    // Now expire a message in the remote before doing the Get requests (for every partition). Remove these keys from
    // expected key set. Even though they are requested, they should not go into the local store. However, this cycle
    // of replication must be successful.
    PartitionId partitionId = idsToExpectByPartition.keySet().iterator().next();
    Iterator<StoreKey> keySet = idsToExpectByPartition.get(partitionId).iterator();
    StoreKey keyToExpire = keySet.next();
    keySet.remove();
    MessageInfo msgInfoToExpire = null;

    for (MessageInfo info : remoteHost.infosByPartition.get(partitionId)) {
      if (info.getStoreKey().equals(keyToExpire)) {
        msgInfoToExpire = info;
        break;
      }
    }

    int i = remoteHost.infosByPartition.get(partitionId).indexOf(msgInfoToExpire);
    remoteHost.infosByPartition.get(partitionId)
        .set(i, new MessageInfo(msgInfoToExpire.getStoreKey(), msgInfoToExpire.getSize(), msgInfoToExpire.isDeleted(),
            msgInfoToExpire.isTtlUpdated(), 1, msgInfoToExpire.getAccountId(), msgInfoToExpire.getContainerId(),
            msgInfoToExpire.getOperationTimeMs()));

    replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize),
        replicasToReplicate.get(remoteHost.dataNodeId), responses);

    Assert.assertEquals(idsToExpectByPartition.keySet(), localHost.infosByPartition.keySet());
    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        localHost.infosByPartition.values()
            .stream()
            .flatMap(Collection::stream)
            .map(MessageInfo::getStoreKey)
            .collect(Collectors.toSet()));
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
    Pair<Host, Host> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    Host localHost = localAndRemoteHosts.getFirst();
    Host remoteHost = localAndRemoteHosts.getSecond();
    long expectedThrottleDurationMs =
        localHost.dataNodeId.getDatacenterName().equals(remoteHost.dataNodeId.getDatacenterName())
            ? config.replicationIntraReplicaThreadThrottleSleepDurationMs
            : config.replicationInterReplicaThreadThrottleSleepDurationMs;
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
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
              containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
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
          containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
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
          getMessageInfo(toDeleteId, localHost.infosByPartition.get(partitionId), true, false));
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

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
      expectedIndex = assertMissingKeysAndFixMissingStoreKeys(expectedIndex, batchSize - 1, batchSize, missingKeysCount,
          replicaThread, remoteHost, replicasToReplicate);

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

    // 1 expired + 1 corrupt + 1 put (never present) + 1 deleted (never present) expected missing buffers
    verifyNoMoreMissingKeysAndExpectedMissingBufferCount(remoteHost, localHost, replicaThread, replicasToReplicate,
        idsToBeIgnoredByPartition, storeKeyConverter, expectedIndex, expectedIndex + 1, 4);

    // tests to verify replica thread throttling and idling functions in the following steps:
    // 1. all replicas are in sync, thread level sleep and replica quarantine are both enabled.
    // 2. add put messages to some replica and verify that replication for replicas remain disabled.
    // 3. forward the time so replication for replicas are re-enabled and check replication resumes.
    // 4. add more put messages to ensure replication happens continuously when needed and is throttled appropriately.

    // 1. verify that the replica thread sleeps and replicas are temporarily disable when all replicas are synced.
    List<List<RemoteReplicaInfo>> replicasToReplicateList = new ArrayList<>(replicasToReplicate.values());
    // replicate is called and time is moved forward to prepare the replicas for testing.
    replicaThread.replicate(replicasToReplicateList);
    time.sleep(config.replicationSyncedReplicaBackoffDurationMs + 1);
    long currentTimeMs = time.milliseconds();
    replicaThread.replicate(replicasToReplicateList);
    for (List<RemoteReplicaInfo> replicaInfos : replicasToReplicateList) {
      for (RemoteReplicaInfo replicaInfo : replicaInfos) {
        assertEquals("Unexpected re-enable replication time",
            currentTimeMs + config.replicationSyncedReplicaBackoffDurationMs, replicaInfo.getReEnableReplicationTime());
      }
    }
    currentTimeMs = time.milliseconds();
    replicaThread.replicate(replicasToReplicateList);
    assertEquals("Replicas are in sync, replica thread should sleep by replication.thread.idle.sleep.duration.ms",
        currentTimeMs + config.replicationReplicaThreadIdleSleepDurationMs, time.milliseconds());

    // 2. add 3 messages to a partition in the remote host only and verify replication for all replicas should be disabled.
    PartitionId partitionId = clusterMap.getWritablePartitionIds(null).get(0);
    addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3);
    int[] missingKeys = new int[replicasToReplicate.get(remoteHost.dataNodeId).size()];
    for (int i = 0; i < missingKeys.length; i++) {
      missingKeys[i] = replicasToReplicate.get(remoteHost.dataNodeId)
          .get(i)
          .getReplicaId()
          .getPartitionId()
          .isEqual(partitionId.toString()) ? 3 : 0;
    }
    currentTimeMs = time.milliseconds();
    replicaThread.replicate(replicasToReplicateList);
    assertEquals("Replication for all replicas should be disabled and the thread should sleep",
        currentTimeMs + config.replicationReplicaThreadIdleSleepDurationMs, time.milliseconds());
    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);

    // 3. forward the time and run replicate and verify the replication.
    time.sleep(config.replicationSyncedReplicaBackoffDurationMs);
    replicaThread.replicate(replicasToReplicateList);
    missingKeys = new int[replicasToReplicate.get(remoteHost.dataNodeId).size()];
    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);

    // 4. add more put messages and verify that replication continues and is throttled appropriately.
    addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(localHost, remoteHost), 3);
    currentTimeMs = time.milliseconds();
    replicaThread.replicate(new ArrayList<>(replicasToReplicate.values()));
    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);
    assertEquals("Replica thread should sleep exactly " + expectedThrottleDurationMs + " since remote has new token",
        currentTimeMs + expectedThrottleDurationMs, time.milliseconds());

    // verify that throttling on the replica thread is disabled when relevant configs are 0.
    Properties properties = new Properties();
    properties.setProperty("replication.intra.replica.thread.throttle.sleep.duration.ms", "0");
    properties.setProperty("replication.inter.replica.thread.throttle.sleep.duration.ms", "0");
    config = new ReplicationConfig(new VerifiableProperties(properties));
    replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, remoteHost, storeKeyConverter, transformer,
            null);
    replicasToReplicateList = new ArrayList<>(replicasAndThread.getFirst().values());
    replicaThread = replicasAndThread.getSecond();
    currentTimeMs = time.milliseconds();
    replicaThread.replicate(replicasToReplicateList);
    assertEquals("Replica thread should not sleep when throttling is disabled and replicas are out of sync",
        currentTimeMs, time.milliseconds());
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
   * Creates and gets the remote replicas that the local host will deal with and the {@link ReplicaThread} to perform
   * replication with.
   * @param batchSize the number of messages to be returned in each iteration of replication
   * @param clusterMap the {@link ClusterMap} to use
   * @param localHost the local {@link Host} (the one running the replica thread)
   * @param remoteHost the remote {@link Host} (the target of replication)
   * @param storeKeyConverter the {@link StoreKeyConverter} to be used in {@link ReplicaThread}
   * @param transformer the {@link Transformer} to be used in {@link ReplicaThread}
   *  param listener the {@link StoreEventListener} to use.
   * @return a pair whose first element is the set of remote replicas and the second element is the {@link ReplicaThread}
   */
  private Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> getRemoteReplicasAndReplicaThread(int batchSize,
      ClusterMap clusterMap, Host localHost, Host remoteHost, StoreKeyConverter storeKeyConverter,
      Transformer transformer, StoreEventListener listener) {
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populatePerColoMetrics(Collections.singleton(remoteHost.dataNodeId.getDatacenterName()));
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate =
        Collections.singletonMap(remoteHost.dataNodeId, localHost.getRemoteReplicaInfos(remoteHost, listener));
    Map<DataNodeId, Host> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, config, replicationMetrics, null,
            storeKeyConverter, transformer, clusterMap.getMetricRegistry(), false,
            localHost.dataNodeId.getDatacenterName(), new ResponseHandler(clusterMap), time);
    return new Pair<>(replicasToReplicate, replicaThread);
  }

  /**
   * Asserts the number of missing keys between the local and remote replicas and fixes the keys
   * @param expectedIndex initial expected index
   * @param expectedIndexInc increment level for the expected index (how much the findToken index is expected to increment)
   * @param batchSize how large of a batch size the internal MockConnection will use for fixing missing store keys
   * @param expectedMissingKeysSum the number of missing keys expected
   * @param replicaThread replicaThread that will be performing replication
   * @param remoteHost the remote host that keys are being pulled from
   * @param replicasToReplicate list of replicas to replicate between
   * @return expectedIndex + expectedIndexInc
   * @throws Exception
   */
  private int assertMissingKeysAndFixMissingStoreKeys(int expectedIndex, int expectedIndexInc, int batchSize,
      int expectedMissingKeysSum, ReplicaThread replicaThread, Host remoteHost,
      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    return assertMissingKeysAndFixMissingStoreKeys(expectedIndex, expectedIndex, expectedIndexInc, batchSize,
        expectedMissingKeysSum, replicaThread, remoteHost, replicasToReplicate);
  }

  /**
   * Asserts the number of missing keys between the local and remote replicas and fixes the keys
   * @param expectedIndex initial expected index for even numbered partitions
   * @param expectedIndexOdd initial expected index for odd numbered partitions
   * @param expectedIndexInc increment level for the expected index (how much the findToken index is expected to increment)
   * @param batchSize how large of a batch size the internal MockConnection will use for fixing missing store keys
   * @param expectedMissingKeysSum the number of missing keys expected
   * @param replicaThread replicaThread that will be performing replication
   * @param remoteHost the remote host that keys are being pulled from
   * @param replicasToReplicate list of replicas to replicate between
   * @return expectedIndex + expectedIndexInc
   * @throws Exception
   */
  private int assertMissingKeysAndFixMissingStoreKeys(int expectedIndex, int expectedIndexOdd, int expectedIndexInc,
      int batchSize, int expectedMissingKeysSum, ReplicaThread replicaThread, Host remoteHost,
      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    expectedIndex += expectedIndexInc;
    expectedIndexOdd += expectedIndexInc;
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(expectedMissingKeysSum, response.get(i).missingStoreKeys.size());
      assertEquals(i % 2 == 0 ? expectedIndex : expectedIndexOdd,
          ((MockFindToken) response.get(i).remoteToken).getIndex());
      replicasToReplicate.get(remoteHost.dataNodeId).get(i).setToken(response.get(i).remoteToken);
    }
    replicaThread.fixMissingStoreKeys(new MockConnection(remoteHost, batchSize),
        replicasToReplicate.get(remoteHost.dataNodeId), response);
    for (int i = 0; i < response.size(); i++) {
      assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
          replicasToReplicate.get(remoteHost.dataNodeId).get(i).getToken());
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
  private void assertMissingKeys(int[] expectedMissingKeysSum, int batchSize, ReplicaThread replicaThread,
      Host remoteHost, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate) throws Exception {
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, batchSize),
            replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(expectedMissingKeysSum[i], response.get(i).missingStoreKeys.size());
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
  private void verifyNoMoreMissingKeysAndExpectedMissingBufferCount(Host remoteHost, Host localHost,
      ReplicaThread replicaThread, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate,
      Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition, StoreKeyConverter storeKeyConverter,
      int expectedIndex, int expectedIndexOdd, int expectedMissingBuffers) throws Exception {
    // no more missing keys
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.exchangeMetadata(new MockConnection(remoteHost, 4),
            replicasToReplicate.get(remoteHost.dataNodeId));
    assertEquals("Response should contain a response for each replica",
        replicasToReplicate.get(remoteHost.dataNodeId).size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(0, response.get(i).missingStoreKeys.size());
      assertEquals(i % 2 == 0 ? expectedIndex : expectedIndexOdd,
          ((MockFindToken) response.get(i).remoteToken).getIndex());
    }

    Map<PartitionId, List<MessageInfo>> missingInfos =
        remoteHost.getMissingInfos(localHost.infosByPartition, storeKeyConverter);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      // test that the first key has been marked deleted
      List<MessageInfo> messageInfos = localHost.infosByPartition.get(entry.getKey());
      StoreKey deletedId = messageInfos.get(0).getStoreKey();
      assertNotNull(deletedId + " should have been deleted", getMessageInfo(deletedId, messageInfos, true, false));
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
   * Selects a local and remote host for replication tests that need it.
   * @param clusterMap the {@link MockClusterMap} to use.
   * @return a {@link Pair} with the first entry being the "local" host and the second, the "remote" host.
   */
  private Pair<Host, Host> getLocalAndRemoteHosts(MockClusterMap clusterMap) {
    // to make sure we select hosts with the SPECIAL_PARTITION_CLASS, pick hosts from the replicas of that partition
    PartitionId specialPartitionId = clusterMap.getWritablePartitionIds(MockClusterMap.SPECIAL_PARTITION_CLASS).get(0);
    // these hosts have replicas of the "special" partition and all the other partitions.
    Host localHost = new Host(specialPartitionId.getReplicaIds().get(0).getDataNodeId(), clusterMap);
    Host remoteHost = new Host(specialPartitionId.getReplicaIds().get(1).getDataNodeId(), clusterMap);
    return new Pair<>(localHost, remoteHost);
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
          containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      ids.add(id);
      Pair<ByteBuffer, MessageInfo> putMsgInfo = getPutMessage(id, accountId, containerId, toEncrypt);
      for (Host host : hosts) {
        host.addMessage(partitionId, putMsgInfo.getSecond(), putMsgInfo.getFirst().duplicate());
      }
    }
    return ids;
  }

  private BlobId generateRandomBlobId(PartitionId partitionId) {
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    boolean toEncrypt = Utils.getRandomShort(TestUtils.RANDOM) % 2 == 0;
    return new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
        containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
  }

  private void addPutMessagesToReplicasOfPartition(List<StoreKey> ids, List<Host> hosts)
      throws MessageFormatException, IOException {
    List<Transformer> transformerList = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      transformerList.add(null);
    }
    addPutMessagesToReplicasOfPartition(ids, transformerList, hosts);
  }

  private void addPutMessagesToReplicasOfPartition(List<StoreKey> ids, List<Transformer> transformPerId,
      List<Host> hosts) throws MessageFormatException, IOException {
    Iterator<Transformer> transformerIterator = transformPerId.iterator();
    for (StoreKey storeKey : ids) {
      Transformer transformer = transformerIterator.next();
      BlobId id = (BlobId) storeKey;
      Pair<ByteBuffer, MessageInfo> putMsgInfo =
          getPutMessage(id, id.getAccountId(), id.getContainerId(), BlobId.isEncrypted(id.toString()));
      if (transformer != null) {
        Message message = new Message(putMsgInfo.getSecond(), new ByteBufferInputStream(putMsgInfo.getFirst()));
        TransformationOutput output = transformer.transform(message);
        assertNull(output.getException());
        message = output.getMsg();
        putMsgInfo = new Pair<>(
            ByteBuffer.wrap(Utils.readBytesFromStream(message.getStream(), (int) message.getMessageInfo().getSize())),
            message.getMessageInfo());
      }
      for (Host host : hosts) {
        host.addMessage(id.getPartition(), putMsgInfo.getSecond(), putMsgInfo.getFirst().duplicate());
      }
    }
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
    MessageInfo putMsg = getMessageInfo(id, hosts.get(0).infosByPartition.get(partitionId), false, false);
    short aid;
    short cid;
    if (putMsg == null) {
      // the StoreKey must be a BlobId in this case (to get the account and container id)
      aid = ((BlobId) id).getAccountId();
      cid = ((BlobId) id).getContainerId();
    } else {
      aid = putMsg.getAccountId();
      cid = putMsg.getContainerId();
    }
    ByteBuffer buffer = getDeleteMessage(id, aid, cid, CONSTANT_TIME_MS);
    for (Host host : hosts) {
      // ok to send false for ttlUpdated
      host.addMessage(partitionId, new MessageInfo(id, buffer.remaining(), true, false, aid, cid, CONSTANT_TIME_MS),
          buffer.duplicate());
    }
  }

  /**
   * For the given partitionId, constructs ttl update messages and adds them to the given lists.
   * @param partitionId the {@link PartitionId} to use for generating the {@link StoreKey} of the message.
   * @param id the {@link StoreKey} to create a ttl update message for.
   * @param hosts the list of {@link Host} all of which will be populated with the messages.
   * @throws MessageFormatException
   * @throws IOException
   */
  private void addTtlUpdateMessagesToReplicasOfPartition(PartitionId partitionId, StoreKey id, List<Host> hosts)
      throws MessageFormatException, IOException {
    MessageInfo putMsg = getMessageInfo(id, hosts.get(0).infosByPartition.get(partitionId), false, false);
    short aid;
    short cid;
    if (putMsg == null) {
      // the StoreKey must be a BlobId in this case (to get the account and container id)
      aid = ((BlobId) id).getAccountId();
      cid = ((BlobId) id).getContainerId();
    } else {
      aid = putMsg.getAccountId();
      cid = putMsg.getContainerId();
    }
    ByteBuffer buffer = getTtlUpdateMessage(id, aid, cid, UPDATED_EXPIRY_TIME_MS, CONSTANT_TIME_MS);
    for (Host host : hosts) {
      host.addMessage(partitionId,
          new MessageInfo(id, buffer.remaining(), false, true, UPDATED_EXPIRY_TIME_MS, aid, cid, CONSTANT_TIME_MS),
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
            accountId, containerId, encryptionKey != null);
    MessageFormatInputStream stream =
        new PutMessageFormatInputStream(id, encryptionKey == null ? null : ByteBuffer.wrap(encryptionKey),
            blobProperties, ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), blobSize);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return new Pair<>(ByteBuffer.wrap(message),
        new MessageInfo(id, message.length, EXPIRY_TIME_MS, accountId, containerId, CONSTANT_TIME_MS));
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
  private ByteBuffer getTtlUpdateMessage(StoreKey id, short accountId, short containerId, long expiresAtMs,
      long updateTimeMs) throws MessageFormatException, IOException {
    MessageFormatInputStream stream =
        new TtlUpdateMessageFormatInputStream(id, accountId, containerId, expiresAtMs, updateTimeMs);
    byte[] message = Utils.readBytesFromStream(stream, (int) stream.getSize());
    return ByteBuffer.wrap(message);
  }

  /**
   * Gets the {@link MessageInfo} for {@code id} if present.
   * @param id the {@link StoreKey} to look for.
   * @param messageInfos the {@link MessageInfo} list.
   * @param deleteMsg {@code true} if delete msg is requested. {@code false} otherwise
   * @param ttlUpdateMsg {@code true} if ttl update msg is requested. {@code false} otherwise
   * @return the delete {@link MessageInfo} if it exists in {@code messageInfos}. {@code null otherwise.}
   */
  private static MessageInfo getMessageInfo(StoreKey id, List<MessageInfo> messageInfos, boolean deleteMsg,
      boolean ttlUpdateMsg) {
    MessageInfo toRet = null;
    for (MessageInfo messageInfo : messageInfos) {
      if (messageInfo.getStoreKey().equals(id)) {
        if (deleteMsg && messageInfo.isDeleted()) {
          toRet = messageInfo;
          break;
        } else if (ttlUpdateMsg && messageInfo.isTtlUpdated()) {
          toRet = messageInfo;
          break;
        } else if (!deleteMsg && !ttlUpdateMsg) {
          toRet = messageInfo;
          break;
        }
      }
    }
    return toRet;
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
      existingBuffers.add(buffer.duplicate());
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
    Map<PartitionId, List<MessageInfo>> getMissingInfos(Map<PartitionId, List<MessageInfo>> other) throws Exception {
      return getMissingInfos(other, null);
    }

    /**
     * Gets the message infos that are present in this host but missing in {@code other}.
     * @param other the list of {@link MessageInfo} to check against.
     * @return the message infos that are present in this host but missing in {@code other}.
     */
    Map<PartitionId, List<MessageInfo>> getMissingInfos(Map<PartitionId, List<MessageInfo>> other,
        StoreKeyConverter storeKeyConverter) throws Exception {
      Map<PartitionId, List<MessageInfo>> missingInfos = new HashMap<>();
      for (Map.Entry<PartitionId, List<MessageInfo>> entry : infosByPartition.entrySet()) {
        PartitionId partitionId = entry.getKey();
        for (MessageInfo messageInfo : entry.getValue()) {
          boolean found = false;
          StoreKey convertedKey;
          if (storeKeyConverter == null) {
            convertedKey = messageInfo.getStoreKey();
          } else {
            Map<StoreKey, StoreKey> map =
                storeKeyConverter.convert(Collections.singletonList(messageInfo.getStoreKey()));
            convertedKey = map.get(messageInfo.getStoreKey());
            if (convertedKey == null) {
              continue;
            }
          }
          for (MessageInfo otherInfo : other.get(partitionId)) {
            if (convertedKey.equals(otherInfo.getStoreKey()) && messageInfo.isDeleted() == otherInfo.isDeleted()) {
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
   * Returns a merged {@link MessageInfo} for {@code key}
   * @param key the {@link StoreKey} to look for
   * @param partitionInfos the {@link MessageInfo}s for the partition
   * @return a merged {@link MessageInfo} for {@code key}
   */
  private static MessageInfo getMergedMessageInfo(StoreKey key, List<MessageInfo> partitionInfos) {
    MessageInfo info = getMessageInfo(key, partitionInfos, true, false);
    if (info == null) {
      info = getMessageInfo(key, partitionInfos, false, false);
    }
    MessageInfo ttlUpdateInfo = getMessageInfo(key, partitionInfos, false, true);
    if (ttlUpdateInfo != null) {
      info = new MessageInfo(info.getStoreKey(), info.getSize(), info.isDeleted(), true,
          ttlUpdateInfo.getExpirationTimeInMs(), info.getCrc(), info.getAccountId(), info.getContainerId(),
          info.getOperationTimeMs());
    }
    return info;
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

      @Override
      public void doPrefetch(int index, long relativeOffset, long size) {
        return;
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
      List<MessageInfo> infos = new ArrayList<>();
      for (MessageInfo info : newInfos) {
        if (info.isTtlUpdated()) {
          info =
              new MessageInfo(info.getStoreKey(), info.getSize(), info.isDeleted(), false, info.getExpirationTimeInMs(),
                  info.getCrc(), info.getAccountId(), info.getContainerId(), info.getOperationTimeMs());
        }
        infos.add(info);
      }
      messageInfos.addAll(infos);
      if (listener != null) {
        listener.onPut(this, infos);
      }
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
      for (MessageInfo info : messageSetToDelete.getMessageSetInfo()) {
        try {
          messageSetToDelete.writeTo(log);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        MessageInfo ttlUpdateInfo = getMessageInfo(info.getStoreKey(), messageInfos, false, true);
        messageInfos.add(new MessageInfo(info.getStoreKey(), info.getSize(), true, ttlUpdateInfo != null,
            info.getExpirationTimeInMs(), info.getAccountId(), info.getContainerId(), info.getOperationTimeMs()));
      }
    }

    @Override
    public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {
      for (MessageInfo info : messageSetToUpdate.getMessageSetInfo()) {
        if (getMessageInfo(info.getStoreKey(), messageInfos, true, false) != null) {
          throw new StoreException("Deleted", StoreErrorCodes.ID_Deleted);
        } else if (getMessageInfo(info.getStoreKey(), messageInfos, false, true) != null) {
          throw new StoreException("Updated already", StoreErrorCodes.Already_Updated);
        } else if (getMessageInfo(info.getStoreKey(), messageInfos, false, false) == null) {
          throw new StoreException("Not Found", StoreErrorCodes.ID_Not_Found);
        }
        try {
          messageSetToUpdate.writeTo(log);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        messageInfos.add(new MessageInfo(info.getStoreKey(), info.getSize(), false, true, info.getExpirationTimeInMs(),
            info.getAccountId(), info.getContainerId(), info.getOperationTimeMs()));
      }
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries) throws StoreException {
      // unused function
      MockFindToken mockToken = (MockFindToken) token;
      List<MessageInfo> entriesToReturn = new ArrayList<>();
      long currentSizeOfEntriesInBytes = 0;
      int index = mockToken.getIndex();
      Set<StoreKey> processedKeys = new HashSet<>();
      while (currentSizeOfEntriesInBytes < maxSizeOfEntries && index < messageInfos.size()) {
        StoreKey key = messageInfos.get(index).getStoreKey();
        if (processedKeys.add(key)) {
          entriesToReturn.add(getMergedMessageInfo(key, messageInfos));
        }
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
      return getMessageInfo(key, messageInfos, true, false) != null;
    }

    @Override
    public long getSizeInBytes() {
      return log.getEndOffSet();
    }

    @Override
    public boolean isEmpty() {
      return log.blobs.isEmpty();
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
    public void send(Send request) {
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
            MessageInfo infoFound = null;
            for (MessageInfo info : messageInfoList) {
              if (key.equals(info.getStoreKey())) {
                infoFound = getMergedMessageInfo(info.getStoreKey(), messageInfoList);
                messageMetadatasToReturn.get(partitionId).add(null);
                buffersToReturn.add(bufferList.get(index));
                break;
              }
              index++;
            }
            if (infoFound != null) {
              // If MsgInfo says it is deleted, get the original Put Message's MessageInfo as that is what Get Request
              // looks for. Just set the deleted flag to true for the constructed MessageInfo from Put.
              if (infoFound.isDeleted()) {
                MessageInfo putMsgInfo = getMessageInfo(infoFound.getStoreKey(), messageInfoList, false, false);
                infoFound = new MessageInfo(putMsgInfo.getStoreKey(), putMsgInfo.getSize(), true, false,
                    putMsgInfo.getExpirationTimeInMs(), putMsgInfo.getAccountId(), putMsgInfo.getContainerId(),
                    putMsgInfo.getOperationTimeMs());
              }
              infosToReturn.get(partitionId).add(infoFound);
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
          Set<StoreKey> processedKeys = new HashSet<>();
          for (int i = startIndex; i < endIndex; i++) {
            StoreKey key = partitionInfos.get(i).getStoreKey();
            if (processedKeys.add(key)) {
              messageInfosToReturn.add(getMergedMessageInfo(key, partitionInfos));
            }
            indexRequested = i;
          }
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
          List<MessageInfo> infosForPartition = infosToReturn.get(requestInfo.getPartition());
          PartitionResponseInfo partitionResponseInfo;
          if (!getRequest.getGetOption().equals(GetOption.Include_All) && !getRequest.getGetOption()
              .equals(GetOption.Include_Deleted_Blobs) && infosForPartition.stream().anyMatch(MessageInfo::isDeleted)) {
            partitionResponseInfo = new PartitionResponseInfo(requestInfo.getPartition(), ServerErrorCode.Blob_Deleted);
          } else if (!getRequest.getGetOption().equals(GetOption.Include_All) && !getRequest.getGetOption()
              .equals(GetOption.Include_Expired_Blobs) && infosForPartition.stream().anyMatch(MessageInfo::isExpired)) {
            partitionResponseInfo = new PartitionResponseInfo(requestInfo.getPartition(), ServerErrorCode.Blob_Expired);
          } else {
            partitionResponseInfo =
                new PartitionResponseInfo(requestInfo.getPartition(), infosToReturn.get(requestInfo.getPartition()),
                    messageMetadatasToReturn.get(requestInfo.getPartition()));
          }
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
