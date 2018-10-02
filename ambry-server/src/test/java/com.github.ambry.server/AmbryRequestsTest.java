/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStreamTest;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.network.SocketRequestResponseChannel;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.BlobStoreControlAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminResponse;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.Response;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.replication.MockFindTokenFactory;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageInfoTest;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.MockWrite;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link AmbryRequests}.
 */
public class AmbryRequestsTest {
  private static final FindTokenFactory FIND_TOKEN_FACTORY = new MockFindTokenFactory();

  private final MockClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final MockStorageManager storageManager;
  private final MockReplicationManager replicationManager;
  private final AmbryRequests ambryRequests;
  private final MockRequestResponseChannel requestResponseChannel = new MockRequestResponseChannel();
  private final Set<StoreKey> validKeysInStore = new HashSet<>();
  private final Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
  private final MockStoreKeyConverterFactory storeKeyConverterFactory;

  public AmbryRequestsTest() throws IOException, ReplicationException, StoreException, InterruptedException {
    clusterMap = new MockClusterMap();
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    properties.setProperty("replication.no.of.intra.dc.replica.threads", "0");
    properties.setProperty("replication.no.of.inter.dc.replica.threads", "0");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    dataNodeId = clusterMap.getDataNodeIds().get(0);
    storageManager = new MockStorageManager(validKeysInStore, clusterMap.getReplicaIds(dataNodeId));
    storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(conversionMap);
    replicationManager =
        MockReplicationManager.getReplicationManager(verifiableProperties, storageManager, clusterMap, dataNodeId,
            storeKeyConverterFactory);
    ambryRequests = new AmbryRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), FIND_TOKEN_FACTORY, null, replicationManager, null, false,
        storeKeyConverterFactory);
    storageManager.start();
  }

  /**
   * Close the storageManager created.
   */
  @After
  public void after() throws InterruptedException {
    storageManager.shutdown();
  }

  /**
   * Tests that compactions are scheduled correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds =
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    for (PartitionId id : partitionIds) {
      doScheduleCompactionTest(id, ServerErrorCode.No_Error);
      assertEquals("Partition scheduled for compaction not as expected", id,
          storageManager.compactionScheduledPartitionId);
    }
  }

  /**
   * Tests failure scenarios for compaction - disk down, store not scheduled for compaction, exception while scheduling.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionFailureTest() throws InterruptedException, IOException {
    // partitionId not specified
    doScheduleCompactionTest(null, ServerErrorCode.Bad_Request);

    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    // store is not started - Replica_Unavailable
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.Replica_Unavailable);
    storageManager.returnNullStore = false;

    // all stores are shutdown - Disk_Unavailable. This is simulated by shutting down the storage manager.
    storageManager.shutdown();
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.Disk_Unavailable);
    storageManager.returnNullStore = false;
    storageManager.start();
    // make sure the disk is up when storageManager is restarted.
    doScheduleCompactionTest(id, ServerErrorCode.No_Error);

    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk unavailable
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    doScheduleCompactionTest(id, ServerErrorCode.Disk_Unavailable);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);

    // store cannot be scheduled for compaction - Unknown_Error
    storageManager.returnValueOfSchedulingCompaction = false;
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfSchedulingCompaction = true;

    // exception while attempting to schedule - InternalServerError
    storageManager.exceptionToThrowOnSchedulingCompaction = new IllegalStateException();
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnSchedulingCompaction = null;
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#RequestControl} works correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void controlRequestSuccessTest() throws InterruptedException, IOException {
    RequestOrResponseType[] requestOrResponseTypes =
        {RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest, RequestOrResponseType.GetRequest, RequestOrResponseType.ReplicaMetadataRequest, RequestOrResponseType.TtlUpdateRequest};
    for (RequestOrResponseType requestType : requestOrResponseTypes) {
      List<? extends PartitionId> partitionIds =
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
      for (PartitionId id : partitionIds) {
        doRequestControlRequestTest(requestType, id);
      }
      doRequestControlRequestTest(requestType, null);
    }
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#RequestControl} fails when bad input is provided (or when there is
   * bad internal state).
   */
  @Test
  public void controlRequestFailureTest() throws InterruptedException, IOException {
    // cannot disable admin request
    sendAndVerifyRequestControlRequest(RequestOrResponseType.AdminRequest, false, null, ServerErrorCode.Bad_Request);
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#ReplicationControl} works correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void controlReplicationSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds =
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    for (PartitionId id : partitionIds) {
      doControlReplicationTest(id, ServerErrorCode.No_Error);
    }
    doControlReplicationTest(null, ServerErrorCode.No_Error);
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#ReplicationControl} fails when bad input is provided (or when there is
   * bad internal state).
   */
  @Test
  public void controlReplicationFailureTest() throws InterruptedException, IOException {
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyReplicationControlRequest(Collections.EMPTY_LIST, false,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), ServerErrorCode.Bad_Request);
    replicationManager.reset();
    replicationManager.exceptionToThrow = new IllegalStateException();
    sendAndVerifyReplicationControlRequest(Collections.EMPTY_LIST, false,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0),
        ServerErrorCode.Unknown_Error);
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.
  }

  /**
   * Tests for the response received on a {@link CatchupStatusAdminRequest} for different cases
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void catchupStatusSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    assertTrue("This test needs more than one partition to work", partitionIds.size() > 1);
    PartitionId id = partitionIds.get(0);
    ReplicaId thisPartRemoteRep = getRemoteReplicaId(id);
    ReplicaId otherPartRemoteRep = getRemoteReplicaId(partitionIds.get(1));
    List<? extends ReplicaId> replicaIds = id.getReplicaIds();
    assertTrue("This test needs more than one replica for the first partition to work", replicaIds.size() > 1);

    long acceptableLagInBytes = 100;

    // cases with a given partition id
    // all replicas of given partition < acceptableLag
    generateLagOverrides(0, acceptableLagInBytes - 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, true);
    // all replicas of given partition = acceptableLag
    generateLagOverrides(acceptableLagInBytes, acceptableLagInBytes);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, true);
    // 1 replica of some other partition > acceptableLag
    String key = MockReplicationManager.getPartitionLagKey(otherPartRemoteRep.getPartitionId(),
        otherPartRemoteRep.getDataNodeId().getHostname(), otherPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, true);
    // 1 replica of this partition > acceptableLag
    key = MockReplicationManager.getPartitionLagKey(id, thisPartRemoteRep.getDataNodeId().getHostname(),
        thisPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, false);
    // same result if num expected replicas == total count -1.
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 1), ServerErrorCode.No_Error, false);
    // caught up if num expected replicas == total count - 2
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 2), ServerErrorCode.No_Error, true);
    // caught up if num expected replicas == total count - 3
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 3), ServerErrorCode.No_Error, true);
    // all replicas of this partition > acceptableLag
    generateLagOverrides(acceptableLagInBytes + 1, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, false);

    // cases with no partition id provided
    // all replicas of all partitions < acceptableLag
    generateLagOverrides(0, acceptableLagInBytes - 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, true);
    // all replicas of all partitions = acceptableLag
    generateLagOverrides(acceptableLagInBytes, acceptableLagInBytes);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, true);
    // 1 replica of one partition > acceptableLag
    key = MockReplicationManager.getPartitionLagKey(id, thisPartRemoteRep.getDataNodeId().getHostname(),
        thisPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, false);
    // same result if num expected replicas == total count -1.
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 1), ServerErrorCode.No_Error, false);
    // caught up if num expected replicas == total count - 2
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 2), ServerErrorCode.No_Error, true);
    // caught up if num expected replicas == total count - 3
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 3), ServerErrorCode.No_Error, true);
    // all replicas of all partitions > acceptableLag
    generateLagOverrides(acceptableLagInBytes + 1, acceptableLagInBytes + 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.No_Error, false);
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#CatchupStatus} fails when bad input is provided (or when there is
   * bad internal state).
   */
  @Test
  public void catchupStatusFailureTest() throws InterruptedException, IOException {
    // acceptableLagInBytes < 0
    doCatchupStatusTest(null, -1, Short.MAX_VALUE, ServerErrorCode.Bad_Request, false);
    // numReplicasCaughtUpPerPartition = 0
    doCatchupStatusTest(null, 0, (short) 0, ServerErrorCode.Bad_Request, false);
    // numReplicasCaughtUpPerPartition < 0
    doCatchupStatusTest(null, 0, (short) -1, ServerErrorCode.Bad_Request, false);
    // replication manager error
    replicationManager.reset();
    replicationManager.exceptionToThrow = new IllegalStateException();
    doCatchupStatusTest(null, 0, Short.MAX_VALUE, ServerErrorCode.Unknown_Error, false);
  }

  /**
   * Tests for the response received on a {@link BlobStoreControlAdminRequest} for successful case
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void controlBlobStoreSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    PartitionId id = partitionIds.get(0);
    List<? extends ReplicaId> replicaIds = id.getReplicaIds();
    assertTrue("This test needs more than one replica for the first partition to work", replicaIds.size() > 1);
    long acceptableLagInBytes = 0;
    short numReplicasCaughtUpPerPartition = 3;
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    generateLagOverrides(0, acceptableLagInBytes);
    // stop BlobStore
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.No_Error);
    // verify APIs are called in the process of stopping BlobStore
    assertEquals("Compaction on store should be disabled after stopping the BlobStore", false,
        storageManager.compactionEnableVal);
    assertEquals("Partition disabled for compaction not as expected", id,
        storageManager.compactionControlledPartitionId);
    assertEquals("Origins list should be empty", true, replicationManager.originsVal.isEmpty());
    assertEquals("Replication on given BlobStore should be disabled", false, replicationManager.enableVal);
    assertEquals("Partition shutdown not as expected", id, storageManager.shutdownPartitionId);
    // start BlobStore
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.No_Error);
    // verify APIs are called in the process of starting BlobStore
    assertEquals("Partition started not as expected", id, storageManager.startedPartitionId);
    assertEquals("Replication on given BlobStore should be enabled", true, replicationManager.enableVal);
    assertEquals("Partition controlled for compaction not as expected", id,
        storageManager.compactionControlledPartitionId);
    assertEquals("Compaction on store should be enabled after starting the BlobStore", true,
        storageManager.compactionEnableVal);
  }

  /**
   * Tests for the startBlobStore response received on a {@link BlobStoreControlAdminRequest} for different failure cases
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void startBlobStoreFailureTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    PartitionId id = partitionIds.get(0);
    short numReplicasCaughtUpPerPartition = 3;
    // test start BlobStore failure
    storageManager.returnValueOfStartingBlobStore = false;
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfStartingBlobStore = true;
    // test start BlobStore with runtime exception
    storageManager.exceptionToThrowOnStartingBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnStartingBlobStore = null;
    // test enable replication failure
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    replicationManager.controlReplicationReturnVal = true;
    // test enable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfControllingCompaction = true;
    // test enable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, true, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnControllingCompaction = null;
  }

  /**
   * Tests for the stopBlobStore response received on a {@link BlobStoreControlAdminRequest} for different failure cases
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void stopBlobStoreFailureTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    PartitionId id = partitionIds.get(0);
    short numReplicasCaughtUpPerPartition = 3;
    // test partition unknown
    sendAndVerifyStoreControlRequest(null, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Bad_Request);
    // test validate request failure - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Replica_Unavailable);
    storageManager.returnNullStore = false;
    // test validate request failure - Disk_Unavailable
    storageManager.shutdown();
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Disk_Unavailable);
    storageManager.returnNullStore = false;
    storageManager.start();
    // test invalid numReplicasCaughtUpPerPartition
    numReplicasCaughtUpPerPartition = -1;
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Bad_Request);
    numReplicasCaughtUpPerPartition = 3;
    // test disable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfControllingCompaction = true;
    // test disable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnControllingCompaction = null;
    // test disable replication failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    // test peers catchup failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    // all replicas of this partition > acceptableLag
    generateLagOverrides(1, 1);
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Retry_After_Backoff);
    // test shutdown BlobStore failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    storageManager.returnValueOfShutdownBlobStore = false;
    generateLagOverrides(0, 0);
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    // test shutdown BlobStore with runtime exception
    storageManager.exceptionToThrowOnShuttingDownBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, false, numReplicasCaughtUpPerPartition, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnShuttingDownBlobStore = null;
  }

  /**
   * Tests blobIds can be converted as expected and works correctly with GetRequest.
   * If all blobIds can be converted correctly, no error is expected.
   * If any blobId can't be converted correctly, Blob_Not_Found is expected.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void listOfOriginalStoreKeysGetTest() throws InterruptedException, IOException {
    int numIds = 10;
    PartitionId partitionId = clusterMap.getAllPartitionIds(null).get(0);
    List<BlobId> blobIds = new ArrayList<>();
    for (int i = 0; i < numIds; i++) {
      BlobId originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
      BlobId convertedBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.CRAFTED,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, originalBlobId.getAccountId(), originalBlobId.getContainerId(),
          partitionId, false, BlobId.BlobDataType.DATACHUNK);
      conversionMap.put(originalBlobId, convertedBlobId);
      validKeysInStore.add(convertedBlobId);
      blobIds.add(originalBlobId);
    }
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.No_Error);

    // test with duplicates
    List<BlobId> blobIdsWithDups = new ArrayList<>(blobIds);
    // add the same blob ids
    blobIdsWithDups.addAll(blobIds);
    // add converted ids
    conversionMap.values().forEach(id -> blobIdsWithDups.add((BlobId) id));
    sendAndVerifyGetOriginalStoreKeys(blobIdsWithDups, ServerErrorCode.No_Error);
    // store must not have received duplicates
    assertEquals("Size is not as expected", blobIds.size(), MockStorageManager.idsReceived.size());
    for (int i = 0; i < blobIds.size(); i++) {
      BlobId key = blobIds.get(i);
      StoreKey converted = conversionMap.get(key);
      assertEquals(key + "/" + converted + " was not received at the store", converted,
          MockStorageManager.idsReceived.get(i));
    }

    // Check a valid key mapped to null
    BlobId originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
    blobIds.add(originalBlobId);
    conversionMap.put(originalBlobId, null);
    validKeysInStore.add(originalBlobId);
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.No_Error);

    // Check a invalid key mapped to null
    originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
    blobIds.add(originalBlobId);
    conversionMap.put(originalBlobId, null);
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.Blob_Not_Found);

    // Check exception
    storeKeyConverterFactory.setException(new Exception("StoreKeyConverter Mock Exception"));
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.Unknown_Error);
  }

  /**
   * Tests for success and failure scenarios for TTL updates
   * @throws InterruptedException
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void ttlUpdateTest() throws InterruptedException, IOException, MessageFormatException {
    MockPartitionId id =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    long opTimeMs = SystemTime.getInstance().milliseconds();

    // storekey not valid for store
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.Blob_Not_Found);
    // valid now
    validKeysInStore.add(blobId);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.No_Error);
    // after conversion
    validKeysInStore.remove(blobId);
    BlobId converted = new BlobId(blobId.getVersion(), BlobId.BlobIdType.CRAFTED, blobId.getDatacenterId(),
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), blobId.getPartition(),
        BlobId.isEncrypted(blobId.getID()), BlobId.BlobDataType.DATACHUNK);
    // not in conversion map
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.Blob_Not_Found);
    // in conversion map but key not valid
    conversionMap.put(blobId, converted);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.Blob_Not_Found);
    // valid now
    validKeysInStore.add(converted);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.No_Error);

    // READ_ONLY is fine too
    changePartitionState(id, true);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.No_Error);
    changePartitionState(id, false);

    miscTtlUpdateFailuresTest();
  }

  // helpers

  // general

  /**
   * Calls {@link AmbryRequests#handleRequests(Request)} with {@code request} and returns the {@link Response} received.
   * @param request the {@link Request} to process
   * @param expectedServerErrorCode the expected {@link ServerErrorCode} in the server response.
   * @return the {@link Response} received.
   * @throws InterruptedException
   * @throws IOException
   */
  private Response sendRequestGetResponse(RequestOrResponse request, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    Request mockRequest = MockRequest.fromRequest(request);
    ambryRequests.handleRequests(mockRequest);
    assertEquals("Request accompanying response does not match original request", mockRequest,
        requestResponseChannel.lastOriginalRequest);
    assertNotNull("Response not sent", requestResponseChannel.lastResponse);
    Response response = (Response) requestResponseChannel.lastResponse;
    assertNotNull("Response is not of type Response", response);
    assertEquals("Correlation id in response does match the one in the request", request.getCorrelationId(),
        response.getCorrelationId());
    assertEquals("Client id in response does match the one in the request", request.getClientId(),
        response.getClientId());
    assertEquals("Error code does not match expected", expectedServerErrorCode, response.getError());
    return response;
  }

  /**
   * Sends and verifies that an operation specific request works correctly.
   * @param request the {@link Request} to send to {@link AmbryRequests}
   * @param expectedErrorCode the {@link ServerErrorCode} expected in the response. For some requests this is the
   *                          response in the constituents rather than the actual response ({@link GetResponse} and
   *                          {@link ReplicaMetadataResponse}).
   * @param forceCheckOpReceived if {@code true}, checks the operation received at the {@link Store} even if
   *                             there is an error expected. Always checks op received if {@code expectedErrorCode} is
   *                             {@link ServerErrorCode#No_Error}. Skips the check otherwise.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyOperationRequest(RequestOrResponse request, ServerErrorCode expectedErrorCode,
      Boolean forceCheckOpReceived) throws InterruptedException, IOException {
    storageManager.resetStore();
    RequestOrResponseType requestType = request.getRequestType();
    Response response = sendRequestGetResponse(request,
        EnumSet.of(RequestOrResponseType.GetRequest, RequestOrResponseType.ReplicaMetadataRequest).contains(requestType)
            ? ServerErrorCode.No_Error : expectedErrorCode);
    if (expectedErrorCode.equals(ServerErrorCode.No_Error) || forceCheckOpReceived) {
      assertEquals("Operation received at the store not as expected", requestType,
          MockStorageManager.operationReceived);
    }
    if (requestType == RequestOrResponseType.GetRequest) {
      GetResponse getResponse = (GetResponse) response;
      for (PartitionResponseInfo info : getResponse.getPartitionResponseInfoList()) {
        assertEquals("Error code does not match expected", expectedErrorCode, info.getErrorCode());
      }
    } else if (requestType == RequestOrResponseType.ReplicaMetadataRequest) {
      ReplicaMetadataResponse replicaMetadataResponse = (ReplicaMetadataResponse) response;
      for (ReplicaMetadataResponseInfo info : replicaMetadataResponse.getReplicaMetadataResponseInfoList()) {
        assertEquals("Error code does not match expected", expectedErrorCode, info.getError());
      }
    }
  }

  /**
   * Sends and verifies that a {@link AdminRequestOrResponseType#BlobStoreControl} request received the error code
   * expected.
   * @param partitionId the {@link PartitionId} to send the request for. Can be {@code null}.
   * @param enable {@code true} if BlobStore needs to be started. {@code false} otherwise.
   * @param numReplicasCaughtUpPerPartition the number of peer replicas which have caught up with this store before proceeding.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyStoreControlRequest(PartitionId partitionId, boolean enable,
      short numReplicasCaughtUpPerPartition, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, correlationId, clientId);
    BlobStoreControlAdminRequest blobStoreControlAdminRequest =
        new BlobStoreControlAdminRequest(numReplicasCaughtUpPerPartition, enable, adminRequest);
    Response response = sendRequestGetResponse(blobStoreControlAdminRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
  }

  /**
   * @param id the {@link PartitionId} to find the {@link ReplicaId} for on the current node
   * @return the {@link ReplicaId} corresponding to {@code id} on this data node
   */
  private ReplicaId findReplica(PartitionId id) {
    ReplicaId replicaId = null;
    for (ReplicaId replica : id.getReplicaIds()) {
      if (replica.getDataNodeId().equals(dataNodeId)) {
        replicaId = replica;
        break;
      }
    }
    return replicaId;
  }

  /**
   * Changes the write state of the partition
   * @param id the {@link MockPartitionId} whose status needs to change
   * @param seal if {@code true}, partition will become
   * {@link com.github.ambry.clustermap.PartitionState#READ_ONLY}. Otherwise
   * {@link com.github.ambry.clustermap.PartitionState#READ_WRITE}
   */
  private void changePartitionState(PartitionId id, boolean seal) {
    MockReplicaId replicaId = (MockReplicaId) findReplica(id);
    replicaId.setSealedState(seal);
    ((MockPartitionId) id).resolvePartitionStatus();
  }

  /**
   * Gets the data of size in the {@code messageWriteSet} into a {@link ByteBuffer}
   * @param messageWriteSet the {@link MessageWriteSet} to read from
   * @param size the size of data to read. If this size is greater than the amount of data in {@code messageWriteSet},
   *             then this function will loop infinitely
   * @return a {@link ByteBuffer} containing data of size {@code size} from {@code messageWriteSet}
   * @throws IOException
   */
  private ByteBuffer getDataInWriteSet(MessageWriteSet messageWriteSet, int size) throws IOException {
    MockWrite write = new MockWrite(size);
    long sizeWritten = 0;
    while (sizeWritten < size) {
      sizeWritten += messageWriteSet.writeTo(write);
    }
    return write.getBuffer();
  }

  // scheduleCompactionSuccessTest() and scheduleCompactionFailuresTest() helpers

  /**
   * Schedules a compaction for {@code id} and checks that the {@link ServerErrorCode} returned matches
   * {@code expectedServerErrorCode}.
   * @param id the {@link PartitionId} to schedule compaction for.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected when the request is processed.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doScheduleCompactionTest(PartitionId id, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.TriggerCompaction, id, correlationId, clientId);
    Response response = sendRequestGetResponse(adminRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
  }

  // controlRequestSuccessTest() and controlRequestFailureTest() helpers

  /**
   * Does the test for {@link AdminRequestOrResponseType#RequestControl} by checking that
   * 1. The request that is being disabled works
   * 2. The disabling of the request works
   * 3. The request disabling has taken effect
   * 4. The enabling of the request works
   * 5. The re-enabled request works correctly
   * @param toControl the {@link RequestOrResponseType} to control.
   * @param id the {@link PartitionId} to disable {@code toControl} on. Can be {@code null}.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doRequestControlRequestTest(RequestOrResponseType toControl, PartitionId id)
      throws InterruptedException, IOException {
    List<? extends PartitionId> idsToTest;
    if (id == null) {
      idsToTest = clusterMap.getAllPartitionIds(null);
    } else {
      idsToTest = Collections.singletonList(id);
    }
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error, null);
    // disable the request
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.No_Error);
    // check that it is disabled
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.Temporarily_Disabled, false);
    // ok to call disable again
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.No_Error);
    // enable
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.No_Error);
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error, null);
    // ok to call enable again
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.No_Error);
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error, null);
  }

  /**
   * Sends and verifies that GetRequest with a list of original blobIds works correctly.
   * @param blobIds List of blobIds for GetRequest.
   * @param expectedErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyGetOriginalStoreKeys(List<BlobId> blobIds, ServerErrorCode expectedErrorCode)
      throws InterruptedException, IOException {
    PartitionId partitionId = blobIds.get(0).getPartition();
    int correlationId = blobIds.get(0).getContainerId();
    String clientId = UtilsTest.getRandomString(10);

    PartitionRequestInfo pRequestInfo = new PartitionRequestInfo(partitionId, blobIds);
    RequestOrResponse request =
        new GetRequest(correlationId, clientId, MessageFormatFlags.All, Collections.singletonList(pRequestInfo),
            GetOption.Include_All);
    storageManager.resetStore();
    if (!expectedErrorCode.equals(ServerErrorCode.Unknown_Error)) {
      // known error will be filled to each PartitionResponseInfo and set ServerErrorCode.No_Error in response.
      Response response = sendRequestGetResponse(request, ServerErrorCode.No_Error);
      assertEquals("Operation received at the store not as expected", RequestOrResponseType.GetRequest,
          MockStorageManager.operationReceived);
      for (PartitionResponseInfo info : ((GetResponse) response).getPartitionResponseInfoList()) {
        assertEquals("Error code does not match expected", expectedErrorCode, info.getErrorCode());
      }
    } else {
      sendRequestGetResponse(request, ServerErrorCode.Unknown_Error);
    }
  }

  /**
   * Sends and verifies that an operation specific request works correctly.
   * @param requestType the type of the request to send.
   * @param ids the partitionIds to send requests for.
   * @param expectedErrorCode the {@link ServerErrorCode} expected in the response. For some requests this is the
   *                          response in the constituents rather than the actual response ({@link GetResponse} and
   *                          {@link ReplicaMetadataResponse}).
   * @param forceCheckOpReceived if {@code true}, checks the operation received at the {@link Store} even if
   *                             there is an error expected. Always checks op received if {@code expectedErrorCode} is
   *                             {@link ServerErrorCode#No_Error}. Skips the check otherwise.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyOperationRequest(RequestOrResponseType requestType, List<? extends PartitionId> ids,
      ServerErrorCode expectedErrorCode, Boolean forceCheckOpReceived) throws InterruptedException, IOException {
    for (PartitionId id : ids) {
      int correlationId = TestUtils.RANDOM.nextInt();
      String clientId = UtilsTest.getRandomString(10);
      BlobId originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
      BlobId convertedBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.CRAFTED,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, originalBlobId.getAccountId(), originalBlobId.getContainerId(), id,
          false, BlobId.BlobDataType.DATACHUNK);
      conversionMap.put(originalBlobId, convertedBlobId);
      validKeysInStore.add(convertedBlobId);
      RequestOrResponse request;
      switch (requestType) {
        case PutRequest:
          BlobProperties properties =
              new BlobProperties(0, "serviceId", originalBlobId.getAccountId(), originalBlobId.getAccountId(), false);
          request = new PutRequest(correlationId, clientId, originalBlobId, properties, ByteBuffer.allocate(0),
              ByteBuffer.allocate(0), 0, BlobType.DataBlob, null);
          break;
        case DeleteRequest:
          request = new DeleteRequest(correlationId, clientId, originalBlobId, SystemTime.getInstance().milliseconds());
          break;
        case GetRequest:
          PartitionRequestInfo pRequestInfo = new PartitionRequestInfo(id, Collections.singletonList(originalBlobId));
          request =
              new GetRequest(correlationId, clientId, MessageFormatFlags.All, Collections.singletonList(pRequestInfo),
                  GetOption.Include_All);
          break;
        case ReplicaMetadataRequest:
          ReplicaMetadataRequestInfo rRequestInfo =
              new ReplicaMetadataRequestInfo(id, FIND_TOKEN_FACTORY.getNewFindToken(), "localhost", "/tmp");
          request = new ReplicaMetadataRequest(correlationId, clientId, Collections.singletonList(rRequestInfo),
              Long.MAX_VALUE);
          break;
        case TtlUpdateRequest:
          request = new TtlUpdateRequest(correlationId, clientId, originalBlobId, Utils.Infinite_Time,
              SystemTime.getInstance().milliseconds());
          break;
        default:
          throw new IllegalArgumentException(requestType + " not supported by this function");
      }
      sendAndVerifyOperationRequest(request, expectedErrorCode, forceCheckOpReceived);
    }
  }

  /**
   * Sends and verifies that a {@link AdminRequestOrResponseType#RequestControl} request received the error code
   * expected.
   * @param toControl the {@link AdminRequestOrResponseType#RequestControl} to control.
   * @param enable {@code true} if {@code toControl} needs to be enabled. {@code false} otherwise.
   * @param id the {@link PartitionId} to send the request for. Can be {@code null}.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyRequestControlRequest(RequestOrResponseType toControl, boolean enable, PartitionId id,
      ServerErrorCode expectedServerErrorCode) throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.RequestControl, id, correlationId, clientId);
    RequestControlAdminRequest controlRequest = new RequestControlAdminRequest(toControl, enable, adminRequest);
    Response response = sendRequestGetResponse(controlRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
  }

  // controlReplicationSuccessTest() and controlReplicationFailureTest() helpers

  /**
   * Does the test for {@link AdminRequestOrResponseType#ReplicationControl} by checking that the request is correctly
   * deserialized in {@link AmbryRequests} and passed to the {@link ReplicationManager}.
   * @param id the {@link PartitionId} to disable replication on. Can be {@code null} in which case replication will
   *           be enabled/disabled on all partitions.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doControlReplicationTest(PartitionId id, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int numOrigins = TestUtils.RANDOM.nextInt(8) + 2;
    List<String> origins = new ArrayList<>();
    for (int i = 0; i < numOrigins; i++) {
      origins.add(UtilsTest.getRandomString(TestUtils.RANDOM.nextInt(8) + 2));
    }
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    sendAndVerifyReplicationControlRequest(origins, false, id, expectedServerErrorCode);
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    sendAndVerifyReplicationControlRequest(origins, true, id, expectedServerErrorCode);
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    sendAndVerifyReplicationControlRequest(Collections.EMPTY_LIST, true, id, expectedServerErrorCode);
  }

  /**
   * Sends and verifies that a {@link AdminRequestOrResponseType#ReplicationControl} request received the error code
   * expected and that {@link AmbryRequests} sent the right details to {@link ReplicationManager}.
   * @param origins the list of datacenters from which replication should be enabled/disabled.
   * @param enable {@code true} if replication needs to be enabled. {@code false} otherwise.
   * @param id the {@link PartitionId} to send the request for. Can be {@code null}.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyReplicationControlRequest(List<String> origins, boolean enable, PartitionId id,
      ServerErrorCode expectedServerErrorCode) throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.ReplicationControl, id, correlationId, clientId);
    ReplicationControlAdminRequest controlRequest = new ReplicationControlAdminRequest(origins, enable, adminRequest);
    Response response = sendRequestGetResponse(controlRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
    List<PartitionId> idsVal;
    if (id == null) {
      idsVal = clusterMap.getAllPartitionIds(null);
    } else {
      idsVal = Collections.singletonList(id);
    }
    if (!expectedServerErrorCode.equals(ServerErrorCode.Unknown_Error)) {
      assertEquals("Origins not as provided in request", origins, replicationManager.originsVal);
      assertEquals("Enable not as provided in request", enable, replicationManager.enableVal);
      assertEquals("Ids not as provided in request", idsVal.size(), replicationManager.idsVal.size());
      assertTrue("Ids not as provided in request", replicationManager.idsVal.containsAll(idsVal));
    }
  }

  // catchupStatusSuccessTest() helpers

  /**
   * Gets a "remote" replica for the given {@code partitionId}
   * @param partitionId the {@link PartitionId} for which a remote replica is required.
   * @return a "remote" replica for the given {@code partitionId}
   */
  private ReplicaId getRemoteReplicaId(PartitionId partitionId) {
    ReplicaId replicaId = null;
    for (ReplicaId id : partitionId.getReplicaIds()) {
      if (!id.getDataNodeId().equals(dataNodeId)) {
        replicaId = id;
        break;
      }
    }
    if (replicaId == null) {
      throw new IllegalStateException("There is no remote replica for " + partitionId);
    }
    return replicaId;
  }

  /**
   * Generates lag overrides in {@code replicationManager} with each lag a number between {@code base} and
   * {@code upperBound} both inclusive.
   * @param base the minimum value of lag (inclusive)
   * @param upperBound the maximum value of lag (inclusive)
   */
  private void generateLagOverrides(long base, long upperBound) {
    replicationManager.lagOverrides = new HashMap<>();
    for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        String key = MockReplicationManager.getPartitionLagKey(partitionId, replicaId.getDataNodeId().getHostname(),
            replicaId.getReplicaPath());
        Long value = base + Utils.getRandomLong(TestUtils.RANDOM, upperBound - base + 1);
        replicationManager.lagOverrides.put(key, value);
      }
    }
  }

  // catchupStatusSuccessTest() and catchupStatusFailureTest() helpers

  /**
   * Does the test for {@link AdminRequestOrResponseType#CatchupStatus} by checking that the request is correctly
   * deserialized in {@link AmbryRequests} and the necessary info obtained from {@link ReplicationManager}.
   * @param id the {@link PartitionId} to disable replication on. Can be {@code null}.
   * @param acceptableLagInBytes the value of acceptable lag to set in the {@link CatchupStatusAdminRequest}.
   * @param numReplicasCaughtUpPerPartition the value of num replicas caught up per partition to set in the
   *                          {@link CatchupStatusAdminRequest}.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @param expectedIsCaughtUp the expected return from {@link CatchupStatusAdminResponse#isCaughtUp()}.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doCatchupStatusTest(PartitionId id, long acceptableLagInBytes, short numReplicasCaughtUpPerPartition,
      ServerErrorCode expectedServerErrorCode, boolean expectedIsCaughtUp) throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.CatchupStatus, id, correlationId, clientId);
    CatchupStatusAdminRequest catchupStatusRequest =
        new CatchupStatusAdminRequest(acceptableLagInBytes, numReplicasCaughtUpPerPartition, adminRequest);
    Response response = sendRequestGetResponse(catchupStatusRequest, expectedServerErrorCode);
    assertTrue("Response not of type CatchupStatusAdminResponse", response instanceof CatchupStatusAdminResponse);
    CatchupStatusAdminResponse adminResponse = (CatchupStatusAdminResponse) response;
    assertEquals("Catchup status not as expected", expectedIsCaughtUp, adminResponse.isCaughtUp());
  }

  // ttlUpdateTest() helpers

  /**
   * Does a TTL update and checks for success if {@code expectedErrorCode} is {@link ServerErrorCode#No_Error}. Else,
   * checks for failure with the code {@code expectedErrorCode}.
   * @param correlationId the correlation ID to use in the request
   * @param clientId the client ID to use in the request
   * @param blobId the blob ID to use in the request
   * @param expiresAtMs the expiry time (ms) to use in the request
   * @param opTimeMs the operation time (ms) to use in the request
   * @param expectedErrorCode the expected {@link ServerErrorCode}
   * @throws InterruptedException
   * @throws IOException
   * @throws MessageFormatException
   */
  private void doTtlUpdate(int correlationId, String clientId, BlobId blobId, long expiresAtMs, long opTimeMs,
      ServerErrorCode expectedErrorCode) throws InterruptedException, IOException, MessageFormatException {
    TtlUpdateRequest request = new TtlUpdateRequest(correlationId, clientId, blobId, expiresAtMs, opTimeMs);
    sendAndVerifyOperationRequest(request, expectedErrorCode, true);
    if (expectedErrorCode == ServerErrorCode.No_Error) {
      verifyTtlUpdate(request.getBlobId(), expiresAtMs, opTimeMs, MockStorageManager.messageWriteSetReceived);
    }
  }

  /**
   * Exercises various failure paths for TTL updates
   * @throws InterruptedException
   * @throws IOException
   */
  private void miscTtlUpdateFailuresTest() throws InterruptedException, IOException {
    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    // store exceptions
    for (StoreErrorCodes code : StoreErrorCodes.values()) {
      MockStorageManager.storeException = new StoreException("expected", code);
      ServerErrorCode expectedErrorCode = ErrorMapping.getStoreErrorMapping(code);
      sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
          expectedErrorCode, true);
      MockStorageManager.storeException = null;
    }
    // runtime exception
    MockStorageManager.runtimeException = new RuntimeException("expected");
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.Unknown_Error, true);
    MockStorageManager.runtimeException = null;
    // store is not started/is stopped/otherwise unavailable - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.Replica_Unavailable, false);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk down
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.Disk_Unavailable, false);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);
    // request disabled is checked in request control tests
  }

  /**
   * Verifies that the TTL update request was delivered to the {@link Store} correctly.
   * @param blobId the {@link BlobId} that was updated
   * @param expiresAtMs the new expire time (in ms)
   * @param opTimeMs the op time (in ms)
   * @param messageWriteSet the {@link MessageWriteSet} received at the {@link Store}
   * @throws IOException
   * @throws MessageFormatException
   */
  private void verifyTtlUpdate(BlobId blobId, long expiresAtMs, long opTimeMs, MessageWriteSet messageWriteSet)
      throws IOException, MessageFormatException {
    BlobId key = (BlobId) conversionMap.getOrDefault(blobId, blobId);
    assertEquals("There should be one message in the write set", 1, messageWriteSet.getMessageSetInfo().size());
    MessageInfo info = messageWriteSet.getMessageSetInfo().get(0);

    // verify stream
    ByteBuffer record = getDataInWriteSet(messageWriteSet, (int) info.getSize());
    int expectedSize = record.remaining();
    InputStream stream = new ByteBufferInputStream(record);

    // verify stream
    MessageFormatInputStreamTest.checkTtlUpdateMessage(stream, null, key, key.getAccountId(), key.getContainerId(),
        expiresAtMs, opTimeMs);

    // verify MessageInfo
    // since the record has been verified, the buffer size before verification is a good indicator of size
    MessageInfoTest.checkGetters(info, key, expectedSize, false, true, expiresAtMs, null, key.getAccountId(),
        key.getContainerId(), opTimeMs);
  }

  /**
   * Implementation of {@link Request} to help with tests.
   */
  private static class MockRequest implements Request {

    private final InputStream stream;

    /**
     * Constructs a {@link MockRequest} from {@code request}.
     * @param request the {@link RequestOrResponse} to construct the {@link MockRequest} for.
     * @return an instance of {@link MockRequest} that represents {@code request}.
     * @throws IOException
     */
    static MockRequest fromRequest(RequestOrResponse request) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate((int) request.sizeInBytes());
      request.writeTo(new ByteBufferChannel(buffer));
      buffer.flip();
      // read length (to bring it to a state where AmbryRequests can handle it).
      buffer.getLong();
      return new MockRequest(new ByteBufferInputStream(buffer));
    }

    /**
     * Constructs a {@link MockRequest}.
     * @param stream the {@link InputStream} that will be returned on a call to {@link #getInputStream()}.
     */
    private MockRequest(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getInputStream() {
      return stream;
    }

    @Override
    public long getStartTimeInMs() {
      return 0;
    }
  }

  /**
   * An extension of {@link SocketRequestResponseChannel} to help with tests.
   */
  private static class MockRequestResponseChannel extends SocketRequestResponseChannel {
    /**
     * {@link Request} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Request lastOriginalRequest = null;

    /**
     * The {@link Send} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Send lastResponse = null;

    MockRequestResponseChannel() {
      super(1, 1);
    }

    @Override
    public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics) {
      lastResponse = payloadToSend;
      lastOriginalRequest = originalRequest;
    }
  }

  /**
   * An extension of {@link StorageManager} to help with tests.
   */
  private static class MockStorageManager extends StorageManager {

    /**
     * The operation received at the store.
     */
    static RequestOrResponseType operationReceived = null;

    /**
     * The {@link MessageWriteSet} received at the store (only for put, delete and ttl update)
     */
    static MessageWriteSet messageWriteSetReceived = null;

    /**
     * The IDs received at the store (only for get)
     */
    static List<? extends StoreKey> idsReceived = null;

    /**
     * The {@link StoreGetOptions} received at the store (only for get)
     */
    static EnumSet<StoreGetOptions> storeGetOptionsReceived;

    /**
     * The {@link FindToken} received at the store (only for findEntriesSince())
     */
    static FindToken tokenReceived = null;

    /**
     * The maxTotalSizeOfEntries received at the store (only for findEntriesSince())
     */
    static Long maxTotalSizeOfEntriesReceived = null;

    /**
     * StoreException to throw when an API is invoked
     */
    static StoreException storeException = null;

    /**
     * RuntimeException to throw when an API is invoked. Will be preferred over {@link #storeException}.
     */
    static RuntimeException runtimeException = null;

    /**
     * An empty {@link Store} implementation.
     */
    private Store store = new Store() {

      @Override
      public void start() throws StoreException {
        throwExceptionIfRequired();
      }

      @Override
      public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions)
          throws StoreException {
        operationReceived = RequestOrResponseType.GetRequest;
        idsReceived = ids;
        storeGetOptionsReceived = storeGetOptions;
        throwExceptionIfRequired();
        checkValidityOfIds(ids);
        return new StoreInfo(new MessageReadSet() {
          @Override
          public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) {
            return 0;
          }

          @Override
          public int count() {
            return 0;
          }

          @Override
          public long sizeInBytes(int index) {
            return 0;
          }

          @Override
          public StoreKey getKeyAt(int index) {
            return null;
          }

          @Override
          public void doPrefetch(int index, long relativeOffset, long size) {
            return;
          }
        }, Collections.EMPTY_LIST);
      }

      @Override
      public void put(MessageWriteSet messageSetToWrite) throws StoreException {
        operationReceived = RequestOrResponseType.PutRequest;
        messageWriteSetReceived = messageSetToWrite;
        throwExceptionIfRequired();
      }

      @Override
      public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
        operationReceived = RequestOrResponseType.DeleteRequest;
        messageWriteSetReceived = messageSetToDelete;
        throwExceptionIfRequired();
        checkValidityOfIds(
            messageSetToDelete.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()));
      }

      @Override
      public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {
        operationReceived = RequestOrResponseType.TtlUpdateRequest;
        messageWriteSetReceived = messageSetToUpdate;
        throwExceptionIfRequired();
        checkValidityOfIds(
            messageSetToUpdate.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()));
      }

      @Override
      public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
        operationReceived = RequestOrResponseType.ReplicaMetadataRequest;
        tokenReceived = token;
        maxTotalSizeOfEntriesReceived = maxTotalSizeOfEntries;
        throwExceptionIfRequired();
        return new FindInfo(Collections.EMPTY_LIST, FIND_TOKEN_FACTORY.getNewFindToken());
      }

      @Override
      public Set<StoreKey> findMissingKeys(List<StoreKey> keys) {
        throw new UnsupportedOperationException();
      }

      @Override
      public StoreStats getStoreStats() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isKeyDeleted(StoreKey key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getSizeInBytes() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isEmpty() {
        return false;
      }

      public void shutdown() throws StoreException {
        throwExceptionIfRequired();
      }

      /**
       * Throws a {@link RuntimeException} or {@link StoreException} if so configured
       * @throws StoreException
       */
      private void throwExceptionIfRequired() throws StoreException {
        if (runtimeException != null) {
          throw runtimeException;
        }
        if (storeException != null) {
          throw storeException;
        }
      }

      /**
       * Checks the validity of the {@code ids}
       * @param ids the {@link StoreKey}s to check
       * @throws StoreException if the key is not valid
       */
      private void checkValidityOfIds(Collection<? extends StoreKey> ids) throws StoreException {
        for (StoreKey id : ids) {
          if (!validKeysInStore.contains(id)) {
            throw new StoreException("Not a valid key.", StoreErrorCodes.ID_Not_Found);
          }
        }
      }
    };

    private static final VerifiableProperties VPROPS = new VerifiableProperties(new Properties());

    /**
     * if {@code true}, a {@code null} {@link Store} is returned on a call to {@link #getStore(PartitionId)}. Otherwise
     * {@link #store} is returned.
     */
    boolean returnNullStore = false;
    /**
     * If non-null, the given exception is thrown when {@link #scheduleNextForCompaction(PartitionId)} is called.
     */
    RuntimeException exceptionToThrowOnSchedulingCompaction = null;
    /**
     * If non-null, the given exception is thrown when {@link #controlCompactionForBlobStore(PartitionId, boolean)} is called.
     */
    RuntimeException exceptionToThrowOnControllingCompaction = null;
    /**
     * If non-null, the given exception is thrown when {@link #shutdownBlobStore(PartitionId)} is called.
     */
    RuntimeException exceptionToThrowOnShuttingDownBlobStore = null;
    /**
     * If non-null, the given exception is thrown when {@link #startBlobStore(PartitionId)} is called.
     */
    RuntimeException exceptionToThrowOnStartingBlobStore = null;
    /**
     * The return value for a call to {@link #scheduleNextForCompaction(PartitionId)}.
     */
    boolean returnValueOfSchedulingCompaction = true;
    /**
     * The return value for a call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}.
     */
    boolean returnValueOfControllingCompaction = true;
    /**
     * The return value for a call to {@link #shutdownBlobStore(PartitionId)}.
     */
    boolean returnValueOfShutdownBlobStore = true;
    /**
     * The return value for a call to {@link #startBlobStore(PartitionId)}.
     */
    boolean returnValueOfStartingBlobStore = true;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #scheduleNextForCompaction(PartitionId)}
     */
    PartitionId compactionScheduledPartitionId = null;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}
     */
    PartitionId compactionControlledPartitionId = null;
    /**
     * The {@link boolean} that was provided in the call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}
     */
    Boolean compactionEnableVal = null;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #shutdownBlobStore(PartitionId)}
     */
    PartitionId shutdownPartitionId = null;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #startBlobStore(PartitionId)}
     */
    PartitionId startedPartitionId = null;

    private final Set<StoreKey> validKeysInStore;

    MockStorageManager(Set<StoreKey> validKeysInStore, List<? extends ReplicaId> replicas) throws StoreException {
      super(new StoreConfig(VPROPS), new DiskManagerConfig(VPROPS), Utils.newScheduler(1, true), new MetricRegistry(),
          replicas, null, null, null, null, new MockTime());
      this.validKeysInStore = validKeysInStore;
    }

    @Override
    public Store getStore(PartitionId id) {
      return returnNullStore ? null : store;
    }

    @Override
    public boolean scheduleNextForCompaction(PartitionId id) {
      if (exceptionToThrowOnSchedulingCompaction != null) {
        throw exceptionToThrowOnSchedulingCompaction;
      }
      compactionScheduledPartitionId = id;
      return returnValueOfSchedulingCompaction;
    }

    @Override
    public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
      if (exceptionToThrowOnControllingCompaction != null) {
        throw exceptionToThrowOnControllingCompaction;
      }
      compactionControlledPartitionId = id;
      compactionEnableVal = enabled;
      return returnValueOfControllingCompaction;
    }

    @Override
    public boolean shutdownBlobStore(PartitionId id) {
      if (exceptionToThrowOnShuttingDownBlobStore != null) {
        throw exceptionToThrowOnShuttingDownBlobStore;
      }
      shutdownPartitionId = id;
      return returnValueOfShutdownBlobStore;
    }

    @Override
    public boolean startBlobStore(PartitionId id) {
      if (exceptionToThrowOnStartingBlobStore != null) {
        throw exceptionToThrowOnStartingBlobStore;
      }
      startedPartitionId = id;
      return returnValueOfStartingBlobStore;
    }

    /**
     * Resets variables associated with the {@link Store} impl
     */
    void resetStore() {
      operationReceived = null;
      messageWriteSetReceived = null;
      idsReceived = null;
      storeGetOptionsReceived = null;
      tokenReceived = null;
      maxTotalSizeOfEntriesReceived = null;
    }
  }

  /**
   * An extension of {@link ReplicationManager} to help with testing.
   */
  private static class MockReplicationManager extends ReplicationManager {
    // General variables
    RuntimeException exceptionToThrow = null;
    // Variables for controlling and examining the values provided to controlReplicationForPartitions()
    Boolean controlReplicationReturnVal;
    Collection<PartitionId> idsVal;
    List<String> originsVal;
    Boolean enableVal;
    // Variables for controlling getRemoteReplicaLagFromLocalInBytes()
    // the key is partitionId:hostname:replicaPath
    Map<String, Long> lagOverrides = null;

    /**
     * Static construction helper
     * @param verifiableProperties the {@link VerifiableProperties} to use for config.
     * @param storageManager the {@link StorageManager} to use.
     * @param clusterMap the {@link ClusterMap} to use.
     * @param dataNodeId the {@link DataNodeId} to use.
     * @param storeKeyConverterFactory the {@link StoreKeyConverterFactory} to use.
     * @return an instance of {@link MockReplicationManager}
     * @throws ReplicationException
     */
    static MockReplicationManager getReplicationManager(VerifiableProperties verifiableProperties,
        StorageManager storageManager, ClusterMap clusterMap, DataNodeId dataNodeId,
        StoreKeyConverterFactory storeKeyConverterFactory) throws ReplicationException {
      ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
      StoreConfig storeConfig = new StoreConfig(verifiableProperties);
      return new MockReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, clusterMap,
          dataNodeId, storeKeyConverterFactory);
    }

    /**
     * Constructor for MockReplicationManager.
     * @param replicationConfig the config for replication.
     * @param clusterMapConfig the config for clustermap.
     * @param storeConfig the config for the store.
     * @param storageManager the {@link StorageManager} to use.
     * @param clusterMap the {@link ClusterMap} to use.
     * @param dataNodeId the {@link DataNodeId} to use.
     * @throws ReplicationException
     */
    MockReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
        StoreConfig storeConfig, StorageManager storageManager, ClusterMap clusterMap, DataNodeId dataNodeId,
        StoreKeyConverterFactory storeKeyConverterFactory) throws ReplicationException {
      super(replicationConfig, clusterMapConfig, storeConfig, storageManager, new StoreKeyFactory() {

        @Override
        public StoreKey getStoreKey(DataInputStream stream) {
          return null;
        }

        @Override
        public StoreKey getStoreKey(String input) {
          return null;
        }
      }, clusterMap, null, dataNodeId, null, clusterMap.getMetricRegistry(), null, storeKeyConverterFactory, null);
      reset();
    }

    @Override
    public boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable) {
      failIfRequired();
      if (controlReplicationReturnVal == null) {
        throw new IllegalStateException("Return val not set. Don't know what to return");
      }
      idsVal = ids;
      originsVal = origins;
      enableVal = enable;
      return controlReplicationReturnVal;
    }

    @Override
    public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
      failIfRequired();
      long lag;
      String key = getPartitionLagKey(partitionId, hostName, replicaPath);
      if (lagOverrides == null || !lagOverrides.containsKey(key)) {
        lag = super.getRemoteReplicaLagFromLocalInBytes(partitionId, hostName, replicaPath);
      } else {
        lag = lagOverrides.get(key);
      }
      return lag;
    }

    /**
     * Resets all state
     */
    void reset() {
      exceptionToThrow = null;
      controlReplicationReturnVal = null;
      idsVal = null;
      originsVal = null;
      enableVal = null;
      lagOverrides = null;
    }

    /**
     * Gets the key for the lag override in {@code lagOverrides} using the given parameters.
     * @param partitionId the {@link PartitionId} whose replica {@code hostname} is.
     * @param hostname the hostname of the replica whose lag override key is required.
     * @param replicaPath the replica path of the replica whose lag override key is required.
     * @return
     */
    static String getPartitionLagKey(PartitionId partitionId, String hostname, String replicaPath) {
      return partitionId.toString() + ":" + hostname + ":" + replicaPath;
    }

    /**
     * Throws a {@link RuntimeException} if the {@link MockReplicationManager} is required to.
     */
    private void failIfRequired() {
      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
    }
  }
}
