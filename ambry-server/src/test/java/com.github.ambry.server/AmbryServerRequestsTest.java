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

import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ErrorMapping;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStreamTest;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.network.SocketRequestResponseChannel;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.BlobStoreControlAction;
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
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.MockFindTokenHelper;
import com.github.ambry.replication.MockReplicationManager;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageInfoTest;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.MockWrite;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferDataInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.MockClusterMap.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link AmbryServerRequests}.
 */
@RunWith(Parameterized.class)
public class AmbryServerRequestsTest {

  private final FindTokenHelper findTokenHelper;
  private final MockClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final MockStorageManager storageManager;
  private final MockReplicationManager replicationManager;
  private final MockStatsManager statsManager;
  private final MockRequestResponseChannel requestResponseChannel = new MockRequestResponseChannel();
  private final Set<StoreKey> validKeysInStore = new HashSet<>();
  private final Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
  private final MockStoreKeyConverterFactory storeKeyConverterFactory;
  private final ReplicationConfig replicationConfig;
  private final ServerConfig serverConfig;
  private final ReplicaStatusDelegate mockDelegate = Mockito.mock(ReplicaStatusDelegate.class);
  private final boolean validateRequestOnStoreState;
  private AmbryServerRequests ambryRequests;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public AmbryServerRequestsTest(boolean validateRequestOnStoreState)
      throws IOException, ReplicationException, StoreException, InterruptedException, ReflectiveOperationException {
    this.validateRequestOnStoreState = validateRequestOnStoreState;
    clusterMap = new MockClusterMap();
    Properties properties = createProperties(validateRequestOnStoreState, true);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    replicationConfig = new ReplicationConfig(verifiableProperties);
    serverConfig = new ServerConfig(verifiableProperties);
    StatsManagerConfig statsManagerConfig = new StatsManagerConfig(verifiableProperties);
    dataNodeId = clusterMap.getDataNodeIds().get(0);
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    findTokenHelper = new MockFindTokenHelper(storeKeyFactory, replicationConfig);
    storageManager = new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper);
    storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(conversionMap);
    replicationManager =
        MockReplicationManager.getReplicationManager(verifiableProperties, storageManager, clusterMap, dataNodeId,
            storeKeyConverterFactory);
    statsManager =
        new MockStatsManager(storageManager, clusterMap.getReplicaIds(dataNodeId), clusterMap.getMetricRegistry(),
            statsManagerConfig, null);
    ServerMetrics serverMetrics =
        new ServerMetrics(clusterMap.getMetricRegistry(), AmbryRequests.class, AmbryServer.class);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        storeKeyConverterFactory, statsManager);
    storageManager.start();
    Mockito.when(mockDelegate.unseal(any())).thenReturn(true);
    Mockito.when(mockDelegate.unmarkStopped(anyList())).thenReturn(true);
  }

  private static Properties createProperties(boolean validateRequestOnStoreState,
      boolean handleUndeleteRequestEnabled) {
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    properties.setProperty("replication.no.of.intra.dc.replica.threads", "1");
    properties.setProperty("replication.no.of.inter.dc.replica.threads", "1");
    properties.setProperty("server.validate.request.based.on.store.state",
        Boolean.toString(validateRequestOnStoreState));
    properties.setProperty("server.handle.undelete.request.enabled", Boolean.toString(handleUndeleteRequestEnabled));
    return properties;
  }

  /**
   * Close the storageManager created.
   */
  @After
  public void after() throws InterruptedException {
    storageManager.shutdown();
  }

  /**
   * Tests that requests are validated based on local store state.
   */
  @Test
  public void validateRequestsTest() {
    // choose several replicas and make them in different states (there are 10 replicas on current node)
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(dataNodeId);
    Map<ReplicaState, ReplicaId> stateToReplica = new HashMap<>();
    int cnt = 0;
    for (ReplicaState state : EnumSet.complementOf(EnumSet.of(ReplicaState.ERROR))) {
      stateToReplica.put(state, localReplicas.get(cnt));
      cnt++;
    }
    // set store state
    for (Map.Entry<ReplicaState, ReplicaId> entry : stateToReplica.entrySet()) {
      storageManager.getStore(entry.getValue().getPartitionId()).setCurrentState(entry.getKey());
    }

    for (RequestOrResponseType request : EnumSet.of(RequestOrResponseType.PutRequest, RequestOrResponseType.GetRequest,
        RequestOrResponseType.DeleteRequest, RequestOrResponseType.TtlUpdateRequest,
        RequestOrResponseType.UndeleteRequest)) {
      for (Map.Entry<ReplicaState, ReplicaId> entry : stateToReplica.entrySet()) {
        if (request == RequestOrResponseType.PutRequest) {
          // for PUT request, it is not allowed on OFFLINE,BOOTSTRAP and INACTIVE when validateRequestOnStoreState = true
          if (AmbryServerRequests.PUT_ALLOWED_STORE_STATES.contains(entry.getKey())) {
            assertEquals("Error code is not expected for PUT request", ServerErrorCode.No_Error,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          } else {
            assertEquals("Error code is not expected for PUT request",
                validateRequestOnStoreState ? ServerErrorCode.Temporarily_Disabled : ServerErrorCode.No_Error,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          }
        } else if (AmbryServerRequests.UPDATE_REQUEST_TYPES.contains(request)) {
          // for UNDELETE/DELETE/TTL Update request, they are not allowed on OFFLINE,BOOTSTRAP and INACTIVE when validateRequestOnStoreState = true
          if (AmbryServerRequests.UPDATE_ALLOWED_STORE_STATES.contains(entry.getKey())) {
            assertEquals("Error code is not expected for DELETE/TTL Update", ServerErrorCode.No_Error,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          } else {
            assertEquals("Error code is not expected for DELETE/TTL Update",
                validateRequestOnStoreState ? ServerErrorCode.Temporarily_Disabled : ServerErrorCode.No_Error,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          }
        } else {
          // for GET request, all states should be allowed
          assertEquals("Error code is not expected for GET request", ServerErrorCode.No_Error,
              ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
        }
      }
    }
    // reset all stores state to STANDBY
    for (Map.Entry<ReplicaState, ReplicaId> entry : stateToReplica.entrySet()) {
      storageManager.getStore(entry.getValue().getPartitionId()).setCurrentState(ReplicaState.STANDBY);
    }
  }

  /**
   * Tests that compactions are scheduled correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
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

    PartitionId id = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
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
        {RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest, RequestOrResponseType.GetRequest,
            RequestOrResponseType.ReplicaMetadataRequest, RequestOrResponseType.TtlUpdateRequest,
            RequestOrResponseType.UndeleteRequest};
    for (RequestOrResponseType requestType : requestOrResponseTypes) {
      List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
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
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
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
    sendAndVerifyReplicationControlRequest(Collections.emptyList(), false,
        clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0), ServerErrorCode.Bad_Request);
    replicationManager.reset();
    replicationManager.exceptionToThrow = new IllegalStateException();
    sendAndVerifyReplicationControlRequest(Collections.emptyList(), false,
        clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0), ServerErrorCode.Unknown_Error);
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
  public void controlBlobStoreSuccessTest() throws Exception {
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
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.No_Error);
    // verify APIs are called in the process of stopping BlobStore
    assertEquals("Compaction on store should be disabled after stopping the BlobStore", false,
        storageManager.compactionEnableVal);
    assertEquals("Partition disabled for compaction not as expected", id,
        storageManager.compactionControlledPartitionId);
    assertTrue("Origins list should be empty", replicationManager.originsVal.isEmpty());
    assertEquals("Replication on given BlobStore should be disabled", false, replicationManager.enableVal);
    assertEquals("Partition shutdown not as expected", id, storageManager.shutdownPartitionId);
    // start BlobStore
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.No_Error);
    // verify APIs are called in the process of starting BlobStore
    assertEquals("Partition started not as expected", id, storageManager.startedPartitionId);
    assertEquals("Replication on given BlobStore should be enabled", true, replicationManager.enableVal);
    assertEquals("Partition controlled for compaction not as expected", id,
        storageManager.compactionControlledPartitionId);
    assertEquals("Compaction on store should be enabled after starting the BlobStore", true,
        storageManager.compactionEnableVal);
    // add BlobStore (create a new partition and add one of its replicas to server)
    PartitionId newPartition = clusterMap.createNewPartition(clusterMap.getDataNodes());
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.AddStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.No_Error);
    // remove BlobStore (remove previously added store)
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    doNothing().when(mockStore).deleteStoreFiles();
    Mockito.when(mockStore.getReplicaStatusDelegate()).thenReturn(mockDelegate);
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.No_Error);
    storageManager.overrideStoreToReturn = null;
  }

  @Test
  public void addBlobStoreFailureTest() throws Exception {
    // create newPartition1 that no replica sits on current node.
    List<MockDataNodeId> dataNodes = clusterMap.getDataNodes()
        .stream()
        .filter(node -> !node.getHostname().equals(dataNodeId.getHostname()) || node.getPort() != dataNodeId.getPort())
        .collect(Collectors.toList());
    PartitionId newPartition1 = clusterMap.createNewPartition(dataNodes);
    // test that getting new replica from cluster map fails
    sendAndVerifyStoreControlRequest(newPartition1, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.Replica_Unavailable);

    // create newPartition2 that has one replica on current node
    PartitionId newPartition2 = clusterMap.createNewPartition(clusterMap.getDataNodes());
    // test that adding store into StorageManager fails
    storageManager.returnValueOfAddBlobStore = false;
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfAddBlobStore = true;

    // test that adding replica into ReplicationManager fails (we first add replica into ReplicationManager to trigger failure)
    ReplicaId replicaToAdd = clusterMap.getBootstrapReplica(newPartition2.toPathString(), dataNodeId);
    replicationManager.addReplica(replicaToAdd);
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.Unknown_Error);
    assertTrue("Remove replica from replication manager should succeed.",
        replicationManager.removeReplica(replicaToAdd));

    // test that adding replica into StatsManager fails
    statsManager.returnValOfAddReplica = false;
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.Unknown_Error);
    statsManager.returnValOfAddReplica = true;
  }

  @Test
  public void removeBlobStoreFailureTest() throws Exception {
    // first, create new partition but don't add to current node
    PartitionId newPartition = clusterMap.createNewPartition(clusterMap.getDataNodes());
    // test store removal failure because store doesn't exist
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.Partition_Unknown);
    // add store on current node for store removal testing
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.No_Error);
    // mock exception in StorageManager
    storageManager.returnValueOfRemoveBlobStore = false;
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfRemoveBlobStore = true;
    // mock exception when deleting files of removed store
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    doThrow(new IOException()).when(mockStore).deleteStoreFiles();
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.Unknown_Error);
    // test store removal success case
    doNothing().when(mockStore).deleteStoreFiles();
    Mockito.when(mockStore.getReplicaStatusDelegate()).thenReturn(mockDelegate);
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.No_Error);
    storageManager.overrideStoreToReturn = null;
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
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfStartingBlobStore = true;
    // test start BlobStore with runtime exception
    storageManager.exceptionToThrowOnStartingBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnStartingBlobStore = null;
    // test enable replication failure
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    replicationManager.controlReplicationReturnVal = true;
    // test enable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfControllingCompaction = true;
    // test enable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
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
    sendAndVerifyStoreControlRequest(null, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Bad_Request);
    // test validate request failure - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Replica_Unavailable);
    storageManager.returnNullStore = false;
    // test validate request failure - Disk_Unavailable
    storageManager.shutdown();
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Disk_Unavailable);
    storageManager.returnNullStore = false;
    storageManager.start();
    // test invalid numReplicasCaughtUpPerPartition
    numReplicasCaughtUpPerPartition = -1;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Bad_Request);
    numReplicasCaughtUpPerPartition = 3;
    // test disable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfControllingCompaction = true;
    // test disable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnControllingCompaction = null;
    // test disable replication failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    // test peers catchup failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    // all replicas of this partition > acceptableLag
    generateLagOverrides(1, 1);
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Retry_After_Backoff);
    // test shutdown BlobStore failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    storageManager.returnValueOfShutdownBlobStore = false;
    generateLagOverrides(0, 0);
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
    // test shutdown BlobStore with runtime exception
    storageManager.exceptionToThrowOnShuttingDownBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.Unknown_Error);
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
   * @throws StoreException
   */
  @Test
  public void ttlUpdateTest() throws InterruptedException, IOException, MessageFormatException, StoreException {
    MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
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

  /**
   * Tests for server config to enable and disable UNDELETE
   */
  @Test
  public void undeleteEnableDisableTest() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Properties properties = createProperties(validateRequestOnStoreState, false);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ServerConfig serverConfig = new ServerConfig(verifiableProperties);
    ServerMetrics serverMetrics =
        new ServerMetrics(clusterMap.getMetricRegistry(), AmbryRequests.class, AmbryServer.class);
    AmbryServerRequests other = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        storeKeyConverterFactory, statsManager);

    AmbryServerRequests temp = ambryRequests;
    ambryRequests = other;
    try {
      MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
      int correlationId = TestUtils.RANDOM.nextInt();
      String clientId = TestUtils.getRandomString(10);
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
      long opTimeMs = SystemTime.getInstance().milliseconds();
      doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.Temporarily_Disabled);
    } finally {
      ambryRequests = temp;
    }
  }

  /**
   * Tests for success and failure scenarios for UNDELETE
   */
  @Test
  public void undeleteTest() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    long opTimeMs = SystemTime.getInstance().milliseconds();

    // since we already test store key conversion in ttlupdate, we don't test it again in this method.
    // storekey not valid for store
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.Blob_Not_Found);
    // valid now
    validKeysInStore.add(blobId);
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.No_Error);

    // READ_ONLY is fine too
    changePartitionState(id, true);
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.No_Error);
    changePartitionState(id, false);

    miscUndeleteFailuresTest();
  }

  // helpers

  // general

  /**
   * Calls {@link AmbryRequests#handleRequests(NetworkRequest)} with {@code request} and returns the {@link Response} received.
   * @param request the {@link NetworkRequest} to process
   * @param expectedServerErrorCode the expected {@link ServerErrorCode} in the server response.
   * @return the {@link Response} received.
   * @throws InterruptedException
   * @throws IOException
   */
  private Response sendRequestGetResponse(RequestOrResponse request, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    NetworkRequest mockRequest = MockRequest.fromRequest(request);
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
   * @param request the {@link NetworkRequest} to send to {@link AmbryRequests}
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
    if (expectedErrorCode.equals(ServerErrorCode.No_Error) || (forceCheckOpReceived && !expectedErrorCode.equals(
        ServerErrorCode.Temporarily_Disabled))) {
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
   * @param storeControlRequestType type of control operation that will be performed on certain store.
   * @param numReplicasCaughtUpPerPartition the number of peer replicas which have caught up with this store before proceeding.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyStoreControlRequest(PartitionId partitionId, BlobStoreControlAction storeControlRequestType,
      short numReplicasCaughtUpPerPartition, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, correlationId, clientId);
    BlobStoreControlAdminRequest blobStoreControlAdminRequest =
        new BlobStoreControlAdminRequest(numReplicasCaughtUpPerPartition, storeControlRequestType, adminRequest);
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
   * @throws StoreException
   */
  private ByteBuffer getDataInWriteSet(MessageWriteSet messageWriteSet, int size) throws StoreException {
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
    String clientId = TestUtils.getRandomString(10);
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
    String clientId = TestUtils.getRandomString(10);

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
      String clientId = TestUtils.getRandomString(10);
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
              Unpooled.wrappedBuffer(ByteBuffer.allocate(0)), 0, BlobType.DataBlob, null);
          break;
        case DeleteRequest:
          request = new DeleteRequest(correlationId, clientId, originalBlobId, SystemTime.getInstance().milliseconds());
          break;
        case UndeleteRequest:
          request =
              new UndeleteRequest(correlationId, clientId, originalBlobId, SystemTime.getInstance().milliseconds());
          break;
        case GetRequest:
          PartitionRequestInfo pRequestInfo = new PartitionRequestInfo(id, Collections.singletonList(originalBlobId));
          request =
              new GetRequest(correlationId, clientId, MessageFormatFlags.All, Collections.singletonList(pRequestInfo),
                  GetOption.Include_All);
          break;
        case ReplicaMetadataRequest:
          ReplicaMetadataRequestInfo rRequestInfo = new ReplicaMetadataRequestInfo(id,
              findTokenHelper.getFindTokenFactoryFromReplicaType(ReplicaType.DISK_BACKED).getNewFindToken(),
              "localhost", "/tmp", ReplicaType.DISK_BACKED, replicationConfig.replicaMetadataRequestVersion);
          request = new ReplicaMetadataRequest(correlationId, clientId, Collections.singletonList(rRequestInfo),
              Long.MAX_VALUE, replicationConfig.replicaMetadataRequestVersion);
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
    String clientId = TestUtils.getRandomString(10);
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
      origins.add(TestUtils.getRandomString(TestUtils.RANDOM.nextInt(8) + 2));
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
    String clientId = TestUtils.getRandomString(10);
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
    String clientId = TestUtils.getRandomString(10);
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
      ServerErrorCode expectedErrorCode)
      throws InterruptedException, IOException, MessageFormatException, StoreException {
    TtlUpdateRequest request = new TtlUpdateRequest(correlationId, clientId, blobId, expiresAtMs, opTimeMs);
    sendAndVerifyOperationRequest(request, expectedErrorCode, true);
    if (expectedErrorCode == ServerErrorCode.No_Error) {
      verifyTtlUpdate(request.getBlobId(), expiresAtMs, opTimeMs, MockStorageManager.messageWriteSetReceived);
    }
  }

  /**
   * Does a UNDELETE and checks for success if {@code expectedErrorCode} is {@link ServerErrorCode#No_Error}. Else,
   * checks for failure with the code {@code expectedErrorCode}.
   * @param correlationId the correlation ID to use in the request
   * @param clientId the client ID to use in the request
   * @param blobId the blob ID to use in the request
   * @param opTimeMs the operation time (ms) to use in the request
   * @param expectedErrorCode the expected {@link ServerErrorCode}
   * @throws Exception
   */
  private void doUndelete(int correlationId, String clientId, BlobId blobId, long opTimeMs,
      ServerErrorCode expectedErrorCode) throws Exception {
    UndeleteRequest request = new UndeleteRequest(correlationId, clientId, blobId, opTimeMs);
    sendAndVerifyOperationRequest(request, expectedErrorCode, true);
    if (expectedErrorCode == ServerErrorCode.No_Error) {
      verifyUndelete(request.getBlobId(), opTimeMs, MockStorageManager.messageWriteSetReceived);
    }
  }

  /**
   * Exercises various failure paths for TTL updates
   * @throws InterruptedException
   * @throws IOException
   */
  private void miscTtlUpdateFailuresTest() throws InterruptedException, IOException {
    PartitionId id = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
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
   * Exercises various failure paths for UNDELETEs
   * @throws Exception
   */
  private void miscUndeleteFailuresTest() throws Exception {
    PartitionId id = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    // store exceptions
    for (StoreErrorCodes code : StoreErrorCodes.values()) {
      MockStorageManager.storeException = new StoreException("expected", code);
      ServerErrorCode expectedErrorCode = ErrorMapping.getStoreErrorMapping(code);
      sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
          expectedErrorCode, true);
      MockStorageManager.storeException = null;
    }
    // runtime exception
    MockStorageManager.runtimeException = new RuntimeException("expected");
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
        ServerErrorCode.Unknown_Error, true);
    MockStorageManager.runtimeException = null;
    // store is not started/is stopped/otherwise unavailable - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
        ServerErrorCode.Replica_Unavailable, false);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk down
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
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
   * @throws StoreException
   */
  private void verifyTtlUpdate(BlobId blobId, long expiresAtMs, long opTimeMs, MessageWriteSet messageWriteSet)
      throws IOException, MessageFormatException, StoreException {
    BlobId key = (BlobId) conversionMap.getOrDefault(blobId, blobId);
    assertEquals("There should be one message in the write set", 1, messageWriteSet.getMessageSetInfo().size());
    MessageInfo info = messageWriteSet.getMessageSetInfo().get(0);

    // verify stream
    ByteBuffer record = getDataInWriteSet(messageWriteSet, (int) info.getSize());
    int expectedSize = record.remaining();
    InputStream stream = new ByteBufferInputStream(record);

    // verify stream
    MessageFormatInputStreamTest.checkTtlUpdateMessage(stream, null, key, key.getAccountId(), key.getContainerId(),
        expiresAtMs, opTimeMs, (short) 0);

    // verify MessageInfo
    // since the record has been verified, the buffer size before verification is a good indicator of size
    MessageInfoTest.checkGetters(info, key, expectedSize, false, true, false, expiresAtMs, null, key.getAccountId(),
        key.getContainerId(), opTimeMs, (short) 0);
  }

  /**
   * Verifies that the UNDELETE request was delivered to the {@link Store} correctly.
   * @param blobId the {@link BlobId} that was updated
   * @param opTimeMs the op time (in ms)
   * @param messageWriteSet the {@link MessageWriteSet} received at the {@link Store}
   * @throws IOException
   * @throws MessageFormatException
   * @throws StoreException
   */
  private void verifyUndelete(BlobId blobId, long opTimeMs, MessageWriteSet messageWriteSet) throws Exception {
    BlobId key = (BlobId) conversionMap.getOrDefault(blobId, blobId);
    assertEquals("There should be one message in the write set", 1, messageWriteSet.getMessageSetInfo().size());
    MessageInfo info = messageWriteSet.getMessageSetInfo().get(0);

    // verify stream
    ByteBuffer record = getDataInWriteSet(messageWriteSet, (int) info.getSize());
    int expectedSize = record.remaining();
    InputStream stream = new ByteBufferInputStream(record);

    // verify stream
    MessageFormatInputStreamTest.checkUndeleteMessage(stream, null, key, key.getAccountId(), key.getContainerId(),
        opTimeMs, storageManager.returnValueOfUndelete);

    // verify MessageInfo
    // since the record has been verified, the buffer size before verification is a good indicator of size
    MessageInfoTest.checkGetters(info, key, expectedSize, false, false, true, Utils.Infinite_Time, null,
        key.getAccountId(), key.getContainerId(), opTimeMs, storageManager.returnValueOfUndelete);
  }

  /**
   * Implementation of {@link NetworkRequest} to help with tests.
   */
  private static class MockRequest implements NetworkRequest {

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
      return new MockRequest(new ByteBufferDataInputStream(buffer));
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
     * {@link NetworkRequest} provided in the last call to {@link #sendResponse(Send, NetworkRequest, ServerNetworkResponseMetrics).
     */
    NetworkRequest lastOriginalRequest = null;

    /**
     * The {@link Send} provided in the last call to {@link #sendResponse(Send, NetworkRequest, ServerNetworkResponseMetrics).
     */
    Send lastResponse = null;

    MockRequestResponseChannel() {
      super(1, 1);
    }

    @Override
    public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics) {
      lastResponse = payloadToSend;
      lastOriginalRequest = originalRequest;
    }
  }
}
