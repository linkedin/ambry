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
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ErrorMapping;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
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
import com.github.ambry.protocol.AdminResponseWithContent;
import com.github.ambry.protocol.BatchDeletePartitionRequestInfo;
import com.github.ambry.protocol.BatchDeleteRequest;
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
import com.github.ambry.protocol.ReplicateBlobRequest;
import com.github.ambry.protocol.ReplicateBlobResponse;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestAPI;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.Response;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.replication.BackupCheckerThread;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.MockConnectionPool;
import com.github.ambry.replication.MockFindTokenHelper;
import com.github.ambry.replication.MockHost;
import com.github.ambry.replication.MockReplicationManager;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationTestHelper;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.DiskIOScheduler;
import com.github.ambry.store.IdUndeletedStoreException;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageInfoTest;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.MockWrite;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreMetrics;
import com.github.ambry.store.StoreTestUtils;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferDataInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.MockClusterMap.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link AmbryServerRequests}.
 */
@RunWith(Parameterized.class)
public class AmbryServerRequestsTest extends ReplicationTestHelper {

  private final FindTokenHelper findTokenHelper;
  private final MockClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final MockReplicationManager replicationManager;
  private final MockStatsManager statsManager;
  private final MockRequestResponseChannel requestResponseChannel;
  private final Set<StoreKey> validKeysInStore = new HashSet<>();
  private final Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
  private final MockStoreKeyConverterFactory storeKeyConverterFactory;
  private final ReplicationConfig replicationConfig;
  private final ServerConfig serverConfig;
  private final DiskManagerConfig diskManagerConfig;
  private final ClusterMapConfig clusterMapConfig;
  private final ServerMetrics serverMetrics;
  private final String localDc;
  private final ReplicaStatusDelegate mockDelegate = Mockito.mock(ReplicaStatusDelegate.class);
  private final boolean validateRequestOnStoreState;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();
  private MockStorageManager storageManager;
  private MockHelixParticipant helixParticipant;
  private AmbryServerRequests ambryRequests;
  private final StoreKeyFactory storeKeyFactory;

  public AmbryServerRequestsTest(boolean validateRequestOnStoreState, boolean enableContinuousReplication)
      throws IOException, ReplicationException, StoreException, InterruptedException, ReflectiveOperationException {
    super(ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2,
        ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6, enableContinuousReplication);
    this.validateRequestOnStoreState = validateRequestOnStoreState;
    clusterMap = new MockClusterMap();
    localDc = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    Properties properties = createProperties(validateRequestOnStoreState, true);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    replicationConfig = new ReplicationConfig(verifiableProperties);
    serverConfig = new ServerConfig(verifiableProperties);
    diskManagerConfig = new DiskManagerConfig(verifiableProperties);
    clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    StatsManagerConfig statsManagerConfig = new StatsManagerConfig(verifiableProperties);
    dataNodeId =
        clusterMap.getDataNodeIds().stream().filter(node -> node.getDatacenterName().equals(localDc)).findFirst().get();
    storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    findTokenHelper = new MockFindTokenHelper(storeKeyFactory, replicationConfig);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    helixParticipant = new MockHelixParticipant(clusterMapConfig);
    storageManager = new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, null);
    storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(conversionMap);
    replicationManager =
        MockReplicationManager.getReplicationManager(verifiableProperties, storageManager, clusterMap, dataNodeId,
            storeKeyConverterFactory);
    statsManager = new MockStatsManager(storageManager, clusterMap, clusterMap.getReplicaIds(dataNodeId),
        clusterMap.getMetricRegistry(), statsManagerConfig, null, dataNodeId);
    serverMetrics = new ServerMetrics(clusterMap.getMetricRegistry(), AmbryRequests.class, AmbryServer.class);
    requestResponseChannel = new MockRequestResponseChannel(new NetworkConfig(verifiableProperties));
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);
    storageManager.start();
    Mockito.when(mockDelegate.unseal(any())).thenReturn(true);
    Mockito.when(mockDelegate.unmarkStopped(anyList())).thenReturn(true);
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}});
  }

  private static Properties createProperties(boolean validateRequestOnStoreState,
      boolean handleUndeleteRequestEnabled) {
    List<TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new TestUtils.ZkInfo(null, "DC1", (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
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
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    properties.setProperty("num.io.threads", "1");
    properties.setProperty("queued.max.requests", "1");
    return properties;
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  /**
   * Close the storageManager created.
   */
  @After
  public void after() throws InterruptedException {
    storageManager.shutdown();
    nettyByteBufLeakHelper.afterTest();
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
        RequestOrResponseType.DeleteRequest, RequestOrResponseType.BatchDeleteRequest, RequestOrResponseType.TtlUpdateRequest,
        RequestOrResponseType.UndeleteRequest)) {
      for (Map.Entry<ReplicaState, ReplicaId> entry : stateToReplica.entrySet()) {
        if (request == RequestOrResponseType.PutRequest) {
          // for PUT request, it is not allowed on OFFLINE,BOOTSTRAP and INACTIVE when validateRequestOnStoreState = true
          if (AmbryServerRequests.PUT_ALLOWED_STORE_STATES.contains(entry.getKey())) {
            assertEquals("Error code is not expected for PUT request", ServerErrorCode.NoError,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          } else {
            assertEquals("Error code is not expected for PUT request",
                validateRequestOnStoreState ? ServerErrorCode.TemporarilyDisabled : ServerErrorCode.NoError,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          }
        } else if (AmbryServerRequests.UPDATE_REQUEST_TYPES.contains(request)) {
          // for UNDELETE/DELETE/TTL Update request, they are not allowed on OFFLINE,BOOTSTRAP and INACTIVE when validateRequestOnStoreState = true
          if (AmbryServerRequests.UPDATE_ALLOWED_STORE_STATES.contains(entry.getKey())) {
            assertEquals("Error code is not expected for DELETE/TTL Update", ServerErrorCode.NoError,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          } else {
            assertEquals("Error code is not expected for DELETE/TTL Update",
                validateRequestOnStoreState ? ServerErrorCode.TemporarilyDisabled : ServerErrorCode.NoError,
                ambryRequests.validateRequest(entry.getValue().getPartitionId(), request, false));
          }
        } else {
          // for GET request, all states should be allowed
          assertEquals("Error code is not expected for GET request", ServerErrorCode.NoError,
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
      doScheduleCompactionTest(id, ServerErrorCode.NoError);
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
  public void scheduleCompactionFailureTest() throws InterruptedException, IOException, StoreException {
    // partitionId not specified
    doScheduleCompactionTest(null, ServerErrorCode.BadRequest);

    PartitionId id = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    // store is not started - Replica_Unavailable
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.ReplicaUnavailable);
    storageManager.returnNullStore = false;

    // all stores are shutdown - Disk_Unavailable. This is simulated by shutting down the storage manager.
    storageManager.shutdown();
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.DiskUnavailable);

    // Recreate storage manager and pass it to AmbryRequests so that Disk Managers are created again
    storageManager = new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, null);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);
    storageManager.returnNullStore = false;
    storageManager.start();
    // make sure the disk is up when storageManager is restarted.
    doScheduleCompactionTest(id, ServerErrorCode.NoError);

    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk unavailable
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    doScheduleCompactionTest(id, ServerErrorCode.DiskUnavailable);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);

    // store cannot be scheduled for compaction - Unknown_Error
    storageManager.returnValueOfSchedulingCompaction = false;
    doScheduleCompactionTest(id, ServerErrorCode.UnknownError);
    storageManager.returnValueOfSchedulingCompaction = true;

    // exception while attempting to schedule - InternalServerError
    storageManager.exceptionToThrowOnSchedulingCompaction = new IllegalStateException();
    doScheduleCompactionTest(id, ServerErrorCode.UnknownError);
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
        {  RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest,
            RequestOrResponseType.BatchDeleteRequest, RequestOrResponseType.GetRequest,
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
    sendAndVerifyRequestControlRequest(RequestOrResponseType.AdminRequest, false, null, ServerErrorCode.BadRequest);
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
      doControlReplicationTest(id, ServerErrorCode.NoError);
    }
    doControlReplicationTest(null, ServerErrorCode.NoError);
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
        clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0), ServerErrorCode.BadRequest);
    replicationManager.reset();
    replicationManager.exceptionToThrow = new IllegalStateException();
    sendAndVerifyReplicationControlRequest(Collections.emptyList(), false,
        clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0), ServerErrorCode.UnknownError);
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
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, true);
    // all replicas of given partition = acceptableLag
    generateLagOverrides(acceptableLagInBytes, acceptableLagInBytes);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, true);
    // 1 replica of some other partition > acceptableLag
    String key = MockReplicationManager.getPartitionLagKey(otherPartRemoteRep.getPartitionId(),
        otherPartRemoteRep.getDataNodeId().getHostname(), otherPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, true);
    // 1 replica of this partition > acceptableLag
    key = MockReplicationManager.getPartitionLagKey(id, thisPartRemoteRep.getDataNodeId().getHostname(),
        thisPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, false);
    // same result if num expected replicas == total count -1.
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 1), ServerErrorCode.NoError, false);
    // caught up if num expected replicas == total count - 2
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 2), ServerErrorCode.NoError, true);
    // caught up if num expected replicas == total count - 3
    doCatchupStatusTest(id, acceptableLagInBytes, (short) (replicaIds.size() - 3), ServerErrorCode.NoError, true);
    // all replicas of this partition > acceptableLag
    generateLagOverrides(acceptableLagInBytes + 1, acceptableLagInBytes + 1);
    doCatchupStatusTest(id, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, false);

    // cases with no partition id provided
    // all replicas of all partitions < acceptableLag
    generateLagOverrides(0, acceptableLagInBytes - 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, true);
    // all replicas of all partitions = acceptableLag
    generateLagOverrides(acceptableLagInBytes, acceptableLagInBytes);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, true);
    // 1 replica of one partition > acceptableLag
    key = MockReplicationManager.getPartitionLagKey(id, thisPartRemoteRep.getDataNodeId().getHostname(),
        thisPartRemoteRep.getReplicaPath());
    replicationManager.lagOverrides.put(key, acceptableLagInBytes + 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, false);
    // same result if num expected replicas == total count -1.
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 1), ServerErrorCode.NoError, false);
    // caught up if num expected replicas == total count - 2
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 2), ServerErrorCode.NoError, true);
    // caught up if num expected replicas == total count - 3
    doCatchupStatusTest(null, acceptableLagInBytes, (short) (replicaIds.size() - 3), ServerErrorCode.NoError, true);
    // all replicas of all partitions > acceptableLag
    generateLagOverrides(acceptableLagInBytes + 1, acceptableLagInBytes + 1);
    doCatchupStatusTest(null, acceptableLagInBytes, Short.MAX_VALUE, ServerErrorCode.NoError, false);
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#CatchupStatus} fails when bad input is provided (or when there is
   * bad internal state).
   */
  @Test
  public void catchupStatusFailureTest() throws InterruptedException, IOException {
    // acceptableLagInBytes < 0
    doCatchupStatusTest(null, -1, Short.MAX_VALUE, ServerErrorCode.BadRequest, false);
    // numReplicasCaughtUpPerPartition = 0
    doCatchupStatusTest(null, 0, (short) 0, ServerErrorCode.BadRequest, false);
    // numReplicasCaughtUpPerPartition < 0
    doCatchupStatusTest(null, 0, (short) -1, ServerErrorCode.BadRequest, false);
    // replication manager error
    replicationManager.reset();
    replicationManager.exceptionToThrow = new IllegalStateException();
    doCatchupStatusTest(null, 0, Short.MAX_VALUE, ServerErrorCode.UnknownError, false);
  }

  /**
   * Tests for the response received on a {@link BlobStoreControlAdminRequest} for successful case
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void controlBlobStoreSuccessTest() throws Exception {
    // Recreate storage manager and ambryRequest to pass in HelixParticipant
    storageManager.shutdown();
    helixParticipant.overrideDisableReplicaMethod = true;
    storageManager =
        new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, helixParticipant);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);
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
        ServerErrorCode.NoError);
    // verify APIs are called in the process of stopping BlobStore
    assertEquals("Compaction on store should be disabled after stopping the BlobStore", false,
        storageManager.compactionEnableVal);
    assertEquals("Partition disabled for compaction not as expected", id,
        storageManager.compactionControlledPartitionId);
    assertTrue("Origins list should be empty", replicationManager.originsVal.isEmpty());
    assertEquals("Replication on given BlobStore should be disabled", false, replicationManager.enableVal);
    assertEquals("Partition shutdown not as expected", id, storageManager.shutdownPartitionId);
    assertEquals("Partition disabled not as expected", id.toPathString(),
        helixParticipant.getDisabledReplicas().get(0));
    // start BlobStore
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.NoError);
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
        ServerErrorCode.NoError);
    // remove BlobStore (remove previously added store)
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    doNothing().when(mockStore).deleteStoreFiles();
    Mockito.when(mockStore.getReplicaStatusDelegates()).thenReturn(Collections.singletonList(mockDelegate));
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.NoError);
    storageManager.overrideStoreToReturn = null;
    helixParticipant.overrideDisableReplicaMethod = false;
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
        ServerErrorCode.ReplicaUnavailable);

    // create newPartition2 that has one replica on current node
    PartitionId newPartition2 = clusterMap.createNewPartition(clusterMap.getDataNodes());
    // test that adding store into StorageManager fails
    storageManager.returnValueOfAddBlobStore = false;
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.UnknownError);
    storageManager.returnValueOfAddBlobStore = true;

    // test that adding replica into ReplicationManager fails (we first add replica into ReplicationManager to trigger failure)
    ReplicaId replicaToAdd = clusterMap.getBootstrapReplica(newPartition2.toPathString(), dataNodeId);
    replicationManager.addReplica(replicaToAdd);
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.UnknownError);
    assertTrue("Remove replica from replication manager should succeed.",
        replicationManager.removeReplica(replicaToAdd));

    // test that adding replica into StatsManager fails
    statsManager.returnValOfAddReplica = false;
    sendAndVerifyStoreControlRequest(newPartition2, BlobStoreControlAction.AddStore, (short) 0,
        ServerErrorCode.UnknownError);
    statsManager.returnValOfAddReplica = true;
  }

  @Test
  public void removeBlobStoreFailureTest() throws Exception {
    // first, create new partition but don't add to current node
    PartitionId newPartition = clusterMap.createNewPartition(clusterMap.getDataNodes());
    // test store removal failure because store doesn't exist
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.PartitionUnknown);
    // add store on current node for store removal testing
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.AddStore, (short) 0, ServerErrorCode.NoError);
    // mock exception in StorageManager
    storageManager.returnValueOfRemoveBlobStore = false;
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.UnknownError);
    storageManager.returnValueOfRemoveBlobStore = true;
    // mock exception when deleting files of removed store
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    doThrow(new IOException()).when(mockStore).deleteStoreFiles();
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.UnknownError);
    // test store removal success case
    doNothing().when(mockStore).deleteStoreFiles();
    Mockito.when(mockStore.getReplicaStatusDelegates()).thenReturn(Collections.singletonList(mockDelegate));
    sendAndVerifyStoreControlRequest(newPartition, BlobStoreControlAction.RemoveStore, (short) 0,
        ServerErrorCode.NoError);
    storageManager.overrideStoreToReturn = null;
  }

  /**
   * Tests for the startBlobStore response received on a {@link BlobStoreControlAdminRequest} for different failure cases
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void startBlobStoreFailureTest() throws Exception {
    // Recreate storage manager and ambryRequest to pass in HelixParticipant
    storageManager.shutdown();
    storageManager =
        new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, helixParticipant);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);

    // find a partition that has a replica on current node
    ReplicaId localReplica = clusterMap.getReplicaIds(dataNodeId).get(0);
    MockPartitionId id = (MockPartitionId) localReplica.getPartitionId();
    short numReplicasCaughtUpPerPartition = 3;
    // test start BlobStore failure
    storageManager.returnValueOfStartingBlobStore = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.returnValueOfStartingBlobStore = true;
    // test start BlobStore with runtime exception
    storageManager.exceptionToThrowOnStartingBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.exceptionToThrowOnStartingBlobStore = null;
    // test enable replication failure
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    replicationManager.controlReplicationReturnVal = true;
    // test enable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.returnValueOfControllingCompaction = true;
    // test enable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.exceptionToThrowOnControllingCompaction = null;
    // test HelixParticipant resets partition error
    helixParticipant.resetPartitionVal = false;
    id.replicaAndState.put(localReplica, ReplicaState.ERROR);
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    helixParticipant.resetPartitionVal = true;
    id.replicaAndState.put(localReplica, ReplicaState.STANDBY);
    // test stop list update failure
    helixParticipant.setStoppedStateReturnVal = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StartStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    helixParticipant.setStoppedStateReturnVal = null;
  }

  /**
   * Tests for the stopBlobStore response received on a {@link BlobStoreControlAdminRequest} for different failure cases
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void stopBlobStoreFailureTest() throws InterruptedException, IOException, StoreException {
    List<? extends PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    PartitionId id = partitionIds.get(0);
    short numReplicasCaughtUpPerPartition = 3;
    // test partition unknown
    sendAndVerifyStoreControlRequest(null, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.BadRequest);
    // test validate request failure - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.ReplicaUnavailable);
    storageManager.returnNullStore = false;
    // test validate request failure - Disk_Unavailable
    storageManager.shutdown();
    storageManager.returnNullStore = true;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.DiskUnavailable);
    // Recreate storage manager and pass it to AmbryRequests so that disk managers are created again
    storageManager = new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, null);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);
    storageManager.returnNullStore = false;
    storageManager.start();
    // test invalid numReplicasCaughtUpPerPartition
    numReplicasCaughtUpPerPartition = -1;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.BadRequest);
    numReplicasCaughtUpPerPartition = 3;
    // test disable compaction failure
    storageManager.returnValueOfControllingCompaction = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.returnValueOfControllingCompaction = true;
    // test disable compaction with runtime exception
    storageManager.exceptionToThrowOnControllingCompaction = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    storageManager.exceptionToThrowOnControllingCompaction = null;
    // test disable replication failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = false;
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    // test peers catchup failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    // all replicas of this partition > acceptableLag
    generateLagOverrides(1, 1);
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.RetryAfterBackoff);
    // test shutdown BlobStore failure
    replicationManager.reset();
    replicationManager.controlReplicationReturnVal = true;
    storageManager.returnValueOfShutdownBlobStore = false;
    generateLagOverrides(0, 0);
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
    // test shutdown BlobStore with runtime exception
    storageManager.exceptionToThrowOnShuttingDownBlobStore = new IllegalStateException();
    sendAndVerifyStoreControlRequest(id, BlobStoreControlAction.StopStore, numReplicasCaughtUpPerPartition,
        ServerErrorCode.UnknownError);
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
          ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
      BlobId convertedBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.CRAFTED,
          ClusterMap.UNKNOWN_DATACENTER_ID, originalBlobId.getAccountId(), originalBlobId.getContainerId(), partitionId,
          false, BlobId.BlobDataType.DATACHUNK);
      conversionMap.put(originalBlobId, convertedBlobId);
      validKeysInStore.add(convertedBlobId);
      blobIds.add(originalBlobId);
    }
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.NoError);

    // test with duplicates
    List<BlobId> blobIdsWithDups = new ArrayList<>(blobIds);
    // add the same blob ids
    blobIdsWithDups.addAll(blobIds);
    // add converted ids
    conversionMap.values().forEach(id -> blobIdsWithDups.add((BlobId) id));
    sendAndVerifyGetOriginalStoreKeys(blobIdsWithDups, ServerErrorCode.NoError);
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
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
    blobIds.add(originalBlobId);
    conversionMap.put(originalBlobId, null);
    validKeysInStore.add(originalBlobId);
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.NoError);

    // Check a invalid key mapped to null
    originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partitionId, false, BlobId.BlobDataType.DATACHUNK);
    blobIds.add(originalBlobId);
    conversionMap.put(originalBlobId, null);
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.BlobNotFound);

    // Check exception
    storeKeyConverterFactory.setException(new Exception("StoreKeyConverter Mock Exception"));
    sendAndVerifyGetOriginalStoreKeys(blobIds, ServerErrorCode.UnknownError);
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
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    long opTimeMs = SystemTime.getInstance().milliseconds();

    // storekey not valid for store
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.BlobNotFound);
    // valid now
    validKeysInStore.add(blobId);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.NoError);
    // after conversion
    validKeysInStore.remove(blobId);
    BlobId converted = new BlobId(blobId.getVersion(), BlobId.BlobIdType.CRAFTED, blobId.getDatacenterId(),
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), blobId.getPartition(),
        BlobId.isEncrypted(blobId.getID()), BlobId.BlobDataType.DATACHUNK);
    // not in conversion map
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.BlobNotFound);
    // in conversion map but key not valid
    conversionMap.put(blobId, converted);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.BlobNotFound);
    // valid now
    validKeysInStore.add(converted);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.NoError);

    // READ_ONLY is fine too
    changePartitionState(id, true);
    doTtlUpdate(correlationId++, clientId, blobId, expiresAtMs, opTimeMs, ServerErrorCode.NoError);
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
        diskManagerConfig, storeKeyConverterFactory, statsManager, null);

    AmbryServerRequests temp = ambryRequests;
    ambryRequests = other;
    try {
      MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
      int correlationId = TestUtils.RANDOM.nextInt();
      String clientId = TestUtils.getRandomString(10);
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
      long opTimeMs = SystemTime.getInstance().milliseconds();
      doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.TemporarilyDisabled);
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
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    long opTimeMs = SystemTime.getInstance().milliseconds();

    // since we already test store key conversion in ttlupdate, we don't test it again in this method.
    // storekey not valid for store
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.BlobNotFound);
    // valid now
    validKeysInStore.add(blobId);
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.NoError);

    // READ_ONLY is fine too
    changePartitionState(id, true);
    doUndelete(correlationId++, clientId, blobId, opTimeMs, ServerErrorCode.NoError);
    changePartitionState(id, false);

    miscUndeleteFailuresTest();
  }

  /**
   *  Tests that delete operation on a non-existent blob fails with Blob_Not_Found error when 'isForceDelete' flag is
   *  disabled in 'DeleteRequest'
   */
  @Test
  public void forceDeleteDisabledTest() throws StoreException, IOException, InterruptedException {
    assumeFalse(this.validateRequestOnStoreState);
    MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    RequestOrResponse request =
        new DeleteRequest(correlationId, clientId, blobId, SystemTime.getInstance().milliseconds(), false);
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    doThrow(new StoreException("Blob is not found", StoreErrorCodes.IDNotFound)).when(mockStore).delete(anyList());
    // Operation should fail with Blob_Not_Found error code
    sendRequestGetResponse(request, ServerErrorCode.BlobNotFound);
    // Verify forceDelete() is not called
    Mockito.verify(mockStore, never()).forceDelete(anyList());
    storageManager.overrideStoreToReturn = null;
  }

  /**
   *  Tests that delete operation on a non-existent blob succeeds when 'isForceDelete' flag is
   *  enabled in 'DeleteRequest'
   */
  @Test
  public void forceDeleteEnabledTest() throws StoreException, IOException, InterruptedException {
    assumeFalse(this.validateRequestOnStoreState);
    MockPartitionId id = (MockPartitionId) clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
    RequestOrResponse request =
        new DeleteRequest(correlationId, clientId, blobId, SystemTime.getInstance().milliseconds(), (short) 3, true);
    BlobStore mockStore = Mockito.mock(BlobStore.class);
    storageManager.overrideStoreToReturn = mockStore;
    mockStore.setCurrentState(ReplicaState.STANDBY);
    doThrow(new StoreException("Blob is not found", StoreErrorCodes.IDNotFound)).when(mockStore).delete(anyList());
    doNothing().when(mockStore).forceDelete(anyList());
    // Operation should succeed
    sendRequestGetResponse(request, ServerErrorCode.NoError);
    // Verify delete and forceDelete are called
    Mockito.verify(mockStore, atLeastOnce()).delete(anyList());
    Mockito.verify(mockStore, atLeastOnce()).forceDelete(anyList());
    storageManager.overrideStoreToReturn = null;
  }

  /**
   * Tests success case for ReplicateBlobRequest.
   * Should replicate the PutBlob from the remote host.
   */
  @Test
  public void testReplicateBlobSuccess() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost remoteHost = null;
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId && remoteHost == null) {
        remoteHost = host;
      }
      hosts.put(host.dataNodeId, host);
    }
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKey;

    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, remoteHost.dataNodeId.getHostname(),
            remoteHost.dataNodeId.getPort());

    storageManager.resetStore();
    Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Operation received at the store not as expected", RequestOrResponseType.PutRequest,
        MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest is successful. ", replicateBlobResponse.getError(),
        ServerErrorCode.NoError);
  }

  /**
   * Tests ReplicateBlobRequest when the Blob on the source host is deleted.
   * Should replicate both the PutBlob and the DELETE.
   */
  @Test
  public void testReplicateBlobWhenSourceIsDeleted() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost remoteHost = null;
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId && remoteHost == null) {
        remoteHost = host;
      }
      hosts.put(host.dataNodeId, host);
    }
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);
    // add delete to the remote host
    addDeleteMessagesToReplicasOfPartition(partitionId, storeKey, Arrays.asList(remoteHost));

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKey;

    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, remoteHost.dataNodeId.getHostname(),
            remoteHost.dataNodeId.getPort());
    storageManager.resetStore();
    validKeysInStore.add(blobId);
    Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Operation received at the store not as expected", RequestOrResponseType.DeleteRequest,
        MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest is successful. ", replicateBlobResponse.getError(),
        ServerErrorCode.NoError);
  }

  /**
   * Tests ReplicateBlobRequest when the Blob on the source host is updated.
   * Should replicate both the PutBlob and the TtlUpdate.
   */
  @Test
  public void testReplicateBlobWhenSourceTtlUpdated() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost remoteHost = null;
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId && remoteHost == null) {
        remoteHost = host;
      }
      hosts.put(host.dataNodeId, host);
    }
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);
    // add TtlUpdate to the remote host
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, storeKey, Arrays.asList(remoteHost), -1);

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKey;

    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, remoteHost.dataNodeId.getHostname(),
            remoteHost.dataNodeId.getPort());
    storageManager.resetStore();
    validKeysInStore.add(blobId);
    Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Operation received at the store not as expected", RequestOrResponseType.TtlUpdateRequest,
        MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest is successful. ", replicateBlobResponse.getError(),
        ServerErrorCode.NoError);
  }

  /**
   * Tests ReplicateBlobRequest when the Blob on the source host doesn't exist.
   */
  @Test
  public void testReplicateBlobWhenSourceNotExist() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost remoteHost = null;
    MockHost randomHost = null;
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId) {
        if (remoteHost == null) {
          remoteHost = host;
        } else {
          randomHost = host;
        }
      }
      hosts.put(host.dataNodeId, host);
    }
    // remoteHost has the Blob with "storeKey" key but doesn't have the BLob with "storeKeyNotExist" key
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);
    StoreKey storeKeyNotExist = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(randomHost), 1).get(0);

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKeyNotExist;

    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, remoteHost.dataNodeId.getHostname(),
            remoteHost.dataNodeId.getPort());
    storageManager.resetStore();
    Response response = sendRequestGetResponse(request, ServerErrorCode.BlobNotFound);
    assertEquals("Operation received at the store not as expected", null, MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest fails with Blob_Not_Found.", replicateBlobResponse.getError(),
        ServerErrorCode.BlobNotFound);
  }

  /**
   * Test the source data node is the server itself
   * Should return successful status but do nothing
   */
  @Test
  public void testReplicateBlobToItself() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost remoteHost = null;
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      // set remoteHost to itself
      if (dataNodeId == host.dataNodeId) {
        remoteHost = host;
      }
      hosts.put(host.dataNodeId, host);
    }
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1).get(0);

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKey;

    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, remoteHost.dataNodeId.getHostname(),
            remoteHost.dataNodeId.getPort());

    storageManager.resetStore();
    Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
    // shouldn't receive any request
    assertEquals("Operation received at the store not as expected", null, MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest is successful. ", replicateBlobResponse.getError(),
        ServerErrorCode.NoError);
  }

  @Test
  public void testReplicateBlobWhenTargetConditions() throws Exception {
    // Besides the above test cases to test the different conditions on the source host, need verify the different cases on the local store.
    // We test it with Integration test which uses the production BlobStore instead of the mock layer.
    // Refer to ServerHttp2Test.replicateBlobCaseTest
    // test PutBlob but StoreErrorCodes.Already_Exist
    // test PutBlob and applyTTLUpdate but hit StoreErrorCodes.Already_Updated or StoreErrorCodes.ID_Deleted
    // test PutBlob and applyDelete but hit StoreErrorCodes.ID_Deleted or StoreErrorCodes.Life_Version_Conflict
  }

  /**
   * Tests ReplicateBlobRequestV2: either invalid request or no-op requests.
   */
  @Test
  public void testReplicateBlobV2InvalidOrNoOpRequest() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost sourceHost = null; // the remote source host to replicate the blob from
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId && sourceHost == null) {
        sourceHost = host;
      }
      hosts.put(host.dataNodeId, host);
    }
    StoreKey storeKey = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(sourceHost), 1).get(0);

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    BlobId blobId = (BlobId) storeKey;

    storageManager.resetStore();

    // ReplicateBlobRequest operationType RequestOrResponseType.GetRequest is wrong, expect to return Bad_Request.
    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
            sourceHost.dataNodeId.getPort(), RequestOrResponseType.GetRequest, 0, (short) 0, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);
    assertEquals("expect no operation is received", null, MockStorageManager.operationReceived);

    // ReplicateBlobRequest operationType RequestOrResponseType.PutRequest is wrong, expect to return Bad_Request.
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.PutRequest, 0, (short) 0, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);
    assertEquals("expect no operation is received", null, MockStorageManager.operationReceived);

    // ReplicateBlobRequest operationType RequestOrResponseType.PutResponse is wrong, expect to return Bad_Request.
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.PutResponse, 0, (short) 0, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);
    assertEquals("expect no operation is received", null, MockStorageManager.operationReceived);

    // lifeVersion is not expected
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, dataNodeId.getHostname(), dataNodeId.getPort(),
        RequestOrResponseType.TtlUpdateRequest, System.currentTimeMillis(), (short) 0, -1);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);
    assertEquals("expect no operation is received", null, MockStorageManager.operationReceived);

    // source replica is itself, do nothing
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, dataNodeId.getHostname(), dataNodeId.getPort(),
        RequestOrResponseType.TtlUpdateRequest, System.currentTimeMillis(), (short) -1, -1);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("expect no operation is received", null, MockStorageManager.operationReceived);
    ReplicateBlobResponse replicateBlobResponse = (ReplicateBlobResponse) response;
    assertEquals("expect ReplicateBlobRequest is successful. ", replicateBlobResponse.getError(),
        ServerErrorCode.NoError);
  }

  /**
   * Tests ReplicateBlobRequestV2 to repair TtlUpdate or Delete.
   */
  @Test
  public void testReplicateBlobV2() throws Exception {
    Assume.assumeTrue(
        MessageFormatRecord.getCurrentMessageHeaderVersion() >= MessageFormatRecord.Message_Header_Version_V3);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    MockPartitionId partitionId =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MockHost sourceHost = null; // the remote source host to replicate the blob from
    for (ReplicaId replica : partitionId.getReplicaIds()) {
      MockHost host = new MockHost(replica.getDataNodeId(), clusterMap);
      if (dataNodeId != host.dataNodeId) {
        if (sourceHost == null) {
          sourceHost = host;
        }
      }
      hosts.put(host.dataNodeId, host);
    }

    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 5);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, storeKeyFactory,
        serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant, connectionPool);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);
    storageManager.resetStore();

    // PutBlob and TtlUpdate exists on the source host
    BlobId blobId = (BlobId) addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(sourceHost), 1).get(0);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, (StoreKey) blobId, Arrays.asList(sourceHost), -1);
    ReplicateBlobRequest request =
        new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
            sourceHost.dataNodeId.getPort(), RequestOrResponseType.PutRequest, 0, (short) 0, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);

    storageManager.resetStore();
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.TtlUpdateRequest, 0, (short) -1, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    validKeysInStore.add(blobId);
    sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Received operation is not expected.", RequestOrResponseType.TtlUpdateRequest,
        MockStorageManager.operationReceived);
    validKeysInStore.remove(blobId);

    // PutBlob, TtlUpdate and Delete exists on the source host
    blobId = (BlobId) addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(sourceHost), 1).get(0);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, (StoreKey) blobId, Arrays.asList(sourceHost), -1);
    addDeleteMessagesToReplicasOfPartition(partitionId, (StoreKey) blobId, Arrays.asList(sourceHost));
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.PutRequest, 0, (short) 0, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    sendRequestGetResponse(request, ServerErrorCode.BadRequest);

    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.TtlUpdateRequest, 0, (short) -1, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    validKeysInStore.add(blobId);
    sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Received operation is not expected.", RequestOrResponseType.TtlUpdateRequest,
        MockStorageManager.operationReceived);
    validKeysInStore.remove(blobId);

    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.DeleteRequest, 0, (short) -1, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    validKeysInStore.add(blobId);
    sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Received operation is not expected.", RequestOrResponseType.DeleteRequest,
        MockStorageManager.operationReceived);
    validKeysInStore.remove(blobId);

    // PutBlob and Delete exists on the source host
    blobId = (BlobId) addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(sourceHost), 1).get(0);
    addDeleteMessagesToReplicasOfPartition(partitionId, (StoreKey) blobId, Arrays.asList(sourceHost));
    request = new ReplicateBlobRequest(correlationId, clientId, blobId, sourceHost.dataNodeId.getHostname(),
        sourceHost.dataNodeId.getPort(), RequestOrResponseType.DeleteRequest, 0, (short) -1, 0);
    assertEquals(ReplicateBlobRequest.VERSION_2, request.getVersionId());
    validKeysInStore.add(blobId);
    sendRequestGetResponse(request, ServerErrorCode.NoError);
    assertEquals("Received operation is not expected.", RequestOrResponseType.DeleteRequest,
        MockStorageManager.operationReceived);
    validKeysInStore.remove(blobId);

    // We test the cases that target under different conditions with integration test.
  }

  /**
   * Test that cross-colo metrics are updated correctly when {@link AmbryRequests} handles {@link GetRequest} and
   * {@link ReplicaMetadataRequest}
   * @throws Exception
   */
  @Test
  public void crossColoMetricsUpdateTest() throws Exception {
    // find a node in remote dc
    DataNodeId remoteNode = clusterMap.getDataNodeIds()
        .stream()
        .filter(node -> !node.getDatacenterName().equals(localDc))
        .findFirst()
        .get();
    PartitionId id = clusterMap.getReplicaIds(remoteNode).get(0).getPartitionId();
    // send cross-colo metadata request and verify
    String clientId = "replication-metadata-" + remoteNode.getHostname() + "[" + remoteNode.getDatacenterName() + "]";
    List<Response> responseList =
        sendAndVerifyOperationRequest(RequestOrResponseType.ReplicaMetadataRequest, Collections.singletonList(id),
            ServerErrorCode.NoError, null, clientId);
    assertEquals("cross-colo metadata exchange bytes are not expected", responseList.get(0).sizeInBytes(),
        serverMetrics.crossColoMetadataExchangeBytesRate.get(remoteNode.getDatacenterName()).getCount());
    responseList.forEach(Response::release);
    // send cross-colo get request and verify
    clientId =
        GetRequest.Replication_Client_Id_Prefix + remoteNode.getHostname() + "[" + remoteNode.getDatacenterName() + "]";
    responseList = sendAndVerifyOperationRequest(RequestOrResponseType.GetRequest, Collections.singletonList(id),
        ServerErrorCode.NoError, null, clientId);
    assertEquals("cross-colo fetch bytes are not expected", responseList.get(0).sizeInBytes(),
        serverMetrics.crossColoFetchBytesRate.get(remoteNode.getDatacenterName()).getCount());
    responseList.forEach(Response::release);
  }

  void verifyCRCInMetadata(PartitionId id, String replicaPath, DataNodeId datanode, Long crc)
      throws IOException, InterruptedException {
    String clientId = "replication-metadata-" + datanode.getHostname() + "[" + datanode.getDatacenterName() + "]";
    ReplicaMetadataRequestInfo rinfo = new ReplicaMetadataRequestInfo(id,
        new StoreFindToken(), datanode.getHostname(), replicaPath, ReplicaType.DISK_BACKED,
        replicationConfig.replicaMetadataRequestVersion);
    RequestOrResponse md_request = new ReplicaMetadataRequest(TestUtils.RANDOM.nextInt(), clientId,
        Collections.singletonList(rinfo), Long.MAX_VALUE, replicationConfig.replicaMetadataRequestVersion);
    ReplicaMetadataResponse response =
        (ReplicaMetadataResponse) sendRequestGetResponse(md_request, ServerErrorCode.NoError);
    assertTrue("response from replica must not be empty",
        ((ReplicaMetadataResponse) response).getReplicaMetadataResponseInfoList().size() > 0);
    // Compare CRC from in-mem store and metadata request
    for (ReplicaMetadataResponseInfo respinfo : response.getReplicaMetadataResponseInfoList()) {
      assertTrue("messages from replica must not be empty",respinfo.getMessageInfoList().size() > 0);
      for (MessageInfo minfo : respinfo.getMessageInfoList()) {
        assertEquals(String.format("Expected CRC = %s, Received CRC = %s", crc, minfo.getCrc()), crc, minfo.getCrc());
      }
    }
  }

  @Test
  public void testFetchCRC() throws IOException, InterruptedException, StoreException, ReflectiveOperationException {
    // This works for TestStore, not real store
    assumeFalse(this.validateRequestOnStoreState);

    // Get rid of  the mock helper, we need the real token-helper to issue real store requests
    FindTokenHelper findTokenHelper1 = new FindTokenHelper(storeKeyFactory, replicationConfig);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper1, null, replicationManager,
        storeKeyFactory, serverConfig, diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);

    DataNodeId datanode = clusterMap.getDataNodeIds().stream()
        .filter(node -> node.getDatacenterName().equals(localDc))
        .findFirst().get();
    ReplicaId replica = clusterMap.getReplicaIds(datanode).get(0);
    PartitionId id = replica.getPartitionId();

    // Create local store
    BlobStore localStore =
        new BlobStore(id.getReplicaIds().get(0), new StoreConfig(verifiableProperties),
            Utils.newScheduler(1, false), Utils.newScheduler(1, false),
            null, new DiskIOScheduler(null), StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
            new StoreMetrics(clusterMap.getMetricRegistry()), new StoreMetrics("UnderCompaction",
            clusterMap.getMetricRegistry()), null, null, null,
            Collections.singletonList(mock(ReplicaStatusDelegate.class)), new MockTime(),
            new InMemAccountService(false, false), null,
            Utils.newScheduler(1, false));
    localStore.start();
    storageManager.overrideStoreToReturn = localStore;

    // Create a blob
    BlobId blobId = generateRandomBlobId(id);
    String testContent = "test_content";
    BlobProperties properties = new BlobProperties(testContent.length(), "serviceId", blobId.getAccountId(),
        blobId.getAccountId(), false);
    ByteBuffer content_buf = ByteBuffer.allocate(testContent.length());
    content_buf.put(testContent.getBytes());
    content_buf.flip(); // Most important line !
    ByteBuf content_bytebuf = Unpooled.wrappedBuffer(content_buf);

    // Put blob
    String clientId = "replication-metadata-" + datanode.getHostname() + "[" + datanode.getDatacenterName() + "]";
    RequestOrResponse put_request = new PutRequest(TestUtils.RANDOM.nextInt(), clientId, blobId, properties,
        content_buf, content_bytebuf, testContent.length(), BlobType.DataBlob, null);
    Response response = sendRequestGetResponse(put_request, ServerErrorCode.NoError);

    // Send metadata request from regular server app
    verifyCRCInMetadata(id, id.toPathString(), datanode, null);

    // Get CRC from in-mem store
    EnumSet<StoreGetOptions> storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted,
        StoreGetOptions.Store_Include_Expired);
    StoreInfo stinfo = localStore.get(Collections.singletonList(blobId), storeGetOptions);
    MessageReadSet rdset = stinfo.getMessageReadSet();
    MessageInfo minfo2 = stinfo.getMessageReadSetInfo().get(0);
    rdset.doPrefetch(0, minfo2.getSize() - MessageFormatRecord.Crc_Size,
        MessageFormatRecord.Crc_Size);
    ByteBuf crcbuf = rdset.getPrefetchedData(0);
    long crc = crcbuf.getLong(0);
    crcbuf.release();
    verifyCRCInMetadata(id, BackupCheckerThread.DR_Verifier_Keyword + File.separator + id.toPathString(),
        datanode, new Long(crc));

    // Delete blob and get metadata again
    RequestOrResponse del_request = new DeleteRequest(TestUtils.RANDOM.nextInt(), clientId, blobId,
        SystemTime.getInstance().milliseconds());
    sendRequestGetResponse(del_request, ServerErrorCode.NoError);
    verifyCRCInMetadata(id, BackupCheckerThread.DR_Verifier_Keyword + File.separator + id.toPathString(),
        datanode, null);
  }

  /**
   * Performs the AdminRequest and Response as well as checking if the content is a json and expected
   * @param partitionId necessary to fulfill the {@link AdminRequest}
   * @param description for what this function call is attempting to test
   * @param expectedResponse the response that should be returned from {@link AdminResponseWithContent}
   * @throws IOException
   * @throws InterruptedException
   */
  public void doRequestAndHealthCheck(PartitionId partitionId, String description, JSONObject expectedResponse)
      throws IOException, InterruptedException {

    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = TestUtils.getRandomString(10);

    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.HealthCheck, partitionId, correlationId, clientId);

    AdminResponseWithContent response =
        (AdminResponseWithContent) sendRequestGetResponse(adminRequest, ServerErrorCode.NoError);
    String content = new String(response.getContent(), StandardCharsets.UTF_8);

    //Ensures the response is as expected
    JSONObject contentJSON = new JSONObject(content);

    //Either compares the health by string or compares the arrays without order
    assertEquals(description, expectedResponse.toString(), contentJSON.toString());
  }

  /**
   * Allows an extra property to be made and then encapsulates this property in a new environment to be tested on
   * @param properties the set of properties this environment will have
   * @param propertyKey the new key to be added to the properties variable
   * @param propertyValue the new value for the propertyKey
   * @throws IOException
   * @throws InterruptedException
   * @throws StoreException
   */
  public void setPropertyToAmbryRequests(Properties properties, String propertyKey, String propertyValue)
      throws IOException, InterruptedException, StoreException {
    storageManager.shutdown();
    properties.setProperty(propertyKey, propertyValue); //Adds the new key-value pair

    //Initializes the new environment through clusterMap, storageManager, and AmbryRequests
    ClusterMap clusterMap = new MockClusterMap();
    DiskManagerConfig diskManagerConfig = new DiskManagerConfig(new VerifiableProperties(properties));
    storageManager =
        new MockStorageManager(validKeysInStore, clusterMap, dataNodeId, findTokenHelper, null, diskManagerConfig);
    ambryRequests = new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), serverMetrics, findTokenHelper, null, replicationManager, null, serverConfig,
        diskManagerConfig, storeKeyConverterFactory, statsManager, helixParticipant);
    storageManager.start();
  }

  /**
   * Tests Replica State and Disk State for appropriate responses from healthCheck admin endpoint
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void healthCheckTest() throws InterruptedException, StoreException, IOException {

    //retrieves writeable partitions to test with
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
    JSONObject expectedJSONReponse = new JSONObject();
    expectedJSONReponse.put("brokenDisks", new JSONArray());
    expectedJSONReponse.put("unstablePartitions", new JSONArray());

    for (PartitionId id : partitionIds) {

      // Test 1: "Good" response excepted where the host state is in either Standby or Leader
      ReplicaState originalPartitionState = storageManager.getStore(id).getCurrentState();

      //change Partition to Standby or Leader
      storageManager.getStore(id)
          .setCurrentState(TestUtils.RANDOM.nextInt() % 2 == 0 ? ReplicaState.STANDBY : ReplicaState.LEADER);

      expectedJSONReponse.put("health", ServerHealthStatus.GOOD);
      doRequestAndHealthCheck(id, "Payload was expected to indicate healthy host", expectedJSONReponse);

      //Test 2: "BAD" response expected where we change partition to error State
      //change all replica states in partition to ERROR
      storageManager.getStore(id).setCurrentState(ReplicaState.ERROR);

      expectedJSONReponse.put("health", ServerHealthStatus.BAD);
      JSONArray unstablePartitions = new JSONArray();
      JSONObject unstablePartition = new JSONObject();
      unstablePartition.put("partitionId", id);
      unstablePartition.put("reason", ReplicaState.ERROR);
      unstablePartitions.put(unstablePartition);
      expectedJSONReponse.put("unstablePartitions", unstablePartitions);
      doRequestAndHealthCheck(id, "Payload was expected to be BAD with no disk healthchecks", expectedJSONReponse);

      //restore the state of the Partition
      storageManager.getStore(id).setCurrentState(originalPartitionState);
      expectedJSONReponse.put("unstablePartitions", new JSONArray());
    }

    //Test 3: "GOOD" response expected, enabling disk healthchecking on the partition of interest and
    // disk's should be healthy

    Properties currentProperties = createProperties(validateRequestOnStoreState, true);
    setPropertyToAmbryRequests(currentProperties, "disk.manager.disk.healthcheck.enabled", "true");
    TimeUnit.SECONDS.sleep(5); // Enough time to perform each step of disk healthcheck(create,write,read,delete)

    expectedJSONReponse.put("health", ServerHealthStatus.GOOD);
    doRequestAndHealthCheck(partitionIds.get(0),
        "Payload was expected to be GOOD with all disks passing disk healthcheck", expectedJSONReponse);

    //Restores the environment from no longer using disk healthchecks
    currentProperties = createProperties(validateRequestOnStoreState, true);
    setPropertyToAmbryRequests(currentProperties, "disk.manager.disk.healthcheck.enabled", "false");
  }

  // helpers

  // general

  /**
   * Calls {@link RequestAPI#handleRequests(NetworkRequest)} with {@code request} and returns the {@link Response} received.
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
   *                             {@link ServerErrorCode#NoError}. Skips the check otherwise.
   * @throws InterruptedException
   * @throws IOException
   * @return the response associated with given request.
   */
  private Response sendAndVerifyOperationRequest(RequestOrResponse request, ServerErrorCode expectedErrorCode,
      Boolean forceCheckOpReceived) throws InterruptedException, IOException {
    storageManager.resetStore();
    RequestOrResponseType requestType = request.getRequestType();
    Response response = sendRequestGetResponse(request,
        EnumSet.of(RequestOrResponseType.GetRequest, RequestOrResponseType.ReplicaMetadataRequest).contains(requestType)
            ? ServerErrorCode.NoError : expectedErrorCode);
    if (expectedErrorCode.equals(ServerErrorCode.NoError) || (forceCheckOpReceived && !expectedErrorCode.equals(
        ServerErrorCode.TemporarilyDisabled))) {
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
    return response;
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
    response.release();
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
    replicaId.setReplicaSealStatus(seal ? ReplicaSealStatus.SEALED : ReplicaSealStatus.NOT_SEALED);
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
    List<Response> responses = sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.NoError, null, null);
    responses.forEach(Response::release);
    // disable the request
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.NoError);
    // check that it is disabled
    responses = sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.TemporarilyDisabled, false, null);
    responses.forEach(Response::release);
    // ok to call disable again
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.NoError);
    // enable
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.NoError);
    // check that everything works
    responses = sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.NoError, null, null);
    responses.forEach(Response::release);
    // ok to call enable again
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.NoError);
    // check that everything works
    responses = sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.NoError, null, null);
    responses.forEach(Response::release);
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
    if (!expectedErrorCode.equals(ServerErrorCode.UnknownError)) {
      // known error will be filled to each PartitionResponseInfo and set ServerErrorCode.No_Error in response.
      Response response = sendRequestGetResponse(request, ServerErrorCode.NoError);
      assertEquals("Operation received at the store not as expected", RequestOrResponseType.GetRequest,
          MockStorageManager.operationReceived);
      for (PartitionResponseInfo info : ((GetResponse) response).getPartitionResponseInfoList()) {
        assertEquals("Error code does not match expected", expectedErrorCode, info.getErrorCode());
      }
      response.release();
    } else {
      sendRequestGetResponse(request, ServerErrorCode.UnknownError).release();
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
   *                             {@link ServerErrorCode#NoError}. Skips the check otherwise.
   * @param clientIdStr the clientId string to construct request. if null, generate a random string as clientId.
   * @throws InterruptedException
   * @throws IOException
   * @return a list of {@link Response}(s) associated with given partition ids.
   */
  private List<Response> sendAndVerifyOperationRequest(RequestOrResponseType requestType,
      List<? extends PartitionId> ids, ServerErrorCode expectedErrorCode, Boolean forceCheckOpReceived,
      String clientIdStr) throws InterruptedException, IOException {
    List<Response> responses = new ArrayList<>();
    for (PartitionId id : ids) {
      int correlationId = TestUtils.RANDOM.nextInt();
      String clientId = clientIdStr == null ? TestUtils.getRandomString(10) : clientIdStr;
      BlobId originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), id, false, BlobId.BlobDataType.DATACHUNK);
      BlobId convertedBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.CRAFTED,
          ClusterMap.UNKNOWN_DATACENTER_ID, originalBlobId.getAccountId(), originalBlobId.getContainerId(), id, false,
          BlobId.BlobDataType.DATACHUNK);
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
        case BatchDeleteRequest:
          BatchDeletePartitionRequestInfo batchDeletePartitionRequestInfo = new BatchDeletePartitionRequestInfo(id, Collections.singletonList(originalBlobId));
          request = new BatchDeleteRequest(correlationId, clientId, Collections.singletonList(batchDeletePartitionRequestInfo), SystemTime.getInstance().milliseconds());
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
      responses.add(sendAndVerifyOperationRequest(request, expectedErrorCode, forceCheckOpReceived));
    }
    return responses;
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
    response.release();
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
    if (!expectedServerErrorCode.equals(ServerErrorCode.UnknownError)) {
      assertEquals("Origins not as provided in request", origins, replicationManager.originsVal);
      assertEquals("Enable not as provided in request", enable, replicationManager.enableVal);
      assertEquals("Ids not as provided in request", idsVal.size(), replicationManager.idsVal.size());
      assertTrue("Ids not as provided in request", replicationManager.idsVal.containsAll(idsVal));
    }
    response.release();
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
    response.release();
  }

  // ttlUpdateTest() helpers

  /**
   * Does a TTL update and checks for success if {@code expectedErrorCode} is {@link ServerErrorCode#NoError}. Else,
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
    sendAndVerifyOperationRequest(request, expectedErrorCode, true).release();
    if (expectedErrorCode == ServerErrorCode.NoError) {
      verifyTtlUpdate(request.getBlobId(), expiresAtMs, opTimeMs, MockStorageManager.messageWriteSetReceived);
    }
  }

  /**
   * Does a UNDELETE and checks for success if {@code expectedErrorCode} is {@link ServerErrorCode#NoError}. Else,
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
    sendAndVerifyOperationRequest(request, expectedErrorCode, true).release();
    if (expectedErrorCode == ServerErrorCode.NoError) {
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
          expectedErrorCode, true, null);
      MockStorageManager.storeException = null;
    }
    // runtime exception
    MockStorageManager.runtimeException = new RuntimeException("expected");
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.UnknownError, true, null);
    MockStorageManager.runtimeException = null;
    // store is not started/is stopped/otherwise unavailable - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.ReplicaUnavailable, false, null);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk down
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    sendAndVerifyOperationRequest(RequestOrResponseType.TtlUpdateRequest, Collections.singletonList(id),
        ServerErrorCode.DiskUnavailable, false, null);
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
      MockStorageManager.storeException =
          code == StoreErrorCodes.IDUndeleted ? new IdUndeletedStoreException("expected", (short) 1)
              : new StoreException("expected", code);
      ServerErrorCode expectedErrorCode = ErrorMapping.getStoreErrorMapping(code);
      sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
          expectedErrorCode, true, null);
      MockStorageManager.storeException = null;
    }
    // runtime exception
    MockStorageManager.runtimeException = new RuntimeException("expected");
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
        ServerErrorCode.UnknownError, true, null);
    MockStorageManager.runtimeException = null;
    // store is not started/is stopped/otherwise unavailable - Replica_Unavailable
    storageManager.returnNullStore = true;
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
        ServerErrorCode.ReplicaUnavailable, false, null);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk down
    ReplicaId replicaId = findReplica(id);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    sendAndVerifyOperationRequest(RequestOrResponseType.UndeleteRequest, Collections.singletonList(id),
        ServerErrorCode.DiskUnavailable, false, null);
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
     * Constructs a {@link MockRequest}.
     * @param stream the {@link InputStream} that will be returned on a call to {@link #getInputStream()}.
     */
    private MockRequest(InputStream stream) {
      this.stream = stream;
    }

    /**
     * Constructs a {@link MockRequest} from {@code request}.
     * @param request the {@link RequestOrResponse} to construct the {@link MockRequest} for.
     * @return an instance of {@link MockRequest} that represents {@code request}.
     * @throws IOException
     */
    static MockRequest fromRequest(RequestOrResponse request) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate((int) request.sizeInBytes());
      request.writeTo(new ByteBufferChannel(buffer));
      request.release();
      buffer.flip();
      // read length (to bring it to a state where AmbryRequests can handle it).
      buffer.getLong();
      return new MockRequest(new ByteBufferDataInputStream(buffer));
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

    MockRequestResponseChannel(NetworkConfig networkConfig) {
      super(networkConfig);
    }

    @Override
    public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics) {
      lastResponse = payloadToSend;
      lastOriginalRequest = originalRequest;
    }
  }
}
