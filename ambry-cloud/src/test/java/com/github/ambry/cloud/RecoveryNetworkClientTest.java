/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.CloudServiceDataNode;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.DiskIOScheduler;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreMetrics;
import com.github.ambry.store.StoreTestUtils;
import com.github.ambry.utils.ByteBufferDataInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class RecoveryNetworkClientTest {

  private static final Logger logger = LoggerFactory.getLogger(RecoveryNetworkClientTest.class);
  protected BlobStore localStore;
  protected Map<String, MessageInfo> azureBlobs;
  protected RecoveryMetrics recoveryMetrics;
  protected RecoveryNetworkClient recoveryNetworkClient;
  protected RemoteReplicaInfo remoteReplicaInfo;
  protected ReplicaThread recoveryThread;
  protected final AtomicInteger requestId;
  protected final AzuriteUtils azuriteUtils;
  protected final CloudReplica cloudReplica;
  protected final CloudServiceDataNode cloudServiceDataNode;
  protected final ClusterMapConfig clusterMapConfig;
  protected final Container testContainer;
  protected final FindTokenHelper findTokenHelper;
  protected final MockClusterMap mockClusterMap;
  protected final MockPartitionId mockPartitionId;
  protected final Properties properties;
  protected final VerifiableProperties verifiableProperties;
  protected final int AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE = 3;
  protected final int NUM_BLOBS = 100;
  protected final long BLOB_SIZE = 1000;
  protected final short ACCOUNT_ID = 1024, CONTAINER_ID = 2048;
  public static final String LOCALHOST = "localhost";

  public RecoveryNetworkClientTest() throws Exception {
    requestId = new AtomicInteger(0);
    // Set test properties
    azuriteUtils = new AzuriteUtils();
    properties = azuriteUtils.getAzuriteConnectionProperties();
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(false));
    properties.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, "1");
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "Partition");
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE,
        String.valueOf(AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE));
    properties.setProperty(ReplicationConfig.REPLICATION_CLOUD_TOKEN_FACTORY,
        RecoveryTokenFactory.class.getCanonicalName());
    properties.setProperty("replication.metadata.request.version", "2");
    verifiableProperties = new VerifiableProperties(properties);
    findTokenHelper = new FindTokenHelper(null, new ReplicationConfig(verifiableProperties));
    // Create test cluster
    mockClusterMap = new MockClusterMap(false, true, 1,
        1, 1, true, false,
        "localhost");
    // Create a test partition
    mockPartitionId =
        (MockPartitionId) mockClusterMap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    // Create a cloud replica
    clusterMapConfig =  new ClusterMapConfig(new VerifiableProperties(properties));
    cloudServiceDataNode = new CloudServiceDataNode(LOCALHOST, clusterMapConfig);
    cloudReplica = new CloudReplica(mockPartitionId, cloudServiceDataNode);
    // Create a test container
    testContainer =
        new Container(CONTAINER_ID, "testContainer", Container.ContainerStatus.ACTIVE,
            "Test Container", false,
            false, false,
            false, null, false,
            false, Collections.emptySet(),
            false, false, Container.NamedBlobMode.DISABLED, ACCOUNT_ID, 0,
            0, 0, "",
            null, Collections.emptySet());
  }

  /**
   * Before the test, deletes all blobs in test container and uploads new ones.
   * @throws ReflectiveOperationException
   * @throws CloudStorageException
   * @throws IOException
   */
  @Before
  public void before() throws ReflectiveOperationException, CloudStorageException, IOException, StoreException {
    // Assume Azurite is up and running
    assumeTrue(azuriteUtils.connectToAzurite());
    // Create a test cloud client
    recoveryNetworkClient =
        new RecoveryNetworkClient(verifiableProperties, mockClusterMap.getMetricRegistry(),
            mockClusterMap, null, null, null);
    recoveryMetrics = recoveryNetworkClient.getRecoveryMetrics();
    // Mark test partition for compaction
    AccountService accountService = Mockito.mock(AccountService.class);
    Mockito.lenient().when(accountService.getContainersByStatus(any()))
        .thenReturn(Collections.singleton(testContainer));
    // Clear the partition
    AzureCloudDestinationSync azuriteClient = azuriteUtils.getAzuriteClient(
        properties, mockClusterMap.getMetricRegistry(),    null, accountService);
    azuriteClient.compactPartition(mockPartitionId.toPathString());
    // Add NUM_BLOBS of size BLOB_SIZE
    azureBlobs = new HashMap<>();
    IntStream.range(0, NUM_BLOBS).forEach(i -> {
      BlobId blobId = CloudTestUtil.getUniqueId(testContainer.getParentAccountId(), testContainer.getId(),
          false, mockPartitionId);
      // FIXME: We are not uploading CRC irl !!!
      azureBlobs.put(blobId.getID(), new MessageInfo(blobId,
          BLOB_SIZE, false, false, false, Utils.Infinite_Time, null,
          testContainer.getParentAccountId(), testContainer.getId(), System.currentTimeMillis(), (short) 0));
    });
    // Upload blobs
    List<MessageInfo> blobIds = new ArrayList<>(azureBlobs.values());
    InputStream blobData = new ByteBufferInputStream(ByteBuffer.wrap(
        TestUtils.getRandomBytes((int) (NUM_BLOBS * BLOB_SIZE))));
    MessageFormatWriteSet messageWriteSet = new MessageFormatWriteSet(
        new MessageSievingInputStream(blobData, blobIds, Collections.emptyList(), new MetricRegistry()),
        blobIds, false);
    azuriteClient.uploadBlobs(messageWriteSet);
    // Create local store
    localStore =
        new BlobStore(mockPartitionId.getReplicaIds().get(0), new StoreConfig(verifiableProperties),
            Utils.newScheduler(1, false),
            Utils.newScheduler(1, false),
            new DiskIOScheduler(null),
            StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
            new StoreMetrics(mockClusterMap.getMetricRegistry()),
            new StoreMetrics("UnderCompaction", mockClusterMap.getMetricRegistry()),
            null, null, null, Collections.singletonList(mock(ReplicaStatusDelegate.class)),
            new MockTime(), new InMemAccountService(false, false), null,
            Utils.newScheduler(1, false));
    localStore.start();
    // Create remote-replica info
    remoteReplicaInfo =
        new RemoteReplicaInfo(cloudReplica, mockPartitionId.getReplicaIds().get(0), localStore, new RecoveryToken(),
            Long.MAX_VALUE, SystemTime.getInstance(), new Port(cloudServiceDataNode.getPort(), PortType.PLAINTEXT));
    // Create ReplicaThread
    MockStoreKeyConverterFactory storeKeyConverterFactory =
        new MockStoreKeyConverterFactory(null, null)
            .setConversionMap(new HashMap<>())
            .setReturnInputIfAbsent(true);
    StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
    recoveryThread =
        new ReplicaThread("recovery-thread", findTokenHelper, mockClusterMap, new AtomicInteger(0),
            cloudServiceDataNode, recoveryNetworkClient, new ReplicationConfig(verifiableProperties),
            new ReplicationMetrics(mockClusterMap.getMetricRegistry(), Collections.emptyList()), null,
            storeKeyConverter, null,
            mockClusterMap.getMetricRegistry(), false, cloudServiceDataNode.getDatacenterName(),
            new ResponseHandler(mockClusterMap), new SystemTime(), null, null,
            null);
    // Add remote-replica-info to replica-thread
    recoveryThread.addRemoteReplicaInfo(remoteReplicaInfo);
  }

  @After
  public void after() throws StoreException, IOException {
    localStore.shutdown();
    String replica = mockPartitionId.getReplicaIds().get(0).getReplicaPath();
    logger.info("Deleting {}", replica);
    FileUtils.deleteDirectory(new File(replica));
  }

  /**
   * Creates a metadata request to send to Azure storage
   * @param recoveryToken
   * @return
   */
  protected List<RequestInfo> createMetadataRequest(RecoveryToken recoveryToken) {
    // requestInfoList -> requestInfo -> replicaMetadataRequest -> replicaMetadataRequestInfoList
    // -> replicaMetadataRequestInfo -> why !!!
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =  new ReplicaMetadataRequestInfo(mockPartitionId,
        recoveryToken, LOCALHOST, "/tmp", ReplicaType.CLOUD_BACKED,
        ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2);
    ReplicaMetadataRequest replicaMetadataRequest = new ReplicaMetadataRequest(requestId.incrementAndGet(),
        "repl-metadata-localhost", Collections.singletonList(replicaMetadataRequestInfo),
        100, ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2);
    RequestInfo requestInfo =
        new RequestInfo(LOCALHOST, new Port(0, PortType.PLAINTEXT), replicaMetadataRequest, cloudReplica,
            null, System.currentTimeMillis(), 0, 0);
    return Collections.singletonList(requestInfo);
  }

  /**
   * Tests that basic recovery of uploaded blobIDs works.
   * In each iteration, it sends a request to Azure Storage and retrieves M blobs where M < Total number of blobs.
   * Finally, checks that all blobIDs have been recovered.
   * @throws Exception
   */
  @Test
  public void testMetadataRecovery() throws Exception {
    RecoveryToken recoveryToken = new RecoveryToken();
    List<MessageInfo> messageInfoList = new ArrayList<>();
    int numPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    // Iterate N times to retrieve all blobIDs
    for (Integer ignored : IntStream.rangeClosed(1, numPages).boxed().collect(Collectors.toList())) {
      // Send metadata request
      List<ResponseInfo> responseInfoList =
          recoveryNetworkClient.sendAndPoll(createMetadataRequest(recoveryToken), Collections.emptySet(), 0);
      // Receive metadata response
      ReplicaMetadataResponseInfo rinfo = ReplicaMetadataResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfoList.get(0).content()), findTokenHelper, mockClusterMap)
          .getReplicaMetadataResponseInfoList().get(0);
      // Extract token
      recoveryToken = (RecoveryToken) rinfo.getFindToken();
      // Extract ambry metadata
      messageInfoList.addAll(rinfo.getMessageInfoList());
    }
    // Assert we recovered all blobIds intact
    assertEquals(numPages, recoveryMetrics.listBlobsSuccessRate.getCount());
    assertTrue(recoveryToken.isEndOfPartition());
    assertEquals(azureBlobs.size(), messageInfoList.size());
    messageInfoList.forEach(messageInfo -> {
      MessageInfo OgMessageInfo = azureBlobs.get(messageInfo.getStoreKey().getID());
      assertEquals(String.format("Expected metadata = %s, Received metadata = %s", OgMessageInfo, messageInfo),
          OgMessageInfo, (messageInfo));
    });
  }

  /**
   * Tests that all blobs are listed in spite of intermittent errors.
   * @throws Exception
   */
  @Test
  public void testMetadataRecoveryFailure() throws Exception {
    final int ERROR_FREQUENCY = 5; // Every n-th page is bad
    class ExceptionCallback implements RecoveryNetworkClientCallback {
      int numCalls = 0;
      public void onListBlobs(ReplicaMetadataRequestInfo request) {
        numCalls += 1;
        // the frequency of err_pages reduces over all iter
        // if there are 34 pages and every 5-th page is bad, then we have 6 bad pages. We need 6 extra calls.
        // if there are 40 calls, then every 6-th call is bad. 34/5 == 40/6;
        if (numCalls % (ERROR_FREQUENCY + 1) == 0) {
          throw new RuntimeException("Exception on Azure Storage list-blobs call");
        }
      }
    }
    recoveryNetworkClient =
        new RecoveryNetworkClient(new VerifiableProperties(properties),
            new MetricRegistry(),    mockClusterMap, null, null, new ExceptionCallback());
    recoveryMetrics = recoveryNetworkClient.getRecoveryMetrics();
    RecoveryToken recoveryToken = new RecoveryToken();
    List<MessageInfo> messageInfoList = new ArrayList<>();
    int numGoodPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    int numBadPages = numGoodPages/ERROR_FREQUENCY;
    int numIter  = numGoodPages + numBadPages;
    // Iterate N times to retrieve all blobIDs
    for (Integer ignored : IntStream.rangeClosed(1, numIter).boxed().collect(Collectors.toList())) {
      // Send metadata request
      List<ResponseInfo> responseInfoList =
          recoveryNetworkClient.sendAndPoll(createMetadataRequest(recoveryToken),
              Collections.emptySet(), 0);
      // Receive metadata response
      ReplicaMetadataResponseInfo rinfo = ReplicaMetadataResponse.readFrom(
              new NettyByteBufDataInputStream(responseInfoList.get(0).content()), findTokenHelper, mockClusterMap)
          .getReplicaMetadataResponseInfoList().get(0);
      if (rinfo.getError() == ServerErrorCode.No_Error) {
        // Extract token
        recoveryToken = (RecoveryToken) rinfo.getFindToken();
        // Extract ambry metadata
        messageInfoList.addAll(rinfo.getMessageInfoList());
      } else {
        assertEquals(ServerErrorCode.IO_Error, rinfo.getError());
      }
    }
    // Assert we recovered all blobIds intact
    assertEquals(numBadPages, recoveryMetrics.listBlobsError.getCount());
    assertTrue(recoveryToken.isEndOfPartition());
    assertEquals(azureBlobs.size(), messageInfoList.size());
    messageInfoList.forEach(messageInfo -> {
      MessageInfo OgMessageInfo = azureBlobs.get(messageInfo.getStoreKey().getID());
      assertEquals(String.format("Expected metadata = %s, Received metadata = %s", OgMessageInfo, messageInfo),
          OgMessageInfo, (messageInfo));
    });
  }

  /**
   * Tests that serialization of RecoveryToken works
   * @throws IOException
   */
  @Test
  public void testRecoveryTokenSerDe() throws IOException {
    RecoveryToken recoveryToken = new RecoveryToken();
    RecoveryToken newRecoveryToken = RecoveryToken.fromBytes(
        new ByteBufferDataInputStream(ByteBuffer.wrap(recoveryToken.toBytes())));
    assertTrue(recoveryToken.equals(newRecoveryToken));
  }

  @Test
  public void testBackupRecovery() throws StoreException {
    int numPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    // Iterate N times to retrieve all blobIDs
    for (Integer ignored : IntStream.rangeClosed(1, numPages).boxed().collect(Collectors.toList())) {
      recoveryThread.replicate();
    }
    StoreInfo localBlobs = localStore.get(
        azureBlobs.values().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.noneOf(StoreGetOptions.class));
    // Assert we recovered all blobIds intact
    assertTrue(((RecoveryToken) remoteReplicaInfo.getToken()).isEndOfPartition());
    assertEquals(azureBlobs.size(), localBlobs.getMessageReadSetInfo().size());
    localBlobs.getMessageReadSetInfo().forEach(messageInfo -> {
      MessageInfo OgMessageInfo = azureBlobs.get(messageInfo.getStoreKey().getID());
      assertEquals(String.format("Expected metadata = %s, Received metadata = %s", OgMessageInfo, messageInfo),
          OgMessageInfo, (messageInfo));
    });
  }
}
