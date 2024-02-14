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
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
  protected RemoteReplicaInfo remoteStore;
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
  protected String BLOB_DATA = "Use the Force, Luke!"; // Use string so we can print it out for comparison
  protected final long BLOB_SIZE = BLOB_DATA.length();
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

  ////////////////////////////////////////////// HELPERS ///////////////////////////////////////////////////////////

  /**
   * Before the test:
   * 1. Deletes all blobs in test container in Azurite and uploads new ones,
   * 2. Creates a local-store to download blobs from Azurite
   * @throws ReflectiveOperationException
   * @throws CloudStorageException
   * @throws IOException
   */
  @Before
  public void before() throws ReflectiveOperationException, CloudStorageException, IOException, StoreException {
    // Assume Azurite is up and running
    assumeTrue(azuriteUtils.connectToAzurite());
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
    remoteStore =
        new RemoteReplicaInfo(cloudReplica, mockPartitionId.getReplicaIds().get(0), localStore, new RecoveryToken(),
            Long.MAX_VALUE, SystemTime.getInstance(), new Port(cloudServiceDataNode.getPort(), PortType.PLAINTEXT));
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
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IntStream.range(0, NUM_BLOBS).forEach(i -> {
      BlobId blobId = CloudTestUtil.getUniqueId(testContainer.getParentAccountId(), testContainer.getId(),
          false, mockPartitionId);
      // FIXME: We are not uploading CRC irl !!!
      azureBlobs.put(blobId.getID(), new MessageInfo(blobId,
          BLOB_SIZE, false, false, false, Utils.Infinite_Time, null,
          testContainer.getParentAccountId(), testContainer.getId(),
          Utils.getTimeInMsToTheNearestSec(System.currentTimeMillis()), (short) 0));
      try {
        outputStream.write(BLOB_DATA.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    // Upload blobs
    List<MessageInfo> blobIds = new ArrayList<>(azureBlobs.values());
    InputStream blobData = new ByteBufferInputStream(ByteBuffer.wrap(outputStream.toByteArray()));
    MessageFormatWriteSet messageWriteSet = new MessageFormatWriteSet(
        new MessageSievingInputStream(blobData, blobIds, Collections.emptyList(), new MetricRegistry()),
        blobIds, false);
    azuriteClient.uploadBlobs(messageWriteSet);
  }

  /**
   * Deletes local-store after test
   * @throws StoreException
   * @throws IOException
   */
  @After
  public void after() throws StoreException, IOException {
    localStore.shutdown();
    String replica = mockPartitionId.getReplicaIds().get(0).getReplicaPath();
    logger.info("Deleting {}", replica);
    FileUtils.deleteDirectory(new File(replica));
  }

  /**
   * Returns a recovery client that connects to Azurite
   * @param callback
   * @return
   */
  protected RecoveryNetworkClient createRecoveryClient(RecoveryNetworkClientCallback callback) {
    // Create a test cloud client
    return new RecoveryNetworkClient(verifiableProperties, mockClusterMap.getMetricRegistry(),
        mockClusterMap, null, null, callback);
  }

  /**
   * Returns a replica thread that replicates from Azurite to local-store
   * @param recoveryNetworkClient
   * @return
   */
  protected ReplicaThread createRecoveryThread(RecoveryNetworkClient recoveryNetworkClient) {
    // Create ReplicaThread
    MockStoreKeyConverterFactory storeKeyConverterFactory =
        new MockStoreKeyConverterFactory(null, null)
            .setConversionMap(new HashMap<>())
            .setReturnInputIfAbsent(true);
    StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
    RecoveryManager recoveryManager = mock(RecoveryManager.class);
    when(recoveryManager.getRecoveryTokenFilename(any())).thenCallRealMethod();
    ReplicaThread recoveryThread =
        new RecoveryThread("recovery-thread", findTokenHelper, mockClusterMap, new AtomicInteger(0),
            cloudServiceDataNode, recoveryNetworkClient, new ReplicationConfig(verifiableProperties),
            new ReplicationMetrics(mockClusterMap.getMetricRegistry(), Collections.emptyList()), null,
            storeKeyConverter, null,
            mockClusterMap.getMetricRegistry(), false, cloudServiceDataNode.getDatacenterName(),
            new ResponseHandler(mockClusterMap), new SystemTime(), null, null,
            null, recoveryManager);
    // Add remote-replica-info to replica-thread
    recoveryThread.addRemoteReplicaInfo(remoteStore);
    return recoveryThread;
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
   * Downloads all metadata from Azurite in pages by iterating given number of times
   * @param callback
   * @param numPages
   * @return
   * @throws Exception
   */
  protected List<MessageInfo> fetchPaginatedMetadata(RecoveryNetworkClientCallback callback, int numPages)
      throws Exception {
    RecoveryToken recoveryToken = new RecoveryToken();
    List<MessageInfo> metadataList = new ArrayList<>();
    RecoveryNetworkClient recoveryNetworkClient = createRecoveryClient(callback);
    // Iterate N times to retrieve all blobIDs
    for (Integer ignored : IntStream.rangeClosed(1, numPages).boxed().collect(Collectors.toList())) {
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
        metadataList.addAll(rinfo.getMessageInfoList());
      }
    }
    assertTrue(recoveryToken.isEndOfPartition());
    assertEquals(azureBlobs.size(), metadataList.size());
    return metadataList;
  }

  ////////////////////////////////////////////// TESTS ///////////////////////////////////////////////////////////

  /**
   * Tests that all blob metadata is downloaded without any error.
   * @throws Exception
   */
  @Test
  public void testMetadataRecoveryClient() throws Exception {
    int numPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    List<MessageInfo> metadataList = fetchPaginatedMetadata(null, numPages);
    // Assert we recovered all blobIds intact
    metadataList.forEach(messageInfo -> {
      assertEquals(String.format("Azure blob metadata does not match that in local disk"),
          azureBlobs.get(messageInfo.getStoreKey().getID()), messageInfo);
    });
  }

  /**
   * Tests that all blob metadata is downloaded in spite of errors.
   * @throws Exception
   */
  @Test
  public void testMetadataRecoveryClientFailure() throws Exception {
    final int NUM_ERRORS = 5; // Every n-th page is bad
    class ExceptionCallback implements RecoveryNetworkClientCallback {
      int numCalls = 0;
      public void onListBlobs(ReplicaMetadataRequestInfo request) {
        numCalls += 1;
        /**
         * If we want a constant number of errors, then the frequency needs to adjusted based on the num of iterations.
         * In this test, we have 100 blobs fetched as 34 pages with 3 blobs per page at max.
         * If every 5-th page throws a retriable error, then we need 40 iterations to fetch all blobs.
         *          * 34 + (34/5) = 40
         * Now, among those 40 iterations, we want 5 erroneous pages. And that is 40/6, hence +1 below.
         * In other words, for a constant number of errors, the frequency with which they occur changes depending on
         * the number of calls or iterations.
         */
        if (numCalls % (NUM_ERRORS + 1) == 0) {
          throw new RuntimeException("Exception on Azure Storage list-blobs call");
        }
      }
    }
    int numGoodPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    int numBadPages = numGoodPages/NUM_ERRORS;
    List<MessageInfo> metadataList = fetchPaginatedMetadata(new ExceptionCallback(),
        numGoodPages + numBadPages);
    // Assert we recovered all blobIds intact
    metadataList.forEach(messageInfo -> {
      assertEquals(String.format("Azure blob metadata does not match that in local disk"),
          azureBlobs.get(messageInfo.getStoreKey().getID()), messageInfo);
    });
  }

  /**
   * Tests serialization of RecoveryToken
   * @throws IOException
   */
  @Test
  public void testRecoveryTokenSerDe() throws IOException {
    RecoveryToken recoveryToken = new RecoveryToken();
    RecoveryToken newRecoveryToken = RecoveryToken.fromBytes(
        new ByteBufferDataInputStream(ByteBuffer.wrap(recoveryToken.toBytes())));
    assertTrue(recoveryToken.equals(newRecoveryToken));
  }

  /**
   * Tests that replication code downloads all blobs from Azurite
   * @throws StoreException
   */
  @Test
  public void testRecoveryReplication() throws StoreException {
    int numPages = (NUM_BLOBS/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE) +
        (NUM_BLOBS % AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE == 0 ? 0 : 1);
    ReplicaThread recoveryThread = createRecoveryThread(createRecoveryClient(null));
    // Iterate N times to retrieve all blobIDs from cloud and store to disk
    IntStream.rangeClosed(1, numPages).forEach(i -> recoveryThread.replicate());
    // Get local blobs
    StoreInfo localBlobs = localStore.get(
        azureBlobs.values().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.noneOf(StoreGetOptions.class));
    // Assert we recovered all blobIds intact
    assertTrue(((RecoveryToken) remoteStore.getToken()).isEndOfPartition());
    assertEquals(azureBlobs.size(), localBlobs.getMessageReadSetInfo().size());
    IntStream.range(0, localBlobs.getMessageReadSetInfo().size()).forEach(i -> {
      // Match metadata
      MessageInfo localInfo = localBlobs.getMessageReadSetInfo().get(i);
      String blobIdStr = localInfo.getStoreKey().getID();
      if (!azureBlobs.get(blobIdStr).equals(localInfo)) {
        logger.error("Azure blob metadata for blob-%s %s does not match that in local disk, expected = {}, found = {}",
            i, blobIdStr, azureBlobs.get(blobIdStr), localInfo);
        fail("Metadata mismatch");
      }
      // Match data
      try {
        localBlobs.getMessageReadSet().doPrefetch(i, 0, BLOB_SIZE);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ByteBuffer localData = ByteBuffer.wrap(new byte[(int) BLOB_SIZE]);
      localBlobs.getMessageReadSet().getPrefetchedData(i).readBytes(localData);
      if (!Arrays.equals(BLOB_DATA.getBytes(), localData.array())) {
        logger.error("Azure blob data for blob-%s %s does not match that in local disk, expected = {}, found = {}",
            i, blobIdStr, BLOB_DATA, new String(localData.array()));
        fail("Data mismatch");
      }
    });
  }
}
