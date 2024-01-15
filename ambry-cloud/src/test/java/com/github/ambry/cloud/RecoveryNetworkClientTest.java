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
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.CloudServiceDataNode;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
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
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ByteBufferDataInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;


public class RecoveryNetworkClientTest {

  private static Logger logger = LoggerFactory.getLogger(RecoveryNetworkClientTest.class);
  public static final String LOCALHOST = "localhost";
  protected Properties properties;
  protected FindTokenHelper findTokenHelper;
  protected AzuriteUtils azuriteUtils;
  protected MockClusterMap mockClusterMap;
  protected RecoveryNetworkClient recoveryNetworkClient;
  protected MockPartitionId mockPartitionId;
  protected final short ACCOUNT_ID = 1024, CONTAINER_ID = 2048;
  protected final int NUM_BLOBS = 100;
  protected final long BLOB_SIZE = 1000;
  protected final int AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE = 3;
  protected Container testContainer;
  protected AtomicInteger counter;
  Map<String, MessageInfo> messageInfoMap;

  public RecoveryNetworkClientTest() throws IOException, ReflectiveOperationException {
    counter = new AtomicInteger(0);
    azuriteUtils = new AzuriteUtils();
    properties = azuriteUtils.getAzuriteConnectionProperties();
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(false));
    properties.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, "1");
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "Partition");
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE,
        String.valueOf(AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE));
    properties.setProperty(ReplicationConfig.REPLICATION_CLOUD_TOKEN_FACTORY,
        RecoveryTokenFactory.class.getCanonicalName());
    findTokenHelper = new FindTokenHelper(null,
        new ReplicationConfig(new VerifiableProperties(properties)));
    // Azure container names have to be 3 char long at least
    mockClusterMap = new MockClusterMap(false, true, 1,
        1, 1, true, false,
        "localhost");
    mockPartitionId =
        (MockPartitionId) mockClusterMap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    testContainer =
        new Container(CONTAINER_ID, "testContainer", Container.ContainerStatus.ACTIVE,
            "Test Container", false,
            false, false,
            false, null, false,
            false, Collections.emptySet(),
            false, false, Container.NamedBlobMode.DISABLED, ACCOUNT_ID, 0,
            0, 0, "",
            null, Collections.emptySet());
    recoveryNetworkClient =
        new RecoveryNetworkClient(new VerifiableProperties(properties),
            new MetricRegistry(),    mockClusterMap, null, null);
  }

  /**
   * Before the test, deletes all blobs in test container and uploads new ones.
   * @throws ReflectiveOperationException
   * @throws CloudStorageException
   * @throws IOException
   */
  @Before
  public void before() throws ReflectiveOperationException, CloudStorageException, IOException {
    // Clear Azure partitions and add some blobs
    AccountService accountService = Mockito.mock(AccountService.class);
    Mockito.lenient().when(accountService.getContainersByStatus(any()))
        .thenReturn(Collections.singleton(testContainer));
    // Azurite client
    AzureCloudDestinationSync azuriteClient = azuriteUtils.getAzuriteClient(
        properties, new MetricRegistry(),    null, accountService);
    // For each partition, clear it and add NUM_BLOBS of size BLOB_SIZE
    messageInfoMap = new HashMap<>();
    azuriteClient.compactPartition(mockPartitionId.toPathString());
    IntStream.range(0, NUM_BLOBS).forEach(i -> {
      BlobId blobId = CloudTestUtil.getUniqueId(testContainer.getParentAccountId(), testContainer.getId(),
          false, mockPartitionId);
      // FIXME: We are not uploading CRC irl !!!
      messageInfoMap.put(blobId.getID(), new MessageInfo(blobId,
          BLOB_SIZE, false, false, false, Utils.Infinite_Time, null,
          testContainer.getParentAccountId(), testContainer.getId(), System.currentTimeMillis(), (short) 0));
    });
    // Upload blobs
    List<MessageInfo> blobIds = new ArrayList<>(messageInfoMap.values());
    InputStream blobData = new ByteBufferInputStream(ByteBuffer.wrap(
        TestUtils.getRandomBytes((int) (NUM_BLOBS * BLOB_SIZE))));
    MessageFormatWriteSet messageWriteSet = new MessageFormatWriteSet(
        new MessageSievingInputStream(blobData, blobIds, Collections.emptyList(), new MetricRegistry()),
        blobIds, false);
    azuriteClient.uploadBlobs(messageWriteSet);
  }

  @After
  public void after() {

  }

  /**
   * Creates a metadata request to send to Azure storage
   * @param recoveryToken
   * @return
   * @throws Exception
   */
  public List<RequestInfo> getRequestInfoList(RecoveryToken recoveryToken) throws Exception {
    ClusterMapConfig clusterMapConfig =  new ClusterMapConfig(new VerifiableProperties(properties));
    CloudServiceDataNode cloudServiceDataNode = new CloudServiceDataNode(LOCALHOST, clusterMapConfig);
    CloudReplica cloudReplica = new CloudReplica(mockPartitionId, cloudServiceDataNode);
    // requestInfoList -> requestInfo -> replicaMetadataRequest -> replicaMetadataRequestInfoList
    // -> replicaMetadataRequestInfo -> why !!!
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =  new ReplicaMetadataRequestInfo(mockPartitionId,
        recoveryToken, LOCALHOST, "/tmp", ReplicaType.CLOUD_BACKED,
        ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2);
    ReplicaMetadataRequest replicaMetadataRequest = new ReplicaMetadataRequest(counter.incrementAndGet(),
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
  public void testBasicRecoveryOfBlobMetadata() throws Exception {
    RecoveryToken recoveryToken = new RecoveryToken();
    List<MessageInfo> messageInfoList = new ArrayList<>();
    int numIter = (NUM_BLOBS + AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE)/AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE;
    // Iterate N times to retrieve all blobIDs
    for (Integer ignored : IntStream.rangeClosed(1, numIter).boxed().collect(Collectors.toList())) {
      // Send metadata request
      List<ResponseInfo> responseInfoList =
          recoveryNetworkClient.sendAndPoll(getRequestInfoList(recoveryToken), Collections.emptySet(), 0);
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
    assertEquals(null, recoveryToken.getToken());
    assertEquals(messageInfoMap.size(), messageInfoList.size());
    for (MessageInfo messageInfo : messageInfoList) {
      MessageInfo OgMessageInfo = messageInfoMap.get(messageInfo.getStoreKey().getID());
      if (!OgMessageInfo.equals(messageInfo)) {
        String err = String.format("Expected metadata = %s, Received metadata = %s", OgMessageInfo, messageInfo);
        fail(err);
      }
    }
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
}
