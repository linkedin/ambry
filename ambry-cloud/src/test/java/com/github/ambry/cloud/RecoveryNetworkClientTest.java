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
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.CloudServiceDataNode;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ByteBufferDataInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
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
  private AzuriteUtils azuriteUtils;
  private MockClusterMap mockClusterMap;
  private RecoveryNetworkClient recoveryNetworkClient;
  private MockPartitionId mockPartitionId;
  private final short ACCOUNT_ID = 1024, CONTAINER_ID = 2048;
  private final int NUM_BLOBS = 100;
  private final long BLOB_SIZE = 1000;
  private Container testContainer;

  private AtomicInteger counter;

  public RecoveryNetworkClientTest() throws IOException {
    counter = new AtomicInteger(0);
    azuriteUtils = new AzuriteUtils();
    // Azure container names have to be 3 char long at least
    mockPartitionId = new MockPartitionId(123450L, MockClusterMap.DEFAULT_PARTITION_CLASS);
    mockClusterMap = new MockClusterMap(false, true, 1,
        1, 1, true, false,
        "localhost");
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
        new RecoveryNetworkClient(new VerifiableProperties(azuriteUtils.getAzuriteConnectionProperties()),
            new MetricRegistry(),    mockClusterMap, null, null);
  }

  @Before
  public void before() throws ReflectiveOperationException, CloudStorageException, IOException {
    // Clear Azure partitions and add some blobs
    AccountService accountService = Mockito.mock(AccountService.class);
    Mockito.lenient().when(accountService.getContainersByStatus(any()))
        .thenReturn(Collections.singleton(testContainer));
    Properties properties = azuriteUtils.getAzuriteConnectionProperties();
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(false));
    // Azurite client
    AzureCloudDestinationSync azuriteClient = azuriteUtils.getAzuriteClient(
        properties, new MetricRegistry(),    null, accountService);
    // For each partition, clear it and add NUM_BLOBS of size BLOB_SIZE
    List<MessageInfo> messageInfoList = new ArrayList<>();
    azuriteClient.compactPartition(mockPartitionId.toPathString());
    IntStream.range(0, NUM_BLOBS).forEach(i -> messageInfoList.add(
        new MessageInfo(CloudTestUtil.getUniqueId(testContainer.getParentAccountId(), testContainer.getId(),
            false, mockPartitionId),
            BLOB_SIZE, false, false, false, Utils.Infinite_Time, new Random().nextLong(),
            testContainer.getParentAccountId(), testContainer.getId(), System.currentTimeMillis(), (short) 0)));
    // Upload blobs
    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(
        TestUtils.getRandomBytes((int) (NUM_BLOBS * BLOB_SIZE))));
    MessageFormatWriteSet messageWriteSet = new MessageFormatWriteSet(
        new MessageSievingInputStream(inputStream, messageInfoList, Collections.emptyList(), new MetricRegistry()),
        messageInfoList, false);
    azuriteClient.uploadBlobs(messageWriteSet);
  }

  public List<RequestInfo> getRequestInfoList() throws Exception {
    ClusterMapConfig clusterMapConfig =  new ClusterMapConfig(
        new VerifiableProperties(azuriteUtils.getAzuriteConnectionProperties()));
    CloudServiceDataNode cloudServiceDataNode = new CloudServiceDataNode("localhost", clusterMapConfig);
    CloudReplica cloudReplica = new CloudReplica(mockPartitionId, cloudServiceDataNode);
    // requestInfoList -> requestInfo -> replicaMetadataRequest -> replicaMetadataRequestInfoList
    // -> replicaMetadataRequestInfo
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =  new ReplicaMetadataRequestInfo(mockPartitionId,
        new RecoveryToken(), "localhost", "/tmp", ReplicaType.CLOUD_BACKED, (short) 1);
    ReplicaMetadataRequest replicaMetadataRequest = new ReplicaMetadataRequest(counter.incrementAndGet(),
        "repl-metadata-localhost", Collections.singletonList(replicaMetadataRequestInfo),
        100, (short) 1);
    RequestInfo requestInfo =
        new RequestInfo("localhost", new Port(0, PortType.PLAINTEXT), replicaMetadataRequest, cloudReplica,
            null, System.currentTimeMillis(), 0, 0);
    return Collections.singletonList(requestInfo);
  }
  @After
  public void after() {

  }

  public void basic() throws Exception {
    List<ResponseInfo> responseInfoList =
        recoveryNetworkClient.sendAndPoll(getRequestInfoList(), Collections.emptySet(), 0);
    ResponseInfo responseInfo = responseInfoList.get(0);
    responseInfo.content();
    // ReplicaMetadataResponse response = ReplicaMetadataResponse.readFrom(responseInfo.content(), findTokenHelper, mockClusterMap);
  }

  @Test
  public void testRecoveryTokenSerDe() throws IOException {
    RecoveryToken recoveryToken = new RecoveryToken();
    RecoveryToken newRecoveryToken = RecoveryToken.fromBytes(
        new ByteBufferDataInputStream(ByteBuffer.wrap(recoveryToken.toBytes())));
    assertTrue(recoveryToken.equals(newRecoveryToken));
  }
}
