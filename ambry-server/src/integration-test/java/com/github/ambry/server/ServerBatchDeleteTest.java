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
package com.github.ambry.server;

import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.BatchDeletePartitionRequestInfo;
import com.github.ambry.protocol.BatchDeleteRequest;
import com.github.ambry.protocol.BatchDeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ServerBatchDeleteTest {
  private MockNotificationSystem notificationSystem;
  private MockTime time;
  private AmbryServer server;
  private MockClusterAgentsFactory mockClusterAgentsFactory;
  private MockClusterMap mockClusterMap;
  private ArrayList<BlobProperties> properties;
  private ArrayList<byte[]> encryptionKey;
  private ArrayList<byte[]> usermetadata;
  private ArrayList<byte[]> data;
  private ArrayList<BlobId> blobIdList1;
  private ArrayList<BlobId> blobIdList2;

  private List<PartitionId> partitionIds;

  private ConnectedChannel channel;

  @Before
  public void initialize() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, true, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    notificationSystem = new MockNotificationSystem(mockClusterMap);
    partitionIds = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    channel = ServerTestUtil.getBlockingChannelBasedOnPortType(new Port(mockClusterMap.getDataNodeIds().get(0).getPort(),
                PortType.PLAINTEXT), "localhost", null, null);
    time = new MockTime(SystemTime.getInstance().milliseconds());
    Properties props = new Properties();
    props.setProperty("host.name", mockClusterMap.getDataNodes().get(0).getHostname());
    props.setProperty("port", Integer.toString(mockClusterMap.getDataNodes().get(0).getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.deleted.message.retention.hours", "10080");
    props.setProperty("server.handle.undelete.request.enabled", "true");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    TestSSLUtils.addHttp2Properties(props, SSLFactory.Mode.SERVER, true);
    VerifiableProperties propverify = new VerifiableProperties(props);
    server = new AmbryServer(propverify, mockClusterAgentsFactory, notificationSystem, time);
    server.startup();
  }

  @After
  public void cleanup() throws IOException {
    if (server != null) {
      server.shutdown();
    }
    if (mockClusterMap != null) {
      mockClusterMap.cleanup();
    }
  }

  /**
   * Uploads a single blob to ambry server node
   * @param blobId the {@link BlobId} that needs to be put
   * @param properties the {@link BlobProperties} of the blob being uploaded
   * @param usermetadata the user metadata of the blob being uploaded
   * @param data the blob content of the blob being uploaded
   * @param channel the {@link ConnectedChannel} to use to send and receive data
   * @throws IOException
   */
  void putBlob(BlobId blobId, BlobProperties properties, byte[] encryptionKey, byte[] usermetadata, byte[] data,
      ConnectedChannel channel) throws IOException {
    PutRequest putRequest0 =
        new PutRequest(1, "client1", blobId, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey == null ? null : ByteBuffer.wrap(encryptionKey));
    PutResponse response0 = PutResponse.readFrom(channel.sendAndReceive(putRequest0).getInputStream());
    Assert.assertEquals(ServerErrorCode.No_Error, response0.getError());
  }


  /**
   * Fetches a single blob from ambry server node
   * @param blobId the {@link BlobId} that needs to be fetched
   * @param channel the {@link ConnectedChannel} to use to send and receive data
   * @throws IOException
   */
  GetResponse getBlob(BlobId blobId, ConnectedChannel channel) throws IOException {
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    partitionRequestInfoList.add(new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId)));
    GetRequest getRequest = new GetRequest(1, "client1", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
    return GetResponse.readFrom(channel.sendAndReceive(getRequest).getInputStream(), mockClusterMap);
  }

  /**
   * Deletes a list of blobs from ambry server node
   * @param batchDeletePartitionRequestInfos the {@link List<BatchDeletePartitionRequestInfo>} that needs to be deleted
   * @param channel the {@link ConnectedChannel} to use to send and receive data
   * @throws IOException
   */
  BatchDeleteResponse deleteBlobs(List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfos, ConnectedChannel channel) throws IOException {
    BatchDeleteRequest batchDeleteRequest = new BatchDeleteRequest(1, "client1", batchDeletePartitionRequestInfos, time.milliseconds());
    return BatchDeleteResponse.readFrom(channel.sendAndReceive(batchDeleteRequest).getInputStream(), mockClusterMap);
  }

  List<BatchDeletePartitionRequestInfo> getBatchDeletePartitionRequestInfoList(){
    List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfoList = new ArrayList<>();
    batchDeletePartitionRequestInfoList.add(new BatchDeletePartitionRequestInfo(partitionIds.get(0), blobIdList1));
    batchDeletePartitionRequestInfoList.add(new BatchDeletePartitionRequestInfo(partitionIds.get(1), blobIdList2));
    return batchDeletePartitionRequestInfoList;
  }

  private void prepareDataForTestingBatchDelete(){
    encryptionKey = new ArrayList<>(5);
    usermetadata = new ArrayList<>(5);
    data = new ArrayList<>(5);
    Random random = new Random();
    for (int i = 0; i < 5; i++) {
      if (i % 2 == 0) {
        encryptionKey.add(new byte[100]);
        random.nextBytes(encryptionKey.get(i));
      } else {
        encryptionKey.add(null);
      }
      usermetadata.add(new byte[1000 + i]);
      data.add(new byte[31870 + i]);
      random.nextBytes(usermetadata.get(i));
      random.nextBytes(data.get(i));
    }

    properties = new ArrayList<>(5);
    properties.add(new BlobProperties(31870, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(new BlobProperties(31871, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), false));
    properties.add(new BlobProperties(31872, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));
    properties.add(
        new BlobProperties(31873, "serviceid1", "ownerid", "jpeg", false, 0, Utils.getRandomShort(TestUtils.RANDOM),
            Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null));
    properties.add(new BlobProperties(31874, "serviceid1", Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), true));

    blobIdList1 = new ArrayList<>(3);
    blobIdList2 = new ArrayList<>(2);
    for (int i = 0; i < 5; i++) {
      if (i%2 == 0) {
        blobIdList1.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
            mockClusterMap.getLocalDatacenterId(), properties.get(i).getAccountId(), properties.get(i).getContainerId(),
            partitionIds.get(0), false, BlobId.BlobDataType.DATACHUNK));
      }
      else{
        blobIdList2.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
            mockClusterMap.getLocalDatacenterId(), properties.get(i).getAccountId(), properties.get(i).getContainerId(),
            partitionIds.get(1), false, BlobId.BlobDataType.DATACHUNK));
      }
    }
  }

  /**
   * Positive test for batch delete
   * <p>
   * This test does the following:
   * 1. Makes 5 puts, across 2 partitions, waits for notification.
   * 2. Creates 1 batch delete request
   * 3. Verifies the BatchDeleteResponse
   *
   * @throws Exception
   */
  @Test
  public void testBatchDeleteSuccess() throws Exception {
    prepareDataForTestingBatchDelete();
    channel.connect();
    for (int i = 0; i < 3; i++) {
      putBlob(blobIdList1.get(i), properties.get(i), encryptionKey.get(i), usermetadata.get(i), data.get(i), channel);
    }
    for (int i = 0; i < 2; i++) {
      putBlob(blobIdList2.get(i), properties.get(i), encryptionKey.get(i), usermetadata.get(i), data.get(i), channel);
    }
    List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfoList = getBatchDeletePartitionRequestInfoList();
    BatchDeleteResponse batchDeleteResponse = deleteBlobs(batchDeletePartitionRequestInfoList, channel);
    Assert.assertEquals(ServerErrorCode.No_Error, batchDeleteResponse.getError());

    // check if blobs are deleted by doing get operation on those blobs
    for (int i = 0; i < 3; i++) {
      GetResponse getResponse = getBlob(blobIdList1.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Deleted, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    for (int i = 0; i < 2; i++) {
      GetResponse getResponse = getBlob(blobIdList2.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Deleted, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    channel.disconnect();
  }

  /**
   * Partial Failure test for batch delete
   * <p>
   * This test does the following:
   * 1. Makes 3 puts for 1 partition, waits for notification.
   * 2. Creates 1 batch delete request for 5 blob deletes (across 2 partitions, one of which should error out)
   * 3. Verifies the BatchDeleteResponse
   *
   * @throws Exception
   */
  @Test
  public void testBatchDeletePartialFailure() throws Exception {
    prepareDataForTestingBatchDelete();
    channel.connect();
    for (int i = 0; i < 3; i++) {
      putBlob(blobIdList1.get(i), properties.get(i), encryptionKey.get(i), usermetadata.get(i), data.get(i), channel);
    }
    List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfoList = getBatchDeletePartitionRequestInfoList();
    BatchDeleteResponse batchDeleteResponse = deleteBlobs(batchDeletePartitionRequestInfoList, channel);
    Assert.assertEquals(ServerErrorCode.Unknown_Error, batchDeleteResponse.getError());

    // check if blobs are deleted by doing get operation on those blobs
    for (int i = 0; i < 3; i++) {
      GetResponse getResponse = getBlob(blobIdList1.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Deleted, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    for (int i = 0; i < 2; i++) {
      GetResponse getResponse = getBlob(blobIdList2.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Not_Found, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    channel.disconnect();
  }

  /**
   * Complete Failure test for batch delete
   * <p>
   * This test does the following:
   * 1. Makes 0 puts.
   * 2. Creates 1 batch delete request for 5 blob deletes.
   * 3. Verifies the BatchDeleteResponse
   *
   * @throws Exception
   */
  @Test
  public void testBatchDeleteCompleteFailure() throws Exception {
    prepareDataForTestingBatchDelete();
    channel.connect();
    List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfoList = getBatchDeletePartitionRequestInfoList();
    BatchDeleteResponse batchDeleteResponse = deleteBlobs(batchDeletePartitionRequestInfoList, channel);
    Assert.assertEquals(ServerErrorCode.Unknown_Error, batchDeleteResponse.getError());
    // check if blobs are deleted by doing get operation on those blobs
    for (int i = 0; i < 3; i++) {
      GetResponse getResponse = getBlob(blobIdList1.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Not_Found, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    for (int i = 0; i < 2; i++) {
      GetResponse getResponse = getBlob(blobIdList2.get(i), channel);
      Assert.assertEquals(ServerErrorCode.Blob_Not_Found, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
    }
    channel.disconnect();
  }
}
