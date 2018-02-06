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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLBlockingChannel;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.router.Callback;
import com.github.ambry.router.CopyingAsyncWritableChannel;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Assert;

import static org.junit.Assert.*;


public final class ServerTestUtil {

  protected static void endToEndTest(Port targetPort, String routerDatacenter, String sslEnabledDatacenters,
      MockCluster cluster, SSLConfig clientSSLConfig, SSLSocketFactory clientSSLSocketFactory, Properties routerProps,
      boolean testEncryption) throws InterruptedException, IOException, InstantiationException {
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);

      BlobProperties properties = new BlobProperties(31870, "serviceid1", accountId, containerId, false);
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(data);
      if (testEncryption) {
        TestUtils.RANDOM.nextBytes(encryptionKey);
      }
      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false);
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false);
      BlobId blobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false);
      BlobId blobId4 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false);
      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      BlockingChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());

      // put blob 2
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest2);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest3);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 4 that is expired
      BlobProperties propertiesExpired =
          new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0, accountId, containerId, false);
      PutRequest putRequest4 =
          new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest4);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response4.getError());

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob properties with expired flag set
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<>();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
          GetOption.Include_Expired_Blobs);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob properties for expired blob
      // 1. With no flag
      ArrayList<BlobId> idsExpired = new ArrayList<>();
      MockPartitionId partitionExpired = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      idsExpired.add(blobId4);
      ArrayList<PartitionRequestInfo> partitionRequestInfoListExpired = new ArrayList<>();
      PartitionRequestInfo partitionRequestInfoExpired = new PartitionRequestInfo(partitionExpired, idsExpired);
      partitionRequestInfoListExpired.add(partitionRequestInfoExpired);
      GetRequest getRequestExpired =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoListExpired,
              GetOption.None);
      channel.send(getRequestExpired);
      InputStream streamExpired = channel.receive().getInputStream();
      GetResponse respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      assertEquals(ServerErrorCode.Blob_Expired, respExpired.getPartitionResponseInfoList().get(0).getErrorCode());

      // 2. With Include_Expired flag
      idsExpired = new ArrayList<>();
      partitionExpired = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      idsExpired.add(blobId4);
      partitionRequestInfoListExpired = new ArrayList<>();
      partitionRequestInfoExpired = new PartitionRequestInfo(partitionExpired, idsExpired);
      partitionRequestInfoListExpired.add(partitionRequestInfoExpired);
      getRequestExpired =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoListExpired,
              GetOption.Include_Expired_Blobs);
      channel.send(getRequestExpired);
      streamExpired = channel.receive().getInputStream();
      respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(respExpired.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("ownerid", propertyOutput.getOwnerId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get user metadata
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest2);
      stream = channel.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          assertArrayEquals("EncryptionKey mismatch", encryptionKey,
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
      } catch (MessageFormatException e) {
        assertEquals(false, true);
      }

      // get blob info
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest3);
      stream = channel.receive().getInputStream();
      GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      InputStream responseStream = resp3.getInputStream();
      // verify blob properties.
      BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(responseStream);
      assertEquals(31870, propertyOutput.getBlobSize());
      assertEquals("serviceid1", propertyOutput.getServiceId());
      assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
      assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
      // verify user metadata
      ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(responseStream);
      Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
      if (testEncryption) {
        assertNotNull("MessageMetadata should not have been null",
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        assertArrayEquals("EncryptionKey mismatch", encryptionKey,
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
      } else {
        assertNull("MessageMetadata should have been null",
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
      }

      // get blob all
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest4);
      stream = channel.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      responseStream = resp4.getInputStream();
      BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      byte[] actualBlobData = new byte[(int) blobAll.getBlobData().getSize()];
      blobAll.getBlobData().getStream().getByteBuffer().get(actualBlobData);
      // verify content
      Assert.assertArrayEquals("Content mismatch", data, actualBlobData);
      if (testEncryption) {
        Assert.assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
        Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
      } else {
        Assert.assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
      }

      // get blob data
      // if encryption is enabled, router will try to decrypt the blob content using encryptionKey which will fail, as
      // encryptionKey in this test doesn't have any relation to the content. Both are random bytes for test purposes.
      if (!testEncryption) {
        // Use router to get the blob
        Properties routerProperties = getRouterProps(routerDatacenter);
        routerProperties.putAll(routerProps);
        VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProperties);
        Router router = new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, new MockNotificationSystem(9),
            getSSLFactoryIfRequired(routerVerifiableProps)).getRouter();
        checkBlobId(router, blobId1, data);
        router.close();
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), properties.getAccountId(), properties.getContainerId(), partition, false));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest5);
      stream = channel.receive().getInputStream();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.Blob_Not_Found, resp5.getPartitionResponseInfoList().get(0).getErrorCode());
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Tests the case where a late PutRequest is sent to each server when it already has a record for the given BlobId.
   * The expected error from each server should be the given error.
   * @param blobId the {@link BlobId} of the blob to be put.
   * @param properties the {@link BlobProperties} of the blob to be put.
   * @param usermetadata the usermetadata of the blob to be put.
   * @param data the blob data of the blob to be put.
   * @param encryptionKey the encryption key of the blob. Could be null for non-encrypted blobs
   * @param channelToDatanode1 the {@link BlockingChannel} to the Datanode1.
   * @param channelToDatanode2 the {@link BlockingChannel} to the Datanode2.
   * @param channelToDatanode3 the {@link BlockingChannel} to the Datanode3.
   * @param expectedErrorCode the {@link ServerErrorCode} that is expected from every Datanode.
   * @throws IOException
   */
  private static void testLatePutRequest(BlobId blobId, BlobProperties properties, byte[] usermetadata, byte[] data,
      byte[] encryptionKey, BlockingChannel channelToDatanode1, BlockingChannel channelToDatanode2,
      BlockingChannel channelToDatanode3, ServerErrorCode expectedErrorCode) throws IOException {
    // Send put requests for an existing blobId for the exact blob to simulate a request arriving late.
    PutRequest latePutRequest1 =
        new PutRequest(1, "client1", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);
    PutRequest latePutRequest2 =
        new PutRequest(1, "client2", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);
    PutRequest latePutRequest3 =
        new PutRequest(1, "client3", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);
    channelToDatanode1.send(latePutRequest1);
    InputStream putResponseStream = channelToDatanode1.receive().getInputStream();
    PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());

    channelToDatanode2.send(latePutRequest2);
    putResponseStream = channelToDatanode2.receive().getInputStream();
    response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());

    channelToDatanode3.send(latePutRequest3);
    putResponseStream = channelToDatanode3.receive().getInputStream();
    response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionTest(int interestedDataNodePortNumber,
      Port dataNode1Port, Port dataNode2Port, Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1,
      SSLConfig clientSSLConfig2, SSLConfig clientSSLConfig3, SSLSocketFactory clientSSLSocketFactory1,
      SSLSocketFactory clientSSLSocketFactory2, SSLSocketFactory clientSSLSocketFactory3,
      MockNotificationSystem notificationSystem, boolean testEncryption)
      throws InterruptedException, IOException, InstantiationException {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainTextPort
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      List<AmbryServer> serverList = cluster.getServers();
      byte[] usermetadata = new byte[100];
      byte[] data = new byte[100];
      byte[] encryptionKey = null;
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      BlobProperties properties = new BlobProperties(100, "serviceid1", accountId, containerId, false);
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(data);
      if (testEncryption) {
        encryptionKey = new byte[100];
        TestUtils.RANDOM.nextBytes(encryptionKey);
      }

      // connect to all the servers
      BlockingChannel channel1 =
          getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel2 =
          getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory2, clientSSLConfig2);
      BlockingChannel channel3 =
          getBlockingChannelBasedOnPortType(dataNode3Port, "localhost", clientSSLSocketFactory3, clientSSLConfig3);

      // put all the blobs to random servers
      channel1.connect();
      channel2.connect();
      channel3.connect();

      int noOfParallelThreads = 3;
      CountDownLatch latch = new CountDownLatch(noOfParallelThreads);
      List<DirectSender> runnables = new ArrayList<DirectSender>(noOfParallelThreads);
      BlockingChannel channel = null;
      for (int i = 0; i < noOfParallelThreads; i++) {
        if (i % noOfParallelThreads == 0) {
          channel = channel1;
        } else if (i % noOfParallelThreads == 1) {
          channel = channel2;
        } else if (i % noOfParallelThreads == 2) {
          channel = channel3;
        }
        DirectSender runnable =
            new DirectSender(cluster, channel, 50, data, usermetadata, properties, encryptionKey, latch);
        runnables.add(runnable);
        Thread threadToRun = new Thread(runnable);
        threadToRun.start();
      }
      assertTrue("Did not put all blobs in 2 minutes", latch.await(2, TimeUnit.MINUTES));

      // wait till replication can complete
      List<BlobId> blobIds = new ArrayList<BlobId>();
      for (int i = 0; i < runnables.size(); i++) {
        blobIds.addAll(runnables.get(i).getBlobIds());
      }
      for (BlobId blobId : blobIds) {
        notificationSystem.awaitBlobCreations(blobId.getID());
      }

      // Now that the blob is created and replicated, test the cases where a put request arrives for the same blob id
      // later than replication.
      testLatePutRequest(blobIds.get(0), properties, usermetadata, data, encryptionKey, channel1, channel2, channel3,
          ServerErrorCode.No_Error);
      // Test the case where a put arrives with the same id as one in the server, but the blob is not identical.
      BlobProperties differentProperties =
          new BlobProperties(properties.getBlobSize(), properties.getServiceId(), accountId, containerId,
              testEncryption);
      testLatePutRequest(blobIds.get(0), differentProperties, usermetadata, data, encryptionKey, channel1, channel2,
          channel3, ServerErrorCode.Blob_Already_Exists);
      byte[] differentUsermetadata = Arrays.copyOf(usermetadata, usermetadata.length);
      differentUsermetadata[0] = (byte) ~differentUsermetadata[0];
      testLatePutRequest(blobIds.get(0), properties, differentUsermetadata, data, encryptionKey, channel1, channel2,
          channel3, ServerErrorCode.Blob_Already_Exists);
      byte[] differentData = Arrays.copyOf(data, data.length);
      differentData[0] = (byte) ~differentData[0];
      testLatePutRequest(blobIds.get(0), properties, usermetadata, differentData, encryptionKey, channel1, channel2,
          channel3, ServerErrorCode.Blob_Already_Exists);

      // verify blob properties, metadata and blob across all nodes
      for (int i = 0; i < 3; i++) {
        channel = null;
        if (i == 0) {
          channel = channel1;
        } else if (i == 1) {
          channel = channel2;
        } else if (i == 2) {
          channel = channel3;
        }

        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
        for (int j = 0; j < blobIds.size(); j++) {
          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                  GetOption.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
            assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
            assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }

          // get user metadata
          getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
              GetOption.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.fail();
          }

          // get blob
          getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.fail();
          }

          // get blob all
          getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
            byte[] blobout = new byte[(int) blobAll.getBlobData().getSize()];
            int readsize = 0;
            while (readsize < blobAll.getBlobData().getSize()) {
              readsize += blobAll.getBlobData()
                  .getStream()
                  .read(blobout, readsize, (int) blobAll.getBlobData().getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              Assert.assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
              Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
            } else {
              Assert.assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
            }
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.fail();
          }
        }
      }

      // delete random blobs, wait for replication and ensure it is deleted in all nodes

      Set<BlobId> blobsDeleted = new HashSet<BlobId>();
      Set<BlobId> blobsChecked = new HashSet<BlobId>();
      for (int i = 0; i < blobIds.size(); i++) {
        int j = new Random().nextInt(3);
        if (j == 0) {
          j = new Random().nextInt(3);
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }
          DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobIds.get(i), System.currentTimeMillis());
          channel.send(deleteRequest);
          InputStream deleteResponseStream = channel.receive().getInputStream();
          DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
          assertEquals(ServerErrorCode.No_Error, deleteResponse.getError());
          blobsDeleted.add(blobIds.get(i));
        }
      }

      Iterator<BlobId> iterator = blobsDeleted.iterator();
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      while (iterator.hasNext()) {
        BlobId deletedId = iterator.next();
        notificationSystem.awaitBlobDeletions(deletedId.getID());
        for (int j = 0; j < 3; j++) {
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }

          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(deletedId);
          partitionRequestInfoList.clear();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(deletedId.getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          assertEquals(ServerErrorCode.Blob_Deleted, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        }
      }

      // take a server down, clean up a mount path, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      MockDataNodeId dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(0));
      for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
        if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
          System.out.println("Cleaning partition " + replicaId.getPartitionId());
        }
      }
      deleteFolderContent(new File(dataNode.getMountPaths().get(0)), false);
      for (int i = 0; i < blobIds.size(); i++) {
        for (ReplicaId replicaId : blobIds.get(i).getPartition().getReplicaIds()) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
            if (blobsDeleted.contains(blobIds.get(i))) {
              notificationSystem.decrementDeletedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
                  dataNode.getPort());
            } else {
              notificationSystem.decrementCreatedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
                  dataNode.getPort());
            }
          }
        }
      }
      serverList.get(0).startup();

      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        if (blobsDeleted.contains(blobIds.get(j))) {
          notificationSystem.awaitBlobDeletions(blobIds.get(j).getID());
        } else {
          notificationSystem.awaitBlobCreations(blobIds.get(j).getID());
        }
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
            assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
            assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get user metadata
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob all
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
          blobsDeleted.remove(blobIds.get(j));
          blobsChecked.add(blobIds.get(j));
        } else {
          try {
            BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
            byte[] blobout = new byte[(int) blobAll.getBlobData().getSize()];
            int readsize = 0;
            while (readsize < blobAll.getBlobData().getSize()) {
              readsize += blobAll.getBlobData()
                  .getStream()
                  .read(blobout, readsize, (int) blobAll.getBlobData().getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              Assert.assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
              Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
            } else {
              Assert.assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }
      }
      assertEquals(0, blobsDeleted.size());
      // take a server down, clean all contents, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      for (int i = 0; i < dataNode.getMountPaths().size(); i++) {
        System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(i));
        for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(i)) == 0) {
            System.out.println("Cleaning partition " + replicaId.getPartitionId());
          }
        }
        deleteFolderContent(new File(dataNode.getMountPaths().get(i)), false);
      }
      for (int i = 0; i < blobIds.size(); i++) {
        if (blobsChecked.contains(blobIds.get(i))) {
          notificationSystem.decrementDeletedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
              dataNode.getPort());
        } else {
          notificationSystem.decrementCreatedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
              dataNode.getPort());
        }
      }
      serverList.get(0).startup();

      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        if (blobsChecked.contains(blobIds.get(j))) {
          notificationSystem.awaitBlobDeletions(blobIds.get(j).getID());
        } else {
          notificationSystem.awaitBlobCreations(blobIds.get(j).getID());
        }
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
            assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
            assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get user metadata
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              assertNotNull("MessageMetadata should not have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
              assertArrayEquals("EncryptionKey mismatch", encryptionKey, resp.getPartitionResponseInfoList()
                  .get(0)
                  .getMessageMetadataList()
                  .get(0)
                  .getEncryptionKey()
                  .array());
            } else {
              assertNull("MessageMetadata should have been null",
                  resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob all
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
          blobsChecked.remove(blobIds.get(j));
        } else {
          try {
            BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
            byte[] blobout = new byte[(int) blobAll.getBlobData().getSize()];
            int readsize = 0;
            while (readsize < blobAll.getBlobData().getSize()) {
              readsize += blobAll.getBlobData()
                  .getStream()
                  .read(blobout, readsize, (int) blobAll.getBlobData().getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
            if (testEncryption) {
              Assert.assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
              Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
            } else {
              Assert.assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
            }
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }
      }
      assertEquals(0, blobsChecked.size());

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest(String sourceDatacenter,
      String sslEnabledDatacenters, PortType portType, MockCluster cluster, MockNotificationSystem notificationSystem,
      Properties routerProps) throws Exception {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", sourceDatacenter);
    props.setProperty("router.put.request.parallelism", "1");
    props.setProperty("router.put.success.target", "1");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", sourceDatacenter);
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    props.putAll(routerProps);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    Router router = new NonBlockingRouterFactory(verifiableProperties, cluster.getClusterMap(), notificationSystem,
        getSSLFactoryIfRequired(verifiableProperties)).getRouter();
    int numberOfRequestsToSend = 15;
    int numberOfVerifierThreads = 3;
    final LinkedBlockingQueue<Payload> payloadQueue = new LinkedBlockingQueue<Payload>();
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
    final CountDownLatch callbackLatch = new CountDownLatch(numberOfRequestsToSend);
    List<Future<String>> putFutures = new ArrayList<>(numberOfRequestsToSend);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    for (int i = 0; i < numberOfRequestsToSend; i++) {
      int size = new Random().nextInt(5000);
      final BlobProperties properties =
          new BlobProperties(size, "service1", "owner id check", "image/jpeg", false, Utils.Infinite_Time, accountId,
              containerId, false);
      final byte[] metadata = new byte[new Random().nextInt(1000)];
      final byte[] blob = new byte[size];
      TestUtils.RANDOM.nextBytes(metadata);
      TestUtils.RANDOM.nextBytes(blob);
      Future<String> future =
          router.putBlob(properties, metadata, new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob)),
              new Callback<String>() {
                @Override
                public void onCompletion(String result, Exception exception) {
                  if (exception == null) {
                    payloadQueue.add(new Payload(properties, metadata, blob, result));
                  } else {
                    exceptionRef.set(exception);
                  }
                  callbackLatch.countDown();
                }
              });
      putFutures.add(future);
    }
    for (Future<String> future : putFutures) {
      future.get(1, TimeUnit.SECONDS);
    }
    assertTrue("Did not receive all callbacks in time", callbackLatch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
    assertEquals("Did not put expected number of blobs", numberOfRequestsToSend, payloadQueue.size());
    Properties sslProps = new Properties();
    sslProps.putAll(routerProps);
    sslProps.setProperty("clustermap.ssl.enabled.datacenters", sslEnabledDatacenters);
    sslProps.setProperty("clustermap.cluster.name", "test");
    sslProps.setProperty("clustermap.datacenter.name", sourceDatacenter);
    sslProps.setProperty("clustermap.host.name", "localhost");
    VerifiableProperties vProps = new VerifiableProperties(sslProps);
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(new Properties())),
            new SSLConfig(vProps), new ClusterMapConfig(vProps), new MetricRegistry());
    CountDownLatch verifierLatch = new CountDownLatch(numberOfVerifierThreads);
    AtomicInteger totalRequests = new AtomicInteger(numberOfRequestsToSend);
    AtomicInteger verifiedRequests = new AtomicInteger(0);
    AtomicBoolean cancelTest = new AtomicBoolean(false);
    for (int i = 0; i < numberOfVerifierThreads; i++) {
      Thread thread = new Thread(
          new Verifier(payloadQueue, verifierLatch, totalRequests, verifiedRequests, cluster.getClusterMap(),
              cancelTest, portType, connectionPool, notificationSystem));
      thread.start();
    }
    assertTrue("Did not verify in 2 minutes", verifierLatch.await(2, TimeUnit.MINUTES));

    assertEquals(totalRequests.get(), verifiedRequests.get());
    router.close();
    connectionPool.shutdown();
  }

  protected static void endToEndReplicationWithMultiNodeSinglePartitionTest(String routerDatacenter,
      String sslEnabledDatacenters, int interestedDataNodePortNumber, Port dataNode1Port, Port dataNode2Port,
      Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1, SSLSocketFactory clientSSLSocketFactory1,
      MockNotificationSystem notificationSystem, Properties routerProps, boolean testEncryption)
      throws InterruptedException, IOException, InstantiationException {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainText port
    try {
      int expectedTokenSize = 0;
      MockClusterMap clusterMap = cluster.getClusterMap();
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      ArrayList<BlobProperties> propertyList = new ArrayList<>();
      ArrayList<BlobId> blobIdList = new ArrayList<>();
      ArrayList<byte[]> dataList = new ArrayList<>();
      ArrayList<byte[]> encryptionKeyList = new ArrayList<>();
      byte[] usermetadata = new byte[1000];
      TestUtils.RANDOM.nextBytes(usermetadata);
      PartitionId partition = clusterMap.getWritablePartitionIds().get(0);

      for (int i = 0; i < 11; i++) {
        short accountId = Utils.getRandomShort(TestUtils.RANDOM);
        short containerId = Utils.getRandomShort(TestUtils.RANDOM);
        propertyList.add(new BlobProperties(1000, "serviceid1", accountId, containerId, testEncryption));
        blobIdList.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
            clusterMap.getLocalDatacenterId(), accountId, containerId, partition, false));
        dataList.add(TestUtils.getRandomBytes(1000));
        if (testEncryption) {
          encryptionKeyList.add(TestUtils.getRandomBytes(128));
        } else {
          encryptionKeyList.add(null);
        }
      }

      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobIdList.get(0), propertyList.get(0), ByteBuffer.wrap(usermetadata),
              ByteBuffer.wrap(dataList.get(0)), propertyList.get(0).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(0) != null ? ByteBuffer.wrap(encryptionKeyList.get(0)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(0), blobIdList.get(0),
          encryptionKeyList.get(0) != null ? ByteBuffer.wrap(encryptionKeyList.get(0)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(0));

      BlockingChannel channel1 =
          getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel2 =
          getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel3 =
          getBlockingChannelBasedOnPortType(dataNode3Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);

      channel1.connect();
      channel2.connect();
      channel3.connect();
      channel1.send(putRequest);
      InputStream putResponseStream = channel1.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());
      // put blob 2
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobIdList.get(1), propertyList.get(1), ByteBuffer.wrap(usermetadata),
              ByteBuffer.wrap(dataList.get(1)), propertyList.get(1).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(1) != null ? ByteBuffer.wrap(encryptionKeyList.get(1)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(1), blobIdList.get(1),
          encryptionKeyList.get(1) != null ? ByteBuffer.wrap(encryptionKeyList.get(1)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(1));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());
      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobIdList.get(2), propertyList.get(2), ByteBuffer.wrap(usermetadata),
              ByteBuffer.wrap(dataList.get(2)), propertyList.get(2).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(2) != null ? ByteBuffer.wrap(encryptionKeyList.get(2)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(2), blobIdList.get(2),
          encryptionKeyList.get(2) != null ? ByteBuffer.wrap(encryptionKeyList.get(2)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(2));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 4
      putRequest = new PutRequest(1, "client1", blobIdList.get(3), propertyList.get(3), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(3)), propertyList.get(3).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(3) != null ? ByteBuffer.wrap(encryptionKeyList.get(3)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(3), blobIdList.get(3),
          encryptionKeyList.get(3) != null ? ByteBuffer.wrap(encryptionKeyList.get(3)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(3));
      channel1.send(putRequest);
      putResponseStream = channel1.receive().getInputStream();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());

      // put blob 5
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(4), propertyList.get(4), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(4)), propertyList.get(4).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(4) != null ? ByteBuffer.wrap(encryptionKeyList.get(4)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(4), blobIdList.get(4),
          encryptionKeyList.get(4) != null ? ByteBuffer.wrap(encryptionKeyList.get(4)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(4));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 6
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(5), propertyList.get(5), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(5)), propertyList.get(5).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(5) != null ? ByteBuffer.wrap(encryptionKeyList.get(5)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(5), blobIdList.get(5),
          encryptionKeyList.get(5) != null ? ByteBuffer.wrap(encryptionKeyList.get(5)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(5));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());
      // wait till replication can complete
      notificationSystem.awaitBlobCreations(blobIdList.get(0).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(3).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(5).getID());

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId mockPartitionId = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobIdList.get(2));
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(mockPartitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel2.send(getRequest1);
      InputStream stream = channel2.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp1.getError());
      assertEquals(ServerErrorCode.No_Error, resp1.getPartitionResponseInfoList().get(0).getErrorCode());
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(1000, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("AccountId mismatch", propertyList.get(2).getAccountId(), propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", propertyList.get(2).getContainerId(), propertyOutput.getContainerId());
        assertEquals("IsEncrypted mismatch", propertyList.get(2).isEncrypted(), propertyOutput.isEncrypted());
      } catch (MessageFormatException e) {
        Assert.fail();
      }
      // get user metadata
      ids.clear();
      ids.add(blobIdList.get(1));
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest2);
      stream = channel1.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp2.getError());
      assertEquals(ServerErrorCode.No_Error, resp2.getPartitionResponseInfoList().get(0).getErrorCode());
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(1),
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob
      ids.clear();
      ids.add(blobIdList.get(0));
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest3);
      stream = channel3.receive().getInputStream();
      GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobData blobData = MessageFormatRecord.deserializeBlob(resp3.getInputStream());
        byte[] blobout = new byte[(int) blobData.getSize()];
        int readsize = 0;
        while (readsize < blobData.getSize()) {
          readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
        }
        Assert.assertArrayEquals(dataList.get(0), blobout);
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(0),
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob all
      ids.clear();
      ids.add(blobIdList.get(0));
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest4);
      stream = channel1.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp4.getInputStream(), blobIdFactory);
        byte[] blobout = new byte[(int) blobAll.getBlobData().getSize()];
        int readsize = 0;
        while (readsize < blobAll.getBlobData().getSize()) {
          readsize += blobAll.getBlobData()
              .getStream()
              .read(blobout, readsize, (int) blobAll.getBlobData().getSize() - readsize);
        }
        Assert.assertArrayEquals(dataList.get(0), blobout);
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null", blobAll.getBlobEncryptionKey());
          assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(0), blobAll.getBlobEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null", blobAll.getBlobEncryptionKey());
        }
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      if (!testEncryption) {
        // get blob data
        // Use router to get the blob
        Properties routerProperties = getRouterProps(routerDatacenter);
        routerProperties.putAll(routerProps);
        VerifiableProperties routerVerifiableProperties = new VerifiableProperties(routerProperties);
        Router router = new NonBlockingRouterFactory(routerVerifiableProperties, clusterMap, notificationSystem,
            getSSLFactoryIfRequired(routerVerifiableProperties)).getRouter();
        checkBlobId(router, blobIdList.get(0), dataList.get(0));
        checkBlobId(router, blobIdList.get(1), dataList.get(1));
        checkBlobId(router, blobIdList.get(2), dataList.get(2));
        checkBlobId(router, blobIdList.get(3), dataList.get(3));
        checkBlobId(router, blobIdList.get(4), dataList.get(4));
        checkBlobId(router, blobIdList.get(5), dataList.get(5));
        router.close();
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      mockPartitionId = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), propertyList.get(0).getAccountId(), propertyList.get(0).getContainerId(),
          mockPartitionId, false));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(mockPartitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest5);
      stream = channel3.receive().getInputStream();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp5.getError());
      assertEquals(ServerErrorCode.Blob_Not_Found, resp5.getPartitionResponseInfoList().get(0).getErrorCode());

      // delete a blob and ensure it is propagated
      DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobIdList.get(0), System.currentTimeMillis());
      expectedTokenSize += getDeleteRecordSize(blobIdList.get(0));
      channel1.send(deleteRequest);
      InputStream deleteResponseStream = channel1.receive().getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
      assertEquals(ServerErrorCode.No_Error, deleteResponse.getError());

      notificationSystem.awaitBlobDeletions(blobIdList.get(0).getID());
      ids = new ArrayList<BlobId>();
      ids.add(blobIdList.get(0));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest6 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest6);
      stream = channel3.receive().getInputStream();
      GetResponse resp6 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp6.getError());
      assertEquals(ServerErrorCode.Blob_Deleted, resp6.getPartitionResponseInfoList().get(0).getErrorCode());

      // get the data node to inspect replication tokens on
      DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      // read the replica file and check correctness
      // The token offset value of 13098 was derived as followed:
      // - Up to this point we have done 6 puts and 1 delete

      // - Each put takes up 2183 bytes in the log (1000 data, 1000 user metadata, 183 ambry metadata)
      // - Each delete takes up 97 bytes in the log
      // - The offset stored in the token will be the position of the last entry in the log (the delete, in this case)
      // - Thus, it will be at the end of the 6 puts: 6 * 2183 = 13098

      checkReplicaTokens(clusterMap, dataNodeId, expectedTokenSize - getDeleteRecordSize(blobIdList.get(0)), "0");

      // Shut down server 1
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();
      // Add more data to server 2 and server 3. Recover server 1 and ensure it is completely replicated
      // put blob 7
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(6), propertyList.get(6), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(6)), propertyList.get(6).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(6) != null ? ByteBuffer.wrap(encryptionKeyList.get(6)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(6), blobIdList.get(6),
          encryptionKeyList.get(6) != null ? ByteBuffer.wrap(encryptionKeyList.get(6)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(6));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 8
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(7), propertyList.get(7), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(7)), propertyList.get(7).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(7) != null ? ByteBuffer.wrap(encryptionKeyList.get(7)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(7), blobIdList.get(7),
          encryptionKeyList.get(7) != null ? ByteBuffer.wrap(encryptionKeyList.get(7)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(7));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 9
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(8), propertyList.get(8), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(8)), propertyList.get(8).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(8) != null ? ByteBuffer.wrap(encryptionKeyList.get(8)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(8), blobIdList.get(8),
          encryptionKeyList.get(8) != null ? ByteBuffer.wrap(encryptionKeyList.get(8)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(8));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 10
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(9), propertyList.get(9), ByteBuffer.wrap(usermetadata),
          ByteBuffer.wrap(dataList.get(9)), propertyList.get(9).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(9) != null ? ByteBuffer.wrap(encryptionKeyList.get(9)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(9), blobIdList.get(9),
          encryptionKeyList.get(9) != null ? ByteBuffer.wrap(encryptionKeyList.get(9)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(9));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 11
      putRequest2 =
          new PutRequest(1, "client1", blobIdList.get(10), propertyList.get(10), ByteBuffer.wrap(usermetadata),
              ByteBuffer.wrap(dataList.get(10)), propertyList.get(10).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(10), blobIdList.get(10),
          encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(10));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      cluster.getServers().get(0).startup();
      // wait for server to recover
      notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(9).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(10).getID());
      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(clusterMap, blobIdList.get(1), channel1, dataList.get(1), encryptionKeyList.get(1));
        checkBlobContent(clusterMap, blobIdList.get(2), channel1, dataList.get(2), encryptionKeyList.get(2));
        checkBlobContent(clusterMap, blobIdList.get(3), channel1, dataList.get(3), encryptionKeyList.get(3));
        checkBlobContent(clusterMap, blobIdList.get(4), channel1, dataList.get(4), encryptionKeyList.get(4));
        checkBlobContent(clusterMap, blobIdList.get(5), channel1, dataList.get(5), encryptionKeyList.get(5));
        checkBlobContent(clusterMap, blobIdList.get(6), channel1, dataList.get(6), encryptionKeyList.get(6));
        checkBlobContent(clusterMap, blobIdList.get(7), channel1, dataList.get(7), encryptionKeyList.get(7));
        checkBlobContent(clusterMap, blobIdList.get(8), channel1, dataList.get(8), encryptionKeyList.get(8));
        checkBlobContent(clusterMap, blobIdList.get(9), channel1, dataList.get(9), encryptionKeyList.get(9));
        checkBlobContent(clusterMap, blobIdList.get(10), channel1, dataList.get(10), encryptionKeyList.get(10));
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // Shutdown server 1. Remove all its data from all mount path. Recover server 1 and ensure node is built
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();

      File mountFile = new File(clusterMap.getReplicaIds(dataNodeId).get(0).getMountPath());
      for (File toDelete : mountFile.listFiles()) {
        deleteFolderContent(toDelete, true);
      }
      notificationSystem.decrementCreatedReplica(blobIdList.get(1).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(2).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(3).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(4).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(5).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(6).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(7).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(8).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(9).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobIdList.get(10).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort());

      cluster.getServers().get(0).startup();
      notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(3).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(5).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(9).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(10).getID());

      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(clusterMap, blobIdList.get(1), channel1, dataList.get(1), encryptionKeyList.get(1));
        checkBlobContent(clusterMap, blobIdList.get(2), channel1, dataList.get(2), encryptionKeyList.get(2));
        checkBlobContent(clusterMap, blobIdList.get(3), channel1, dataList.get(3), encryptionKeyList.get(3));
        checkBlobContent(clusterMap, blobIdList.get(4), channel1, dataList.get(4), encryptionKeyList.get(4));
        checkBlobContent(clusterMap, blobIdList.get(5), channel1, dataList.get(5), encryptionKeyList.get(5));
        checkBlobContent(clusterMap, blobIdList.get(6), channel1, dataList.get(6), encryptionKeyList.get(6));
        checkBlobContent(clusterMap, blobIdList.get(7), channel1, dataList.get(7), encryptionKeyList.get(7));
        checkBlobContent(clusterMap, blobIdList.get(8), channel1, dataList.get(8), encryptionKeyList.get(8));
        checkBlobContent(clusterMap, blobIdList.get(9), channel1, dataList.get(9), encryptionKeyList.get(9));
        checkBlobContent(clusterMap, blobIdList.get(10), channel1, dataList.get(10), encryptionKeyList.get(10));
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Fetches the put record size in log
   * @param properties {@link BlobProperties} associated with the put
   * @param blobId {@link BlobId} associated with the put
   * @param usermetadata Usermetadata associated with the put
   * @param data actual data associated with the put
   * @return the size of the put record in the log
   */
  private static long getPutRecordSize(BlobProperties properties, BlobId blobId, ByteBuffer blobEncryptionKey,
      ByteBuffer usermetadata, byte[] data) {
    return MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize() + blobId.sizeInBytes() + (
        blobEncryptionKey != null ? MessageFormatRecord.BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(
            blobEncryptionKey) : 0) + +MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(
        properties) + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(usermetadata)
        + MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(data.length);
  }

  /**
   * Fetches the delete record size in log
   * @param blobId {@link BlobId} associated with the delete
   * @return the size of the put record in the log
   */
  private static long getDeleteRecordSize(BlobId blobId) {
    return MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize() + blobId.sizeInBytes()
        + MessageFormatRecord.Delete_Format_V2.getDeleteRecordSize();
  }

  /**
   * Repeatedly check the replication token file until a certain offset value on all nodes on a certain
   * partition is found.  Fail if {@code numTries} is exceeded or a token offset larger than the target
   * is found.
   * @param clusterMap the cluster map that contains the data node to inspect
   * @param dataNodeId the data node to inspect
   * @param targetOffset the token offset to look for in the {@code targetPartition}
   * @param targetPartition the name of the partition to look for the {@code targetOffset}
   * @throws Exception
   */
  private static void checkReplicaTokens(MockClusterMap clusterMap, DataNodeId dataNodeId, long targetOffset,
      String targetPartition) throws Exception {
    List<String> mountPaths = ((MockDataNodeId) dataNodeId).getMountPaths();

    // we should have an entry for each partition - remote replica pair
    Set<String> completeSetToCheck = new HashSet<>();
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
    int numRemoteNodes = 0;
    for (ReplicaId replicaId : replicaIds) {
      List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
      if (replicaId.getPartitionId().isEqual(targetPartition)) {
        numRemoteNodes = peerReplicas.size();
      }
      for (ReplicaId peerReplica : peerReplicas) {
        completeSetToCheck.add(replicaId.getPartitionId().toString() + peerReplica.getDataNodeId().getHostname()
            + peerReplica.getDataNodeId().getPort());
      }
    }

    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

    int numTries = 4;
    boolean foundTarget = false;
    while (!foundTarget && numTries > 0) {
      Thread.sleep(5000);
      numTries--;
      Set<String> setToCheck = new HashSet<String>(completeSetToCheck);
      int numFound = 0;
      for (String mountPath : mountPaths) {
        File replicaTokenFile = new File(mountPath, "replicaTokens");
        if (replicaTokenFile.exists()) {
          CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
          DataInputStream dataInputStream = new DataInputStream(crcStream);
          try {
            short version = dataInputStream.readShort();
            assertEquals(0, version);

            System.out.println("setToCheck" + setToCheck.size());
            while (dataInputStream.available() > 8) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(dataInputStream);
              // read remote node host name
              String hostname = Utils.readIntString(dataInputStream);
              // read remote replica path
              Utils.readIntString(dataInputStream);

              // read remote port
              int port = dataInputStream.readInt();
              Assert.assertTrue(setToCheck.contains(partitionId.toString() + hostname + port));
              setToCheck.remove(partitionId.toString() + hostname + port);
              // read total bytes read from local store
              dataInputStream.readLong();
              // read replica token
              StoreFindToken token = (StoreFindToken) factory.getFindToken(dataInputStream);
              System.out.println(
                  "partitionId " + partitionId + " hostname " + hostname + " port " + port + " token " + token);
              Offset endTokenOffset = token.getOffset();
              long parsedToken = endTokenOffset == null ? -1 : endTokenOffset.getOffset();
              System.out.println("The parsed token is " + parsedToken);
              if (partitionId.isEqual(targetPartition)) {
                Assert.assertFalse(
                    "Parsed offset: " + parsedToken + " must not be larger than target value: " + targetOffset,
                    parsedToken > targetOffset);
                if (parsedToken == targetOffset) {
                  numFound++;
                }
              } else {
                assertEquals("Tokens should remain at -1 offsets on unmodified partitions", -1, parsedToken);
              }
            }
            long crc = crcStream.getValue();
            assertEquals(crc, dataInputStream.readLong());
          } catch (IOException e) {
            Assert.fail();
          } finally {
            dataInputStream.close();
          }
        }
      }
      if (numFound == numRemoteNodes) {
        foundTarget = true;
      }
    }
    if (!foundTarget) {
      Assert.fail("Could not find target token offset: " + targetOffset);
    }
  }

  private static void checkBlobId(Router router, BlobId blobId, byte[] data) throws Exception {
    GetBlobResult result = router.getBlob(blobId.getID(), new GetBlobOptionsBuilder().build()).get(1, TimeUnit.SECONDS);
    ReadableStreamChannel blob = result.getBlobDataChannel();
    assertEquals("Size does not match that of data", data.length,
        result.getBlobInfo().getBlobProperties().getBlobSize());
    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel();
    blob.readInto(channel, null).get(1, TimeUnit.SECONDS);
    Assert.assertArrayEquals(data, channel.getData());
  }

  private static void checkBlobContent(MockClusterMap clusterMap, BlobId blobId, BlockingChannel channel,
      byte[] dataToCheck, byte[] encryptionKey) throws IOException, MessageFormatException {
    ArrayList<BlobId> listIds = new ArrayList<BlobId>();
    listIds.add(blobId);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.clear();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), listIds);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest3 =
        new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    channel.send(getRequest3);
    InputStream stream = channel.receive().getInputStream();
    GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    assertEquals(ServerErrorCode.No_Error, resp.getError());
    BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = new byte[(int) blobData.getSize()];
    int readsize = 0;
    while (readsize < blobData.getSize()) {
      readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
    }
    Assert.assertArrayEquals(dataToCheck, blobout);
    if (encryptionKey != null) {
      Assert.assertNotNull("EncryptionKey should not have been null",
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
      Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
    } else {
      Assert.assertNull("EncryptionKey should have been null",
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
    }
  }

  /**
   * Generate and return {@link Properties} to instantiate {@link Router}
   * @param routerDatacenter Router's datacentre
   * @return the {@link Properties} thus constructed
   */
  private static Properties getRouterProps(String routerDatacenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDatacenter);
    properties.setProperty("router.get.cross.dc.enabled", "true");
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", routerDatacenter);
    properties.setProperty("clustermap.host.name", "localhost");
    return properties;
  }

  private static void deleteFolderContent(File folder, boolean deleteParentFolder) {
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteFolderContent(f, true);
        } else {
          f.delete();
        }
      }
    }
    if (deleteParentFolder) {
      folder.delete();
    }
  }

  /**
   * Returns BlockingChannel or SSLBlockingChannel depending on whether the port type is PlainText or SSL
   * for the given targetPort
   * @param targetPort upon which connection has to be established
   * @param hostName upon which connection has to be established
   * @return BlockingChannel
   */
  protected static BlockingChannel getBlockingChannelBasedOnPortType(Port targetPort, String hostName,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    BlockingChannel channel = null;
    if (targetPort.getPortType() == PortType.PLAINTEXT) {
      channel = new BlockingChannel(hostName, targetPort.getPort(), 10000, 10000, 10000, 2000);
    } else if (targetPort.getPortType() == PortType.SSL) {
      channel = new SSLBlockingChannel(hostName, targetPort.getPort(), new MetricRegistry(), 10000, 10000, 10000, 4000,
          sslSocketFactory, sslConfig);
    }
    return channel;
  }

  /**
   * Create an {@link SSLFactory} if there are SSL enabled datacenters in the properties
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @return an {@link SSLFactory}, or {@code null}, if no {@link SSLFactory} is required.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  static SSLFactory getSSLFactoryIfRequired(VerifiableProperties verifiableProperties)
      throws GeneralSecurityException, IOException {
    boolean requiresSSL = new ClusterMapConfig(verifiableProperties).clusterMapSslEnabledDatacenters.length() > 0;
    return requiresSSL ? new SSLFactory(new SSLConfig(verifiableProperties)) : null;
  }
}
