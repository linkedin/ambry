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
import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.cloud.VcrServer;
import com.github.ambry.cloud.VcrTestUtil;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockDiskId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.messageformat.SubRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLBlockingChannel;
import com.github.ambry.network.http2.Http2BlockingChannel;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.BlobStoreControlAction;
import com.github.ambry.protocol.BlobStoreControlAdminRequest;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.router.Callback;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;

import static org.junit.Assert.*;


final class ServerTestUtil {

  static byte[] getBlobDataAndRelease(BlobData blobData) {
    byte[] actualBlobData = new byte[(int) blobData.getSize()];
    ByteBuf buffer = blobData.content();
    try {
      buffer.readBytes(actualBlobData);
    } finally {
      buffer.release();
    }
    return actualBlobData;
  }

  static void endToEndTest(Port targetPort, String routerDatacenter, MockCluster cluster, SSLConfig clientSSLConfig,
      SSLSocketFactory clientSSLSocketFactory, Properties routerProps, boolean testEncryption) {
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);

      BlobProperties properties = new BlobProperties(31870, "serviceid1", accountId, containerId, testEncryption);
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(data);
      if (testEncryption) {
        TestUtils.RANDOM.nextBytes(encryptionKey);
      }
      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId4 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId5 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      ConnectedChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());

      // put blob 2 with an expiry time and apply TTL update later
      BlobProperties propertiesForTtlUpdate =
          new BlobProperties(31870, "serviceid1", "ownerid", "image/png", false, TestUtils.TTL_SECS, accountId,
              containerId, testEncryption, null);
      long ttlUpdateBlobExpiryTimeMs = getExpiryTimeMs(propertiesForTtlUpdate);
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobId2, propertiesForTtlUpdate, ByteBuffer.wrap(usermetadata),
              Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
              testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest2);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest3);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 4 that is expired
      BlobProperties propertiesExpired =
          new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0, accountId, containerId, testEncryption,
              null);
      PutRequest putRequest4 = new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest4);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response4.getError());

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
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
        fail();
      }

      // get blob properties with expired flag set
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
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
        fail();
      }

      // get blob properties for expired blob
      // 1. With no flag
      ArrayList<BlobId> idsExpired = new ArrayList<>();
      MockPartitionId partitionExpired =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
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
      partitionExpired =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
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
        fail();
      }

      // get user metadata
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest2);
      stream = channel.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        assertArrayEquals(usermetadata, userMetadataOutput.array());
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
      assertArrayEquals(usermetadata, userMetadataOutput.array());
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
      byte[] actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
      // verify content
      assertArrayEquals("Content mismatch.", data, actualBlobData);
      if (testEncryption) {
        assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
        assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
      } else {
        assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
      }

      // get blob data
      // if encryption is enabled, router will try to decrypt the blob content using encryptionKey which will fail, as
      // encryptionKey in this test doesn't have any relation to the content. Both are random bytes for test purposes.
      if (!testEncryption) {
        // Use router to get the blob
        Properties routerProperties = getRouterProps(routerDatacenter);
        routerProperties.putAll(routerProps);
        VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProperties);
        AccountService accountService = new InMemAccountService(false, true);
        Router router =
            new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, new MockNotificationSystem(clusterMap),
                getSSLFactoryIfRequired(routerVerifiableProps), accountService).getRouter();
        checkBlobId(router, blobId1, data);
        router.close();
      }

      checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId2, data, false, ttlUpdateBlobExpiryTimeMs);
      updateBlobTtl(channel, blobId2);
      checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId2, data, true, Utils.Infinite_Time);

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), properties.getAccountId(), properties.getContainerId(), partition, false,
          BlobId.BlobDataType.DATACHUNK));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest5);
      stream = channel.receive().getInputStream();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.Blob_Not_Found, resp5.getPartitionResponseInfoList().get(0).getErrorCode());

      // stop the store via AdminRequest
      System.out.println("Begin to stop a BlobStore");
      AdminRequest adminRequest =
          new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionIds.get(0), 1, "clientid2");
      BlobStoreControlAdminRequest controlRequest =
          new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StopStore, adminRequest);
      channel.send(controlRequest);
      stream = channel.receive().getInputStream();
      AdminResponse adminResponse = AdminResponse.readFrom(new DataInputStream(stream));
      assertEquals("Stop store admin request should succeed", ServerErrorCode.No_Error, adminResponse.getError());

      // put a blob on a stopped store, which should fail
      putRequest =
          new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest);
      putResponseStream = channel.receive().getInputStream();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals("Put blob on stopped store should fail", ServerErrorCode.Replica_Unavailable, response.getError());

      // get a blob properties on a stopped store, which should fail
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) blobId1.getPartition();
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals("Get blob properties on stopped store should fail", ServerErrorCode.Replica_Unavailable,
          resp1.getPartitionResponseInfoList().get(0).getErrorCode());

      // delete a blob on a stopped store, which should fail
      DeleteRequest deleteRequest = new DeleteRequest(1, "deleteClient", blobId1, System.currentTimeMillis());
      channel.send(deleteRequest);
      stream = channel.receive().getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(stream));
      assertEquals("Delete blob on stopped store should fail", ServerErrorCode.Replica_Unavailable,
          deleteResponse.getError());

      // start the store via AdminRequest
      System.out.println("Begin to restart the BlobStore");
      adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionIds.get(0), 1, "clientid2");
      controlRequest = new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StartStore, adminRequest);
      channel.send(controlRequest);
      stream = channel.receive().getInputStream();
      adminResponse = AdminResponse.readFrom(new DataInputStream(stream));
      assertEquals("Start store admin request should succeed", ServerErrorCode.No_Error, adminResponse.getError());
      List<? extends ReplicaId> replicaIds = partitionIds.get(0).getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        // forcibly mark replicas and disks as up.
        MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
        mockReplicaId.markReplicaDownStatus(false);
        ((MockDiskId) mockReplicaId.getDiskId()).setDiskState(HardwareState.AVAILABLE, false);
      }

      // put a blob on a restarted store , which should succeed
      PutRequest putRequest5 =
          new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      channel.send(putRequest5);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response5 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals("Put blob on restarted store should succeed", ServerErrorCode.No_Error, response5.getError());

      // get a blob on a restarted store , which should succeed
      ids = new ArrayList<BlobId>();
      PartitionId partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
      assertArrayEquals("Content mismatch.", data, actualBlobData);

      // undelete a not-deleted blob should return fail
      UndeleteRequest undeleteRequest = new UndeleteRequest(1, "undeleteClient", blobId1, System.currentTimeMillis());
      channel.send(undeleteRequest);
      stream = channel.receive().getInputStream();
      UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(new DataInputStream(stream));
      assertEquals("Undelete blob should succeed", ServerErrorCode.Blob_Not_Deleted, undeleteResponse.getError());

      // delete a blob on a restarted store , which should succeed
      deleteRequest = new DeleteRequest(1, "deleteClient", blobId1, System.currentTimeMillis());
      channel.send(deleteRequest);
      stream = channel.receive().getInputStream();
      deleteResponse = DeleteResponse.readFrom(new DataInputStream(stream));
      assertEquals("Delete blob on restarted store should succeed", ServerErrorCode.No_Error,
          deleteResponse.getError());

      // undelete a deleted blob, which should succeed
      undeleteRequest = new UndeleteRequest(2, "undeleteClient", blobId1, System.currentTimeMillis());
      channel.send(undeleteRequest);
      stream = channel.receive().getInputStream();
      undeleteResponse = UndeleteResponse.readFrom(new DataInputStream(stream));
      assertEquals("Undelete blob should succeed", ServerErrorCode.No_Error, undeleteResponse.getError());
      assertEquals("Undelete life version mismatch", undeleteResponse.getLifeVersion(), (short) 1);

      // undelete an already undeleted blob, which should fail
      undeleteRequest = new UndeleteRequest(3, "undeleteClient", blobId1, System.currentTimeMillis());
      channel.send(undeleteRequest);
      stream = channel.receive().getInputStream();
      undeleteResponse = UndeleteResponse.readFrom(new DataInputStream(stream));
      assertEquals("Undelete blob should fail", ServerErrorCode.Blob_Already_Undeleted, undeleteResponse.getError());

      // get an undeleted blob, which should succeed
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
      assertArrayEquals("Content mismatch", data, actualBlobData);

      // Bounce servers to make them read the persisted token file.
      cluster.stopServers();
      cluster.startServers();

      channel.disconnect();
      channel.connect();
      // get an undeleted blob after restart, which should succeed
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
      assertArrayEquals("Content mismatch", data, actualBlobData);

      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      List<? extends ReplicaId> replicaIds = cluster.getClusterMap()
          .getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS)
          .get(0)
          .getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
        ((MockDiskId) mockReplicaId.getDiskId()).setDiskState(HardwareState.AVAILABLE, true);
      }
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
   * @param channelToDatanode1 the {@link ConnectedChannel} to the Datanode1.
   * @param channelToDatanode2 the {@link ConnectedChannel} to the Datanode2.
   * @param channelToDatanode3 the {@link ConnectedChannel} to the Datanode3.
   * @param expectedErrorCode the {@link ServerErrorCode} that is expected from every Datanode.
   * @throws IOException
   */
  private static void testLatePutRequest(BlobId blobId, BlobProperties properties, byte[] usermetadata, byte[] data,
      byte[] encryptionKey, ConnectedChannel channelToDatanode1, ConnectedChannel channelToDatanode2,
      ConnectedChannel channelToDatanode3, ServerErrorCode expectedErrorCode) throws IOException {
    // Send put requests for an existing blobId for the exact blob to simulate a request arriving late.
    PutRequest latePutRequest1 =
        new PutRequest(1, "client1", blobId, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);
    PutRequest latePutRequest2 =
        new PutRequest(1, "client2", blobId, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, encryptionKey != null ? ByteBuffer.wrap(encryptionKey) : null);
    PutRequest latePutRequest3 =
        new PutRequest(1, "client3", blobId, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
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

  /**
   * Tests blobs put to dataNode can be backed up by {@link com.github.ambry.cloud.VcrReplicationManager}.
   * @param cluster the {@link MockCluster} of dataNodes.
   * @param dataNode the datanode where blobs are originally put.
   * @param clientSSLConfig the {@link SSLConfig}.
   * @param clientSSLSocketFactory the {@link SSLSocketFactory}.
   * @param notificationSystem the {@link MockNotificationSystem} to track blobs event in {@link MockCluster}.
   * @param vcrSSLProps SSL related properties for VCR. Can be {@code null}.
   * @param ttl The ttl of blobs in their original PUT.
   * @param doTtlUpdate Do ttlUpdate request if {@true}.
   */
  static void endToEndCloudBackupTest(MockCluster cluster, DataNodeId dataNode, SSLConfig clientSSLConfig,
      SSLSocketFactory clientSSLSocketFactory, MockNotificationSystem notificationSystem, Properties vcrSSLProps,
      long ttl, boolean doTtlUpdate) throws Exception {
    int blobBackupCount = 10;
    int blobSize = 100;
    int userMetaDataSize = 100;
    ClusterMap clusterMap = cluster.getClusterMap();
    ClusterAgentsFactory clusterAgentsFactory = cluster.getClusterAgentsFactory();
    // Send blobs to DataNode
    byte[] userMetadata = new byte[userMetaDataSize];
    byte[] data = new byte[blobSize];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties properties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, ttl, accountId, containerId, false, null);
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);

    Port port = clientSSLConfig == null ? new Port(dataNode.getPort(), PortType.PLAINTEXT)
        : new Port(dataNode.getSSLPort(), PortType.SSL);
    ConnectedChannel channel =
        getBlockingChannelBasedOnPortType(port, "localhost", clientSSLSocketFactory, clientSSLConfig);
    channel.connect();
    CountDownLatch latch = new CountDownLatch(1);
    DirectSender runnable =
        new DirectSender(cluster, channel, blobBackupCount, data, userMetadata, properties, null, latch);
    Thread threadToRun = new Thread(runnable);
    threadToRun.start();
    assertTrue("Did not put all blobs in 2 minutes", latch.await(2, TimeUnit.MINUTES));
    List<BlobId> blobIds = runnable.getBlobIds();
    for (BlobId blobId : blobIds) {
      notificationSystem.awaitBlobCreations(blobId.getID());
      if (doTtlUpdate) {
        updateBlobTtl(channel, blobId);
      }
    }

    // Start Helix Controller and ZK Server.
    int zkPort = 31999;
    String zkConnectString = "localhost:" + zkPort;
    String vcrClusterName = "vcrTestCluster";
    TestUtils.ZkInfo zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1, zkPort, true);
    HelixControllerManager helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, clusterMap);
    // Start the VCR and CloudBackupManager
    Properties props =
        VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString, 12310, 12410,
            vcrSSLProps);
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(blobIds);
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);

    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), clusterAgentsFactory, notificationSystem,
            cloudDestinationFactory);
    vcrServer.startup();

    // Waiting for backup done
    assertTrue("Did not backup all blobs in 2 minutes",
        latchBasedInMemoryCloudDestination.awaitUpload(2, TimeUnit.MINUTES));
    Map<String, CloudBlobMetadata> cloudBlobMetadataMap = latchBasedInMemoryCloudDestination.getBlobMetadata(blobIds);
    for (BlobId blobId : blobIds) {
      CloudBlobMetadata cloudBlobMetadata = cloudBlobMetadataMap.get(blobId.toString());
      assertNotNull("cloudBlobMetadata should not be null", cloudBlobMetadata);
      assertEquals("AccountId mismatch", accountId, cloudBlobMetadata.getAccountId());
      assertEquals("ContainerId mismatch", containerId, cloudBlobMetadata.getContainerId());
      assertEquals("Expiration time mismatch", Utils.Infinite_Time, cloudBlobMetadata.getExpirationTime());
      // TODO: verify other metadata and blob data
    }
    vcrServer.shutdown();
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  static void endToEndReplicationWithMultiNodeMultiPartitionTest(int interestedDataNodePortNumber, Port dataNode1Port,
      Port dataNode2Port, Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1,
      SSLConfig clientSSLConfig2, SSLConfig clientSSLConfig3, SSLSocketFactory clientSSLSocketFactory1,
      SSLSocketFactory clientSSLSocketFactory2, SSLSocketFactory clientSSLSocketFactory3,
      MockNotificationSystem notificationSystem, boolean testEncryption) throws Exception {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainTextPort
    MockClusterMap clusterMap = cluster.getClusterMap();
    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    List<AmbryServer> serverList = cluster.getServers();
    byte[] usermetadata = new byte[100];
    byte[] data = new byte[100];
    byte[] encryptionKey = null;
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties properties =
        new BlobProperties(100, "serviceid1", null, null, false, TestUtils.TTL_SECS, accountId, containerId, false,
            null);
    long expectedExpiryTimeMs = getExpiryTimeMs(properties);
    TestUtils.RANDOM.nextBytes(usermetadata);
    TestUtils.RANDOM.nextBytes(data);
    if (testEncryption) {
      encryptionKey = new byte[100];
      TestUtils.RANDOM.nextBytes(encryptionKey);
    }

    // connect to all the servers
    ConnectedChannel channel1 =
        getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
    ConnectedChannel channel2 =
        getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory2, clientSSLConfig2);
    ConnectedChannel channel3 =
        getBlockingChannelBasedOnPortType(dataNode3Port, "localhost", clientSSLSocketFactory3, clientSSLConfig3);

    // put all the blobs to random servers
    channel1.connect();
    channel2.connect();
    channel3.connect();

    int noOfParallelThreads = 3;
    CountDownLatch latch = new CountDownLatch(noOfParallelThreads);
    List<DirectSender> runnables = new ArrayList<DirectSender>(noOfParallelThreads);
    ConnectedChannel channel = null;
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
        new BlobProperties(properties.getBlobSize(), properties.getServiceId(), accountId, containerId, testEncryption);
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
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
        channel.send(getRequest);
        InputStream stream = channel.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        try {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          assertEquals(100, propertyOutput.getBlobSize());
          assertEquals("serviceid1", propertyOutput.getServiceId());
          assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
          assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          assertEquals("Expiration time mismatch (props)", expectedExpiryTimeMs, getExpiryTimeMs(propertyOutput));
          assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }

        // get user metadata
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        channel.send(getRequest);
        stream = channel.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          e.printStackTrace();
          fail();
        }

        // get blob
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        channel.send(getRequest);
        stream = channel.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobDataAndRelease(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          e.printStackTrace();
          fail();
        }

        // get blob all
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
        channel.send(getRequest);
        stream = channel.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          assertEquals("Expiration time mismatch (props)", expectedExpiryTimeMs,
              getExpiryTimeMs(blobAll.getBlobInfo().getBlobProperties()));
          assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
          byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
        } catch (MessageFormatException e) {
          e.printStackTrace();
          fail();
        }
      }
    }

    // ttl update all blobs and wait for replication
    Map<ConnectedChannel, List<BlobId>> channelToBlobIds = new HashMap<>();
    for (int i = 0; i < blobIds.size(); i++) {
      final BlobId blobId = blobIds.get(i);
      if (i % 3 == 0) {
        channelToBlobIds.computeIfAbsent(channel1, updateChannel -> new ArrayList<>()).add(blobId);
      } else if (i % 3 == 1) {
        channelToBlobIds.computeIfAbsent(channel2, updateChannel -> new ArrayList<>()).add(blobId);
      } else {
        channelToBlobIds.computeIfAbsent(channel3, updateChannel -> new ArrayList<>()).add(blobId);
      }
    }

    channelToBlobIds.entrySet().stream().map(entry -> CompletableFuture.supplyAsync(() -> {
      try {
        for (BlobId blobId : entry.getValue()) {
          updateBlobTtl(entry.getKey(), blobId);
        }
        return null;
      } catch (Throwable e) {
        throw new RuntimeException("Exception updating ttl for: " + entry, e);
      }
    })).forEach(CompletableFuture::join);

    // check that the TTL update has propagated
    blobIds.forEach(blobId -> notificationSystem.awaitBlobUpdates(blobId.getID(), UpdateType.TTL_UPDATE));
    // check all servers
    for (ConnectedChannel channelToUse : new ConnectedChannel[]{channel1, channel2, channel3}) {
      for (BlobId blobId : blobIds) {
        checkTtlUpdateStatus(channelToUse, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
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
            notificationSystem.decrementUpdatedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
                dataNode.getPort(), UpdateType.TTL_UPDATE);
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
        notificationSystem.awaitBlobUpdates(blobIds.get(j).getID(), UpdateType.TTL_UPDATE);
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
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          assertEquals(100, propertyOutput.getBlobSize());
          assertEquals("serviceid1", propertyOutput.getServiceId());
          assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
          assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get user metadata
      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get blob
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobDataAndRelease(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get blob all
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
        blobsDeleted.remove(blobIds.get(j));
        blobsChecked.add(blobIds.get(j));
      } else {
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
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
        notificationSystem.decrementDeletedReplica(blobIds.get(i).getID(), dataNode.getHostname(), dataNode.getPort());
      } else {
        notificationSystem.decrementCreatedReplica(blobIds.get(i).getID(), dataNode.getHostname(), dataNode.getPort());
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
        assertTrue(blobsChecked.contains(blobIds.get(j)));
      } else {
        try {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          assertEquals(100, propertyOutput.getBlobSize());
          assertEquals("serviceid1", propertyOutput.getServiceId());
          assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
          assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get user metadata
      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
      } else {
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get blob
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
      } else {
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobDataAndRelease(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
        } catch (MessageFormatException e) {
          fail();
        }
      }

      // get blob all
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest);
      stream = channel1.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
        blobsChecked.remove(blobIds.get(j));
        blobsDeleted.add(blobIds.get(j));
      } else {
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
        } catch (MessageFormatException e) {
          fail();
        }
      }
    }
    assertEquals(0, blobsChecked.size());

    short expectedLifeVersion = 1;
    for (int i = 0; i < 2; i ++) {
      expectedLifeVersion += i;
      // First undelete all deleted blobs
      for (BlobId deletedId: blobsDeleted){
        UndeleteRequest undeleteRequest = new UndeleteRequest(2, "reptest", deletedId, System.currentTimeMillis());
        channel3.send(undeleteRequest);
        InputStream undeleteResponseStream = channel3.receive().getInputStream();
        UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(new DataInputStream(undeleteResponseStream));
        assertEquals(ServerErrorCode.No_Error, undeleteResponse.getError());
        assertEquals(expectedLifeVersion, undeleteResponse.getLifeVersion());
      }

      Thread.sleep(5000);
      // Then use get request to get all the data back and make sure the lifeVersion is correct
      for (BlobId id: blobsDeleted) {
        // We don't need to wait for blob undeletes, since one of the hosts has Put Record deleted
        // from disk, so undelete this blob would end up replicating Put Record instead of undelete.
        // notificationSystem.awaitBlobUndeletes(id.toString());
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(id);
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(id.getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);

        // get blob all
        GetRequest getRequest =
            new GetRequest(1, "clientid20", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        assertEquals(ServerErrorCode.No_Error, resp.getError());
        assertEquals(1, resp.getPartitionResponseInfoList().size());
        assertEquals(ServerErrorCode.No_Error, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        assertEquals(1, resp.getPartitionResponseInfoList().get(0).getMessageInfoList().size());
        MessageInfo info = resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
        assertEquals(expectedLifeVersion, info.getLifeVersion());
        assertFalse(info.isDeleted());
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }

      for (BlobId id: blobsDeleted) {
        DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", id, System.currentTimeMillis());
        channel3.send(deleteRequest);
        InputStream deleteResponseStream = channel.receive().getInputStream();
        DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
        assertEquals(ServerErrorCode.No_Error, deleteResponse.getError());
      }

      Thread.sleep(1000);
      for (BlobId id: blobsDeleted) {
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(id);
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(id.getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);

        // get blob all
        GetRequest getRequest =
            new GetRequest(1, "clientid200", MessageFormatFlags.All, partitionRequestInfoList, GetOption.Include_All);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        assertEquals(ServerErrorCode.No_Error, resp.getError());
        assertEquals(1, resp.getPartitionResponseInfoList().size());
        assertEquals(ServerErrorCode.No_Error, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        assertEquals(1, resp.getPartitionResponseInfoList().get(0).getMessageInfoList().size());
        MessageInfo info = resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
        assertEquals(expectedLifeVersion, info.getLifeVersion());
        assertTrue(info.isDeleted());
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }
    }

    channel1.disconnect();
    channel2.disconnect();
    channel3.disconnect();
  }

  static void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest(String sourceDatacenter,
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
    AccountService accountService = new InMemAccountService(false, true);
    Router router = new NonBlockingRouterFactory(verifiableProperties, cluster.getClusterMap(), notificationSystem,
        getSSLFactoryIfRequired(verifiableProperties), accountService).getRouter();
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
          new BlobProperties(size, "service1", "owner id check", "image/jpeg", false, TestUtils.TTL_SECS, accountId,
              containerId, false, null);
      final byte[] metadata = new byte[new Random().nextInt(1000)];
      final byte[] blob = new byte[size];
      TestUtils.RANDOM.nextBytes(metadata);
      TestUtils.RANDOM.nextBytes(blob);
      Future<String> future =
          router.putBlob(properties, metadata, new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob)),
              new PutBlobOptionsBuilder().build(), new Callback<String>() {
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
      future.get(20, TimeUnit.SECONDS);
    }
    assertTrue("Did not receive all callbacks in time", callbackLatch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
    // put away for future use
    Payload payload1 = payloadQueue.peek();
    MockClusterMap clusterMap = cluster.getClusterMap();
    BlobId blobId1 = new BlobId(payload1.blobId, clusterMap);

    assertEquals("Did not put expected number of blobs", numberOfRequestsToSend, payloadQueue.size());
    Properties sslProps = new Properties();
    sslProps.putAll(routerProps);
    sslProps.setProperty("clustermap.ssl.enabled.datacenters", sslEnabledDatacenters);
    sslProps.setProperty("clustermap.cluster.name", "test");
    sslProps.setProperty("clustermap.datacenter.name", sourceDatacenter);
    sslProps.setProperty("clustermap.host.name", "localhost");
    sslProps.setProperty("connectionpool.read.timeout.ms", "15000");
    VerifiableProperties vProps = new VerifiableProperties(sslProps);
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(vProps), new SSLConfig(vProps),
            new ClusterMapConfig(vProps), new MetricRegistry());
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

    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    MockDataNodeId dataNodeId = clusterMap.getDataNodes().get(0);
    Port port = new Port(portType == PortType.PLAINTEXT ? dataNodeId.getPort() : dataNodeId.getSSLPort(), portType);
    ConnectedChannel channel = connectionPool.checkOutConnection("localhost", port, 10000);
    PartitionId partitionId = blobId1.getPartition();

    // stop the store via AdminRequest
    System.out.println("Begin to stop a BlobStore");
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, 1, "clientid2");
    BlobStoreControlAdminRequest controlRequest =
        new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StopStore, adminRequest);
    channel.send(controlRequest);
    InputStream stream = channel.receive().getInputStream();
    AdminResponse adminResponse = AdminResponse.readFrom(new DataInputStream(stream));
    assertEquals("Stop store admin request should succeed", ServerErrorCode.No_Error, adminResponse.getError());

    // put a blob on a stopped store, which should fail
    byte[] usermetadata = new byte[1000];
    byte[] data = new byte[3187];
    BlobProperties properties = new BlobProperties(3187, "serviceid1", accountId, containerId, false);
    BlobId blobId2 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        clusterMap.getLocalDatacenterId(), accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
    PutRequest putRequest2 =
        new PutRequest(1, "clientId2", blobId2, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, null);
    channel.send(putRequest2);
    InputStream putResponseStream = channel.receive().getInputStream();
    PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals("Put blob on stopped store should fail", ServerErrorCode.Replica_Unavailable, response2.getError());

    // get a blob properties on a stopped store, which should fail
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    ids.add(blobId1);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest1 =
        new GetRequest(1, "clientId1", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
    channel.send(getRequest1);
    stream = channel.receive().getInputStream();
    GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    assertEquals("Get blob properties on stopped store should fail", ServerErrorCode.Replica_Unavailable,
        resp1.getPartitionResponseInfoList().get(0).getErrorCode());

    // delete a blob on a stopped store, which should fail
    DeleteRequest deleteRequest = new DeleteRequest(1, "clientId1", blobId1, System.currentTimeMillis());
    channel.send(deleteRequest);
    stream = channel.receive().getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(stream));
    assertEquals("Delete blob on stopped store should fail", ServerErrorCode.Replica_Unavailable,
        deleteResponse.getError());

    // start the store via AdminRequest
    System.out.println("Begin to restart the BlobStore");
    adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, 1, "clientId");
    controlRequest = new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StartStore, adminRequest);
    channel.send(controlRequest);
    stream = channel.receive().getInputStream();
    adminResponse = AdminResponse.readFrom(new DataInputStream(stream));
    assertEquals("Start store admin request should succeed", ServerErrorCode.No_Error, adminResponse.getError());
    List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
    for (ReplicaId replicaId : replicaIds) {
      // forcibly mark replicas and disks as up.
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      mockReplicaId.markReplicaDownStatus(false);
      ((MockDiskId) mockReplicaId.getDiskId()).setDiskState(HardwareState.AVAILABLE, false);
    }

    // put a blob on a restarted store , which should succeed
    putRequest2 =
        new PutRequest(1, "clientId2", blobId2, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, null);
    channel.send(putRequest2);
    putResponseStream = channel.receive().getInputStream();
    response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals("Put blob on restarted store should succeed", ServerErrorCode.No_Error, response2.getError());
    // verify the put blob has been replicated successfully.
    notificationSystem.awaitBlobCreations(blobId2.getID());

    // get a blob on a restarted store , which should succeed
    ids = new ArrayList<BlobId>();
    ids.add(blobId2);
    partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest2 =
        new GetRequest(1, "clientId2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
    channel.send(getRequest2);
    stream = channel.receive().getInputStream();
    GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    InputStream responseStream = resp2.getInputStream();
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
    byte[] actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", data, actualBlobData);

    // delete a blob on a restarted store , which should succeed
    deleteRequest = new DeleteRequest(1, "clientId2", blobId2, System.currentTimeMillis());
    channel.send(deleteRequest);
    stream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(stream));
    assertEquals("Delete blob on restarted store should succeed", ServerErrorCode.No_Error, deleteResponse.getError());

    router.close();
    connectionPool.shutdown();
  }

  static void endToEndReplicationWithMultiNodeSinglePartitionTest(String routerDatacenter,
      int interestedDataNodePortNumber, Port dataNode1Port, Port dataNode2Port, Port dataNode3Port, MockCluster cluster,
      SSLConfig clientSSLConfig1, SSLSocketFactory clientSSLSocketFactory1, MockNotificationSystem notificationSystem,
      Properties routerProps, boolean testEncryption) {
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
      PartitionId partition = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);

      for (int i = 0; i < 11; i++) {
        short accountId = Utils.getRandomShort(TestUtils.RANDOM);
        short containerId = Utils.getRandomShort(TestUtils.RANDOM);
        propertyList.add(
            new BlobProperties(1000, "serviceid1", null, null, false, TestUtils.TTL_SECS, accountId, containerId,
                testEncryption, null));
        blobIdList.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
            clusterMap.getLocalDatacenterId(), accountId, containerId, partition, false,
            BlobId.BlobDataType.DATACHUNK));
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
              Unpooled.wrappedBuffer(dataList.get(0)), propertyList.get(0).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(0) != null ? ByteBuffer.wrap(encryptionKeyList.get(0)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(0), blobIdList.get(0),
          encryptionKeyList.get(0) != null ? ByteBuffer.wrap(encryptionKeyList.get(0)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(0));

      ConnectedChannel channel1 =
          getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      ConnectedChannel channel2 =
          getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      ConnectedChannel channel3 =
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
              Unpooled.wrappedBuffer(dataList.get(1)), propertyList.get(1).getBlobSize(), BlobType.DataBlob,
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
              Unpooled.wrappedBuffer(dataList.get(2)), propertyList.get(2).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(3)), propertyList.get(3).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(4)), propertyList.get(4).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(5)), propertyList.get(5).getBlobSize(), BlobType.DataBlob,
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

      checkTtlUpdateStatus(channel3, clusterMap, blobIdFactory, blobIdList.get(5), dataList.get(5), false,
          getExpiryTimeMs(propertyList.get(5)));
      updateBlobTtl(channel3, blobIdList.get(5));
      expectedTokenSize += getUpdateRecordSize(blobIdList.get(5), SubRecord.Type.TTL_UPDATE);
      checkTtlUpdateStatus(channel3, clusterMap, blobIdFactory, blobIdList.get(5), dataList.get(5), true,
          Utils.Infinite_Time);
      notificationSystem.awaitBlobUpdates(blobIdList.get(5).getID(), UpdateType.TTL_UPDATE);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId mockPartitionId =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
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
        fail();
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
        assertArrayEquals(usermetadata, userMetadataOutput.array());
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
        fail();
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
        byte[] blobout = getBlobDataAndRelease(blobData);
        assertArrayEquals(dataList.get(0), blobout);
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
        fail();
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
        byte[] blobout = getBlobDataAndRelease(blobAll.getBlobData());
        assertArrayEquals(dataList.get(0), blobout);
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null", blobAll.getBlobEncryptionKey());
          assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(0), blobAll.getBlobEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null", blobAll.getBlobEncryptionKey());
        }
      } catch (MessageFormatException e) {
        fail();
      }

      if (!testEncryption) {
        // get blob data
        // Use router to get the blob
        Properties routerProperties = getRouterProps(routerDatacenter);
        routerProperties.putAll(routerProps);
        VerifiableProperties routerVerifiableProperties = new VerifiableProperties(routerProperties);
        AccountService accountService = new InMemAccountService(false, true);
        Router router = new NonBlockingRouterFactory(routerVerifiableProperties, clusterMap, notificationSystem,
            getSSLFactoryIfRequired(routerVerifiableProperties), accountService).getRouter();
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
      mockPartitionId =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), propertyList.get(0).getAccountId(), propertyList.get(0).getContainerId(),
          mockPartitionId, false, BlobId.BlobDataType.DATACHUNK));
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
      expectedTokenSize += getUpdateRecordSize(blobIdList.get(0), SubRecord.Type.DELETE);
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
      checkReplicaTokens(clusterMap, dataNodeId,
          expectedTokenSize - getUpdateRecordSize(blobIdList.get(0), SubRecord.Type.DELETE), "0");

      // Shut down server 1
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();
      // Add more data to server 2 and server 3. Recover server 1 and ensure it is completely replicated
      // put blob 7
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(6), propertyList.get(6), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(6)), propertyList.get(6).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(7)), propertyList.get(7).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(8)), propertyList.get(8).getBlobSize(), BlobType.DataBlob,
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
          Unpooled.wrappedBuffer(dataList.get(9)), propertyList.get(9).getBlobSize(), BlobType.DataBlob,
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
              Unpooled.wrappedBuffer(dataList.get(10)), propertyList.get(10).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(10), blobIdList.get(10),
          encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(10));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      checkTtlUpdateStatus(channel2, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), false,
          getExpiryTimeMs(propertyList.get(10)));
      updateBlobTtl(channel2, blobIdList.get(10));
      expectedTokenSize += getUpdateRecordSize(blobIdList.get(10), SubRecord.Type.TTL_UPDATE);
      checkTtlUpdateStatus(channel2, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), true,
          Utils.Infinite_Time);

      cluster.getServers().get(0).startup();
      // wait for server to recover
      notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(9).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(10).getID());
      notificationSystem.awaitBlobUpdates(blobIdList.get(10).getID(), UpdateType.TTL_UPDATE);
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
        fail();
      }

      // check that the ttl update went through
      checkTtlUpdateStatus(channel1, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), true,
          Utils.Infinite_Time);

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
      notificationSystem.decrementUpdatedReplica(blobIdList.get(5).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort(), UpdateType.TTL_UPDATE);
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
      notificationSystem.decrementUpdatedReplica(blobIdList.get(10).getID(), dataNodeId.getHostname(),
          dataNodeId.getPort(), UpdateType.TTL_UPDATE);

      cluster.getServers().get(0).startup();
      notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(3).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(5).getID());
      notificationSystem.awaitBlobUpdates(blobIdList.get(5).getID(), UpdateType.TTL_UPDATE);
      notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(9).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(10).getID());
      notificationSystem.awaitBlobUpdates(blobIdList.get(10).getID(), UpdateType.TTL_UPDATE);

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
        fail();
      }

      // check that the ttl updates are present
      checkTtlUpdateStatus(channel1, clusterMap, blobIdFactory, blobIdList.get(5), dataList.get(5), true,
          Utils.Infinite_Time);
      checkTtlUpdateStatus(channel1, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), true,
          Utils.Infinite_Time);

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
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
    try {
      return MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.getCurrentMessageHeaderVersion())
          + blobId.sizeInBytes() + (blobEncryptionKey != null
          ? MessageFormatRecord.BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(blobEncryptionKey) : 0)
          + +MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(properties)
          + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(usermetadata)
          + MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(data.length);
    } catch (Exception e) {
      fail("Unexpected exception" + e);
    }
    return 0;
  }

  /**
   * @param blobId the blob ID being updated
   * @param updateType the type of update
   * @return the size of the update record in the log
   */
  private static long getUpdateRecordSize(BlobId blobId, SubRecord.Type updateType) {
    try {
      return MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.getCurrentMessageHeaderVersion())
          + blobId.sizeInBytes() + MessageFormatRecord.Update_Format_V3.getRecordSize(updateType);
    } catch (Exception e) {
      fail("Unexpected exception" + e);
    }
    return 0;
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
            assertEquals(1, version);

            while (dataInputStream.available() > 8) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(dataInputStream);
              // read remote node host name
              String hostname = Utils.readIntString(dataInputStream);
              // read remote replica path
              Utils.readIntString(dataInputStream);

              // read remote port
              int port = dataInputStream.readInt();
              assertTrue(setToCheck.contains(partitionId.toString() + hostname + port));
              setToCheck.remove(partitionId.toString() + hostname + port);
              // read total bytes read from local store
              dataInputStream.readLong();
              // read replica type
              ReplicaType replicaType = ReplicaType.values()[dataInputStream.readShort()];
              // read replica token
              StoreFindToken token = (StoreFindToken) factory.getFindToken(dataInputStream);
              System.out.println(
                  "partitionId " + partitionId + " hostname " + hostname + " port " + port + " token " + token);
              Offset endTokenOffset = token.getOffset();
              long parsedToken = endTokenOffset == null ? -1 : endTokenOffset.getOffset();
              System.out.println("The parsed token is " + parsedToken);
              if (partitionId.isEqual(targetPartition)) {
                assertFalse("Parsed offset: " + parsedToken + " must not be larger than target value: " + targetOffset,
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
            fail();
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
      fail("Could not find target token offset: " + targetOffset);
    }
  }

  private static void checkBlobId(Router router, BlobId blobId, byte[] data) throws Exception {
    GetBlobResult result =
        router.getBlob(blobId.getID(), new GetBlobOptionsBuilder().build()).get(20, TimeUnit.SECONDS);
    ReadableStreamChannel blob = result.getBlobDataChannel();
    assertEquals("Size does not match that of data", data.length,
        result.getBlobInfo().getBlobProperties().getBlobSize());
    RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();
    blob.readInto(channel, null).get(1, TimeUnit.SECONDS);

    try (InputStream is = channel.consumeContentAsInputStream()) {
      assertArrayEquals(data, Utils.readBytesFromStream(is, (int) channel.getBytesWritten()));
    }
  }

  private static void checkBlobContent(MockClusterMap clusterMap, BlobId blobId, ConnectedChannel channel,
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
    byte[] blobout = getBlobDataAndRelease(blobData);
    assertArrayEquals(dataToCheck, blobout);
    if (encryptionKey != null) {
      assertNotNull("EncryptionKey should not have been null",
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
      assertArrayEquals("EncryptionKey mismatch", encryptionKey,
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
    } else {
      assertNull("EncryptionKey should have been null",
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
   * Updates the TTL of the given {@code blobId}
   * @param channel the {@link ConnectedChannel} to make the {@link GetRequest} on.
   * @param blobId the ID of the blob to TTL update
   * @throws IOException
   */
  static void updateBlobTtl(ConnectedChannel channel, BlobId blobId) throws IOException {
    TtlUpdateRequest ttlUpdateRequest =
        new TtlUpdateRequest(1, "updateBlobTtl", blobId, Utils.Infinite_Time, SystemTime.getInstance().milliseconds());
    channel.send(ttlUpdateRequest);
    InputStream stream = channel.receive().getInputStream();
    TtlUpdateResponse ttlUpdateResponse = TtlUpdateResponse.readFrom(new DataInputStream(stream));
    assertEquals("Unexpected ServerErrorCode for TtlUpdateRequest", ServerErrorCode.No_Error,
        ttlUpdateResponse.getError());
  }

  /**
   * Checks the TTL update status of the given {@code blobId} based on the args provided
   * @param channel the {@link ConnectedChannel} to make the {@link GetRequest} on.
   * @param clusterMap the {@link ClusterMap} to use
   * @param storeKeyFactory the {@link StoreKeyFactory} to use
   * @param blobId the ID of the blob to check
   * @param expectedBlobData the expected blob data
   * @param ttlUpdated {@code true} if the blob has been ttl updated
   * @param expectedExpiryTimeMs the expected expiry time (in ms)
   * @throws IOException
   * @throws MessageFormatException
   */
  static void checkTtlUpdateStatus(ConnectedChannel channel, ClusterMap clusterMap, StoreKeyFactory storeKeyFactory,
      BlobId blobId, byte[] expectedBlobData, boolean ttlUpdated, long expectedExpiryTimeMs)
      throws IOException, MessageFormatException {
    PartitionRequestInfo requestInfo =
        new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
    List<PartitionRequestInfo> requestInfos = Collections.singletonList(requestInfo);

    // blob properties
    GetRequest request =
        new GetRequest(1, "checkTtlUpdateStatus", MessageFormatFlags.BlobProperties, requestInfos, GetOption.None);
    channel.send(request);
    InputStream stream = channel.receive().getInputStream();
    GetResponse response = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    BlobProperties blobProperties = MessageFormatRecord.deserializeBlobProperties(response.getInputStream());
    if (!ttlUpdated) {
      assertEquals("TTL does not match", expectedExpiryTimeMs, getExpiryTimeMs(blobProperties));
    }
    MessageInfo messageInfo = response.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    assertEquals("Blob ID not as expected", blobId, messageInfo.getStoreKey());
    assertEquals("TTL update state not as expected", ttlUpdated, messageInfo.isTtlUpdated());
    assertEquals("Expiry time is not as expected", expectedExpiryTimeMs, messageInfo.getExpirationTimeInMs());

    // blob all
    request = new GetRequest(1, "checkTtlUpdateStatus", MessageFormatFlags.All, requestInfos, GetOption.None);
    channel.send(request);
    stream = channel.receive().getInputStream();
    response = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    InputStream responseStream = response.getInputStream();
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, storeKeyFactory);
    byte[] actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", expectedBlobData, actualBlobData);
    messageInfo = response.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    assertEquals("Blob ID not as expected", blobId, messageInfo.getStoreKey());
    assertEquals("TTL update state not as expected", ttlUpdated, messageInfo.isTtlUpdated());
    assertEquals("Expiry time is not as expected", expectedExpiryTimeMs, messageInfo.getExpirationTimeInMs());
  }

  /**
   * Gets the expiry time (in ms) as stored by the {@link com.github.ambry.store.BlobStore}.
   * @param blobProperties the properties of the blob
   * @return the expiry time (in ms)
   */
  static long getExpiryTimeMs(BlobProperties blobProperties) {
    return Utils.getTimeInMsToTheNearestSec(
        Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds()));
  }

  /**
   * Returns BlockingChannel, SSLBlockingChannel or Http2BlockingChannel depending on whether the port type is PlainText,
   * SSL or HTTP2 port for the given targetPort
   * @param targetPort upon which connection has to be established
   * @param hostName upon which connection has to be established
   * @return ConnectedChannel
   */
  public static ConnectedChannel getBlockingChannelBasedOnPortType(Port targetPort, String hostName,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    ConnectedChannel channel = null;
    if (targetPort.getPortType() == PortType.PLAINTEXT) {
      channel = new BlockingChannel(hostName, targetPort.getPort(), 10000, 10000, 10000, 2000);
    } else if (targetPort.getPortType() == PortType.SSL) {
      channel = new SSLBlockingChannel(hostName, targetPort.getPort(), new MetricRegistry(), 10000, 10000, 10000, 4000,
          sslSocketFactory, sslConfig);
    } else if (targetPort.getPortType() == PortType.HTTP2) {
      channel = new Http2BlockingChannel(hostName, targetPort.getPort(), sslConfig);
    }
    return channel;
  }

  /**
   * Create an {@link SSLFactory} if there are SSL enabled datacenters in the properties
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @return an {@link SSLFactory}, or {@code null}, if no {@link SSLFactory} is required.
   * @throws Exception
   */
  static SSLFactory getSSLFactoryIfRequired(VerifiableProperties verifiableProperties) throws Exception {
    if (new RouterConfig(verifiableProperties).routerEnableHttp2NetworkClient) {
      return new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    }
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    boolean requiresSSL = clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0;
    return requiresSSL ? SSLFactory.getNewInstance(new SSLConfig(verifiableProperties)) : null;
  }

  /**
   * Create {@link PutMessageFormatInputStream} for a blob with given {@link BlobId} and update {@code blobIdToSizeMap}.
   * @param blobId {@link BlobId} object.
   * @param blobSize size of blob.
   * @param blobIdToSizeMap {@link Map} of {@link BlobId} to size of blob uploaded.
   * @return {@link PutMessageFormatInputStream} object.
   * @throws Exception
   */
  static PutMessageFormatInputStream getPutMessageInputStreamForBlob(BlobId blobId, int blobSize,
      Map<BlobId, Integer> blobIdToSizeMap, short accountId, short containerId) throws Exception {
    int userMetaDataSize = 100;
    byte[] userMetadata = new byte[userMetaDataSize];
    TestUtils.RANDOM.nextBytes(userMetadata);
    byte[] data = new byte[blobSize];
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, Utils.Infinite_Time, accountId, containerId,
            false, null);
    TestUtils.RANDOM.nextBytes(data);
    blobIdToSizeMap.put(blobId, blobSize);
    return new PutMessageFormatInputStream(blobId, null, blobProperties, ByteBuffer.wrap(userMetadata),
        new ByteBufferInputStream(ByteBuffer.wrap(data)), blobSize);
  }

  /**
   * Create {@code blobCount} number of {@link BlobId}s.
   * @param blobCount number of {@link BlobId}s to create.
   * @return list of {@link BlobId}s
   */
  static List<BlobId> createBlobIds(int blobCount, ClusterMap clusterMap, short accountId, short containerId,
      PartitionId partitionId) {
    List<BlobId> blobIds = new ArrayList<>(blobCount);
    for (int i = 0; i < blobCount; i++) {
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
      blobIds.add(blobId);
    }
    return blobIds;
  }
}
