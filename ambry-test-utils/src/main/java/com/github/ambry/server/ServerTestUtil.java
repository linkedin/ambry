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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
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
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.MysqlRepairRequestsDbConfig;
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
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.AdminResponseWithContent;
import com.github.ambry.protocol.BlobIndexAdminRequest;
import com.github.ambry.protocol.BlobStoreControlAction;
import com.github.ambry.protocol.BlobStoreControlAdminRequest;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.ForceDeleteAdminRequest;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.ReplicateBlobRequest;
import com.github.ambry.protocol.ReplicateBlobResponse;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.repair.MysqlRepairRequestsDb;
import com.github.ambry.repair.MysqlRepairRequestsDbFactory;
import com.github.ambry.repair.RepairRequestRecord;
import com.github.ambry.repair.RepairRequestsDb;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockId;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreKeyJacksonConfig;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSocketFactory;
import javax.sql.DataSource;
import org.junit.Assert;

import static org.junit.Assert.*;


public final class ServerTestUtil {
  private static final QuotaChargeCallback QUOTA_CHARGE_EVENT_LISTENER = QuotaTestUtils.createTestQuotaChargeCallback();

  static byte[] getBlobData(BlobData blobData) {
    byte[] actualBlobData = new byte[(int) blobData.getSize()];
    ByteBuf buffer = blobData.content();
    buffer.readBytes(actualBlobData);
    return actualBlobData;
  }

  static void endToEndTest(Port targetPort, String routerDatacenter, MockCluster cluster, SSLConfig clientSSLConfig,
      SSLSocketFactory clientSSLSocketFactory, Properties routerProps, boolean testEncryption) {
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      byte[] userMetadata = new byte[1000];
      byte[] data = new byte[31870];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);

      BlobProperties properties =
          new BlobProperties(31870, "serviceid1", accountId, containerId, testEncryption, cluster.time.milliseconds());
      TestUtils.RANDOM.nextBytes(userMetadata);
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
      BlobId blobId6 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);

      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      ConnectedChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      DataInputStream putResponseStream = channel.sendAndReceive(putRequest).getInputStream();
      PutResponse response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());

      // put blob 2 with an expiry time and apply TTL update later
      BlobProperties propertiesForTtlUpdate =
          new BlobProperties(31870, "serviceid1", "ownerid", "image/png", false, TestUtils.TTL_SECS,
              cluster.time.milliseconds(), accountId, containerId, testEncryption, null, null, null, null);
      long ttlUpdateBlobExpiryTimeMs = getExpiryTimeMs(propertiesForTtlUpdate);
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobId2, propertiesForTtlUpdate, ByteBuffer.wrap(userMetadata),
              Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
              testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest2).getInputStream();
      PutResponse response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest3).getInputStream();
      PutResponse response3 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response3.getError());
      // put blob 4 that is expired
      BlobProperties propertiesExpired =
          new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0, cluster.time.milliseconds(), accountId,
              containerId, testEncryption, null, null, null, null);
      PutRequest putRequest4 = new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest4).getInputStream();
      PutResponse response4 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response4.getError());
      cluster.time.sleep(10000);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<>();
      MockPartitionId partition =
          (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(blobId1);
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      DataInputStream stream = channel.sendAndReceive(getRequest1).getInputStream();
      GetResponse resp1 = GetResponse.readFrom(stream, clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
        releaseNettyBufUnderneathStream(stream);
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
      stream = channel.sendAndReceive(getRequest1).getInputStream();
      resp1 = GetResponse.readFrom(stream, clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
        releaseNettyBufUnderneathStream(stream);
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
      DataInputStream streamExpired = channel.sendAndReceive(getRequestExpired).getInputStream();
      GetResponse respExpired = GetResponse.readFrom(streamExpired, clusterMap);
      Assert.assertEquals(ServerErrorCode.BlobExpired,
          respExpired.getPartitionResponseInfoList().get(0).getErrorCode());
      releaseNettyBufUnderneathStream(streamExpired);
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
      streamExpired = channel.sendAndReceive(getRequestExpired).getInputStream();
      respExpired = GetResponse.readFrom(streamExpired, clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(respExpired.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("ownerid", propertyOutput.getOwnerId());
        assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
        assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
        releaseNettyBufUnderneathStream(streamExpired);
      } catch (MessageFormatException e) {
        fail();
      }
      // get user metadata
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest2).getInputStream();
      GetResponse resp2 = GetResponse.readFrom(stream, clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        assertArrayEquals(userMetadata, userMetadataOutput.array());
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
        releaseNettyBufUnderneathStream(stream);
      } catch (MessageFormatException e) {
        fail();
      }
      // get blob info
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest3).getInputStream();
      GetResponse resp3 = GetResponse.readFrom(stream, clusterMap);
      InputStream responseStream = resp3.getInputStream();
      // verify blob properties.
      BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(responseStream);
      assertEquals(31870, propertyOutput.getBlobSize());
      assertEquals("serviceid1", propertyOutput.getServiceId());
      assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
      assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
      // verify user metadata
      ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(responseStream);
      assertArrayEquals(userMetadata, userMetadataOutput.array());
      if (testEncryption) {
        assertNotNull("MessageMetadata should not have been null",
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
      } else {
        assertNull("MessageMetadata should have been null",
            resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob all
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest4).getInputStream();
      GetResponse resp4 = GetResponse.readFrom(stream, clusterMap);
      responseStream = resp4.getInputStream();
      BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      byte[] actualBlobData = getBlobData(blobAll.getBlobData());
      // verify content
      assertArrayEquals("Content mismatch.", data, actualBlobData);
      if (testEncryption) {
        assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
        assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
      } else {
        assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
      }
      releaseNettyBufUnderneathStream(stream);
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
      updateBlobTtl(channel, blobId2, cluster.time.milliseconds());
      checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId2, data, true, Utils.Infinite_Time);

      // Now get the blob index values
      BlobIndexAdminRequest blobIndexAdminRequest = new BlobIndexAdminRequest(blobId2,
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      AdminResponseWithContent adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(new String(adminResponseWithContent.getContent()), ServerErrorCode.NoError,
          adminResponseWithContent.getError());
      byte[] jsonBytes = adminResponseWithContent.getContent();
      ObjectMapper objectMapper = new ObjectMapper();
      StoreKeyJacksonConfig.setupObjectMapper(objectMapper, new BlobIdFactory(clusterMap));
      Map<String, MessageInfo> messages =
          objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
          });
      // We should have two message infos
      // first one is the creation of the blob id2 and second one if the ttl update for the blob id2
      assertEquals(2, messages.size());
      TreeMap<Long, MessageInfo> sortedMessages = new TreeMap<>();
      for (String key : messages.keySet()) {
        // Key is offset
        sortedMessages.put(Long.parseLong(key.split("_")[2]), messages.get(key));
      }
      List<MessageInfo> sortedMessageList = new ArrayList<>(sortedMessages.values());
      // First one should be PUT
      assertEquals(false, sortedMessageList.get(0).isDeleted());
      assertEquals(new String(jsonBytes), false, sortedMessageList.get(0).isTtlUpdated());
      assertEquals(false, sortedMessageList.get(0).isUndeleted());
      // Second one should be ttlupdate
      assertEquals(false, sortedMessageList.get(1).isDeleted());
      assertEquals(new String(jsonBytes), true, sortedMessageList.get(1).isTtlUpdated());
      assertEquals(false, sortedMessageList.get(1).isUndeleted());

      // Do this test for blob id 5, which is not uploaded yet. we should be getting empty list, without error
      blobIndexAdminRequest = new BlobIndexAdminRequest(blobId5,
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      jsonBytes = adminResponseWithContent.getContent();
      messages = objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
      });
      assertEquals(0, messages.size());

      // Do this test for a fake partition
      MockPartitionId fakePartition = new MockPartitionId(123456, "FAKE");
      BlobId fakeBlobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), fakePartition, false, BlobId.BlobDataType.DATACHUNK);
      blobIndexAdminRequest = new BlobIndexAdminRequest(fakeBlobId,
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, fakePartition, 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.BadRequest, adminResponseWithContent.getError());

      // Do this test for a mal-formatted blobid
      blobIndexAdminRequest = new BlobIndexAdminRequest(new MockId("malformat-id"),
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.BadRequest, adminResponseWithContent.getError());

      //
      // Start the test of ForceDeleteAdminRequest
      //
      // Force delete on a blob which doesn't exist.
      short lifeVersion = 1;
      long nowTime = System.currentTimeMillis();
      ForceDeleteAdminRequest forceDeleteAdminRequest = new ForceDeleteAdminRequest(blobId6, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      AdminResponse adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, adminResponse.getError());
      // Use BlobIndexAdminRequest to verify we have one message info.
      blobIndexAdminRequest = new BlobIndexAdminRequest(blobId6,
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(new String(adminResponseWithContent.getContent()), ServerErrorCode.NoError,
          adminResponseWithContent.getError());
      jsonBytes = adminResponseWithContent.getContent();
      messages = objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
      });
      // We should have one message info
      assertEquals(1, messages.size());
      MessageInfo deleteRecordInfo = messages.values().stream().findFirst().get();
      // it is delete tombstone
      assertEquals(accountId, deleteRecordInfo.getAccountId());
      assertEquals(containerId, deleteRecordInfo.getContainerId());
      assertEquals(true, deleteRecordInfo.isDeleted());
      assertEquals(false, deleteRecordInfo.isTtlUpdated());
      assertEquals(false, deleteRecordInfo.isUndeleted());
      assertEquals(false, deleteRecordInfo.isExpired());
      assertEquals(-1, deleteRecordInfo.getExpirationTimeInMs());
      assertEquals(lifeVersion, deleteRecordInfo.getLifeVersion());
      assertTrue(deleteRecordInfo.getOperationTimeMs() >= Utils.getTimeInMsToTheNearestSec(nowTime));
      assertEquals(null, deleteRecordInfo.getCrc());

      // Do this test for blob id 1, it's a PutBlob. Force delete should return Blob_Already_Exists
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(blobId1, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.BlobAlreadyExists, adminResponse.getError());

      // Do this test for a fake partition
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(fakeBlobId, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, fakePartition, 1, "clientid2"));
      stream = channel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.BadRequest, adminResponse.getError());

      // Do this test for a mal-formatted blobid
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(new MockId("malformat-id"), lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      stream = channel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.BadRequest, adminResponse.getError());
      //
      // End the test of forceDeleteAdminRequest
      //

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), properties.getAccountId(), properties.getContainerId(), partition, false,
          BlobId.BlobDataType.DATACHUNK));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest5).getInputStream();
      GetResponse resp5 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals(ServerErrorCode.BlobNotFound, resp5.getPartitionResponseInfoList().get(0).getErrorCode());
      releaseNettyBufUnderneathStream(stream);
      // stop the store via AdminRequest
      System.out.println("Begin to stop a BlobStore");
      AdminRequest adminRequest =
          new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionIds.get(0), 1, "clientid2");
      BlobStoreControlAdminRequest controlRequest =
          new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StopStore, adminRequest);
      stream = channel.sendAndReceive(controlRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Stop store admin request should succeed", ServerErrorCode.NoError, adminResponse.getError());

      // put a blob on a stopped store, which should fail
      putRequest =
          new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest).getInputStream();
      response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals("Put blob on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
          response.getError());

      // get a blob properties on a stopped store, which should fail
      ids = new ArrayList<>();
      partition = (MockPartitionId) blobId1.getPartition();
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<>();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest1).getInputStream();
      resp1 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals("Get blob properties on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
          resp1.getPartitionResponseInfoList().get(0).getErrorCode());
      releaseNettyBufUnderneathStream(stream);
      // delete a blob on a stopped store, which should fail
      DeleteRequest deleteRequest = new DeleteRequest(1, "deleteClient", blobId1, System.currentTimeMillis());
      stream = channel.sendAndReceive(deleteRequest).getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Delete blob on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
          deleteResponse.getError());
      // get the blobIndex from a stopped
      blobIndexAdminRequest = new BlobIndexAdminRequest(blobId2,
          new AdminRequest(AdminRequestOrResponseType.BlobIndex, blobId2.getPartition(), 1, "clientid2"));
      stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
      adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.ReplicaUnavailable, adminResponseWithContent.getError());

      // force delete a blob from a stopped store
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(blobId2, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, blobId2.getPartition(), 1, "clientid2"));
      stream = channel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.ReplicaUnavailable, adminResponse.getError());

      // start the store via AdminRequest
      System.out.println("Begin to restart the BlobStore");
      adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionIds.get(0), 1, "clientid2");
      controlRequest = new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StartStore, adminRequest);
      stream = channel.sendAndReceive(controlRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Start store admin request should succeed", ServerErrorCode.NoError,
          adminResponse.getError());
      List<? extends ReplicaId> replicaIds = partitionIds.get(0).getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        // forcibly mark replicas and disks as up.
        MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
        mockReplicaId.markReplicaDownStatus(false);
        ((MockDiskId) mockReplicaId.getDiskId()).setDiskState(HardwareState.AVAILABLE, false);
      }

      // put a blob on a restarted store , which should succeed
      PutRequest putRequest5 =
          new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest5).getInputStream();
      PutResponse response5 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals("Put blob on restarted store should succeed", ServerErrorCode.NoError, response5.getError());

      // get a blob on a restarted store , which should succeed
      ids = new ArrayList<>();
      PartitionId partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<>();
      partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest1).getInputStream();
      resp1 = GetResponse.readFrom(stream, clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobData(blobAll.getBlobData());
      assertArrayEquals("Content mismatch.", data, actualBlobData);
      releaseNettyBufUnderneathStream(stream);
      // undelete a not-deleted blob should return fail
      UndeleteRequest undeleteRequest = new UndeleteRequest(1, "undeleteClient", blobId1, System.currentTimeMillis());
      stream = channel.sendAndReceive(undeleteRequest).getInputStream();
      UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Undelete blob should succeed", ServerErrorCode.BlobNotDeleted, undeleteResponse.getError());

      // delete a blob on a restarted store , which should succeed
      deleteRequest = new DeleteRequest(1, "deleteClient", blobId1, System.currentTimeMillis());
      stream = channel.sendAndReceive(deleteRequest).getInputStream();
      deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Delete blob on restarted store should succeed", ServerErrorCode.NoError,
          deleteResponse.getError());

      // undelete a deleted blob, which should succeed
      undeleteRequest = new UndeleteRequest(2, "undeleteClient", blobId1, System.currentTimeMillis());
      stream = channel.sendAndReceive(undeleteRequest).getInputStream();
      undeleteResponse = UndeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Undelete blob should succeed", ServerErrorCode.NoError, undeleteResponse.getError());
      Assert.assertEquals("Undelete life version mismatch", undeleteResponse.getLifeVersion(), (short) 1);

      // undelete an already undeleted blob, which should fail
      undeleteRequest = new UndeleteRequest(3, "undeleteClient", blobId1, System.currentTimeMillis());
      stream = channel.sendAndReceive(undeleteRequest).getInputStream();
      undeleteResponse = UndeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Undelete blob should fail", ServerErrorCode.BlobAlreadyUndeleted,
          undeleteResponse.getError());
      Assert.assertEquals("LifeVersion Mismatch", (short) 1, undeleteResponse.getLifeVersion());

      // get an undeleted blob, which should succeed
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest1).getInputStream();
      resp1 = GetResponse.readFrom(stream, clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobData(blobAll.getBlobData());
      assertArrayEquals("Content mismatch", data, actualBlobData);
      releaseNettyBufUnderneathStream(stream);
      // Bounce servers to make them read the persisted token file.
      cluster.stopServers();
      cluster.reinitServers();

      channel.disconnect();
      channel.connect();
      // get an undeleted blob after restart, which should succeed
      getRequest1 = new GetRequest(1, "clientid1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel.sendAndReceive(getRequest1).getInputStream();
      resp1 = GetResponse.readFrom(stream, clusterMap);
      responseStream = resp1.getInputStream();
      blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
      actualBlobData = getBlobData(blobAll.getBlobData());
      assertArrayEquals("Content mismatch", data, actualBlobData);
      releaseNettyBufUnderneathStream(stream);
      channel.disconnect();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      assertNull(e.getMessage() + "\n" + sw, e);
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
    DataInputStream putResponseStream = channelToDatanode1.sendAndReceive(latePutRequest1).getInputStream();
    PutResponse response = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals(expectedErrorCode, response.getError());

    putResponseStream = channelToDatanode2.sendAndReceive(latePutRequest2).getInputStream();
    response = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals(expectedErrorCode, response.getError());

    putResponseStream = channelToDatanode3.sendAndReceive(latePutRequest3).getInputStream();
    response = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals(expectedErrorCode, response.getError());
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
        new BlobProperties(100, "serviceid1", null, null, false, TestUtils.TTL_SECS, cluster.time.milliseconds(),
            accountId, containerId, false, null, null, null, null);
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
    int totalBlobsToPut = 50;
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
          new DirectSender(cluster, channel, totalBlobsToPut, data, usermetadata, properties, encryptionKey, latch);
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
        ServerErrorCode.NoError);
    // Test the case where a put arrives with the same id as one in the server, but the blob is not identical.
    BlobProperties differentProperties =
        new BlobProperties(properties.getBlobSize(), properties.getServiceId(), accountId, containerId, testEncryption,
            cluster.time.milliseconds());
    testLatePutRequest(blobIds.get(0), differentProperties, usermetadata, data, encryptionKey, channel1, channel2,
        channel3, ServerErrorCode.BlobAlreadyExists);
    byte[] differentUserMetadata = Arrays.copyOf(usermetadata, usermetadata.length);
    differentUserMetadata[0] = (byte) ~differentUserMetadata[0];
    testLatePutRequest(blobIds.get(0), properties, differentUserMetadata, data, encryptionKey, channel1, channel2,
        channel3, ServerErrorCode.BlobAlreadyExists);
    byte[] differentData = Arrays.copyOf(data, data.length);
    differentData[0] = (byte) ~differentData[0];
    testLatePutRequest(blobIds.get(0), properties, usermetadata, differentData, encryptionKey, channel1, channel2,
        channel3, ServerErrorCode.BlobAlreadyExists);

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
        DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
        GetResponse resp = GetResponse.readFrom(stream, clusterMap);
        try {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          assertEquals(100, propertyOutput.getBlobSize());
          assertEquals("serviceid1", propertyOutput.getServiceId());
          assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
          assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          assertEquals("Expiration time mismatch (props)", expectedExpiryTimeMs, getExpiryTimeMs(propertyOutput));
          Assert.assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
          releaseNettyBufUnderneathStream(stream);
        } catch (MessageFormatException e) {
          fail();
        }
        // get user metadata
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        stream = channel.sendAndReceive(getRequest).getInputStream();
        resp = GetResponse.readFrom(stream, clusterMap);
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          Assert.assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          e.printStackTrace();
          fail();
        }
        releaseNettyBufUnderneathStream(stream);
        // get blob
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        stream = channel.sendAndReceive(getRequest).getInputStream();
        resp = GetResponse.readFrom(stream, clusterMap);
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobData(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          Assert.assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          e.printStackTrace();
          fail();
        }
        releaseNettyBufUnderneathStream(stream);
        // get blob all
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
        stream = channel.sendAndReceive(getRequest).getInputStream();
        resp = GetResponse.readFrom(stream, clusterMap);
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          assertEquals("Expiration time mismatch (props)", expectedExpiryTimeMs,
              getExpiryTimeMs(blobAll.getBlobInfo().getBlobProperties()));
          Assert.assertEquals("Expiration time mismatch (MessageInfo)", expectedExpiryTimeMs,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
          byte[] blobout = getBlobData(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          releaseNettyBufUnderneathStream(stream);
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
          updateBlobTtl(entry.getKey(), blobId, cluster.time.milliseconds());
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
        DataInputStream deleteResponseStream = channel.sendAndReceive(deleteRequest).getInputStream();
        DeleteResponse deleteResponse = DeleteResponse.readFrom(deleteResponseStream);
        releaseNettyBufUnderneathStream(deleteResponseStream);
        Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());
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
        DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
        GetResponse resp = GetResponse.readFrom(stream, clusterMap);
        Assert.assertEquals(ServerErrorCode.BlobDeleted, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        releaseNettyBufUnderneathStream(stream);
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
    cluster.reinitServer(0);

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
      DataInputStream stream = channel1.sendAndReceive(getRequest).getInputStream();
      GetResponse resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
          assertEquals(100, propertyOutput.getBlobSize());
          assertEquals("serviceid1", propertyOutput.getServiceId());
          assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
          assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // get user metadata
      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
      } else {
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobData(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob all
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsDeleted.contains(blobIds.get(j)));
        blobsDeleted.remove(blobIds.get(j));
        blobsChecked.add(blobIds.get(j));
      } else {
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobData(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
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
    cluster.reinitServer(0);

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
      DataInputStream stream = channel1.sendAndReceive(getRequest).getInputStream();
      GetResponse resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
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
      releaseNettyBufUnderneathStream(stream);
      // get user metadata
      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
      } else {
        try {
          ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
          assertArrayEquals(usermetadata, userMetadataOutput.array());
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
      } else {
        try {
          BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
          byte[] blobout = getBlobData(blobData);
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("MessageMetadata should not have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
            Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
          } else {
            assertNull("MessageMetadata should have been null",
                resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          }
        } catch (MessageFormatException e) {
          fail();
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob all
      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest).getInputStream();
      resp = GetResponse.readFrom(stream, clusterMap);
      if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobDeleted
          || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.BlobNotFound) {
        assertTrue(blobsChecked.contains(blobIds.get(j)));
        blobsChecked.remove(blobIds.get(j));
        blobsDeleted.add(blobIds.get(j));
      } else {
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobData(blobAll.getBlobData());
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
      releaseNettyBufUnderneathStream(stream);
    }
    assertEquals(0, blobsChecked.size());
    short expectedLifeVersion = 1;
    for (int i = 0; i < 2; i++) {
      expectedLifeVersion += i;
      // First undelete all deleted blobs
      for (BlobId deletedId : blobsDeleted) {
        UndeleteRequest undeleteRequest = new UndeleteRequest(2, "reptest", deletedId, System.currentTimeMillis());
        DataInputStream undeleteResponseStream = channel3.sendAndReceive(undeleteRequest).getInputStream();
        UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(undeleteResponseStream);
        releaseNettyBufUnderneathStream(undeleteResponseStream);
        Assert.assertEquals(ServerErrorCode.NoError, undeleteResponse.getError());
        Assert.assertEquals(expectedLifeVersion, undeleteResponse.getLifeVersion());
      }

      Thread.sleep(5000);
      // Then use get request to get all the data back and make sure the lifeVersion is correct
      for (BlobId id : blobsDeleted) {
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
        DataInputStream stream = channel1.sendAndReceive(getRequest).getInputStream();
        GetResponse resp = GetResponse.readFrom(stream, clusterMap);
        Assert.assertEquals(ServerErrorCode.NoError, resp.getError());
        Assert.assertEquals(1, resp.getPartitionResponseInfoList().size());
        Assert.assertEquals(ServerErrorCode.NoError, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        Assert.assertEquals(1, resp.getPartitionResponseInfoList().get(0).getMessageInfoList().size());
        MessageInfo info = resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
        assertEquals(expectedLifeVersion, info.getLifeVersion());
        assertFalse(info.isDeleted());
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobData(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
          releaseNettyBufUnderneathStream(stream);
        } catch (MessageFormatException e) {
          fail();
        }
      }
      for (BlobId id : blobsDeleted) {
        DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", id, System.currentTimeMillis());
        DataInputStream deleteResponseStream = channel.sendAndReceive(deleteRequest).getInputStream();
        DeleteResponse deleteResponse = DeleteResponse.readFrom(deleteResponseStream);
        releaseNettyBufUnderneathStream(deleteResponseStream);
        Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());
      }

      Thread.sleep(1000);
      for (BlobId id : blobsDeleted) {
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(id);
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(id.getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);

        // get blob all
        GetRequest getRequest =
            new GetRequest(1, "clientid200", MessageFormatFlags.All, partitionRequestInfoList, GetOption.Include_All);
        DataInputStream stream = channel1.sendAndReceive(getRequest).getInputStream();
        GetResponse resp = GetResponse.readFrom(stream, clusterMap);
        Assert.assertEquals(ServerErrorCode.NoError, resp.getError());
        Assert.assertEquals(1, resp.getPartitionResponseInfoList().size());
        Assert.assertEquals(ServerErrorCode.NoError, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        Assert.assertEquals(1, resp.getPartitionResponseInfoList().get(0).getMessageInfoList().size());
        MessageInfo info = resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
        assertEquals(expectedLifeVersion, info.getLifeVersion());
        assertTrue(info.isDeleted());
        try {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
          byte[] blobout = getBlobData(blobAll.getBlobData());
          assertArrayEquals(data, blobout);
          if (testEncryption) {
            assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
            assertArrayEquals("EncryptionKey mismatch", encryptionKey, blobAll.getBlobEncryptionKey().array());
          } else {
            assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
          }
          Assert.assertEquals("Expiration time mismatch in MessageInfo", Utils.Infinite_Time,
              resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs());
          releaseNettyBufUnderneathStream(stream);
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
          new BlobProperties(size, "service1", "owner id check", "image/jpeg", false, TestUtils.TTL_SECS,
              cluster.time.milliseconds(), accountId, containerId, false, null, null, null, null);
      final byte[] metadata = new byte[new Random().nextInt(1000)];
      final byte[] blob = new byte[size];
      TestUtils.RANDOM.nextBytes(metadata);
      TestUtils.RANDOM.nextBytes(blob);
      Future<String> future =
          router.putBlob(null, properties, metadata, new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob)),
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
              }, QUOTA_CHARGE_EVENT_LISTENER);
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
              cancelTest, portType, connectionPool, notificationSystem, cluster.time));
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
    DataInputStream stream = channel.sendAndReceive(controlRequest).getInputStream();
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Stop store admin request should succeed", ServerErrorCode.NoError, adminResponse.getError());

    // put a blob on a stopped store, which should fail
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[3187];
    BlobProperties properties =
        new BlobProperties(3187, "serviceid1", accountId, containerId, false, cluster.time.milliseconds());
    BlobId blobId2 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        clusterMap.getLocalDatacenterId(), accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
    PutRequest putRequest2 =
        new PutRequest(1, "clientId2", blobId2, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, null);
    DataInputStream putResponseStream = channel.sendAndReceive(putRequest2).getInputStream();
    PutResponse response2 = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals("Put blob on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
        response2.getError());

    // get a blob properties on a stopped store, which should fail
    ArrayList<BlobId> ids = new ArrayList<>();
    ids.add(blobId1);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest1 =
        new GetRequest(1, "clientId1", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
    stream = channel.sendAndReceive(getRequest1).getInputStream();
    GetResponse resp1 = GetResponse.readFrom(stream, clusterMap);
    Assert.assertEquals("Get blob properties on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
        resp1.getPartitionResponseInfoList().get(0).getErrorCode());
    releaseNettyBufUnderneathStream(stream);
    // delete a blob on a stopped store, which should fail
    DeleteRequest deleteRequest = new DeleteRequest(1, "clientId1", blobId1, System.currentTimeMillis());
    stream = channel.sendAndReceive(deleteRequest).getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Delete blob on stopped store should fail", ServerErrorCode.ReplicaUnavailable,
        deleteResponse.getError());

    // start the store via AdminRequest
    System.out.println("Begin to restart the BlobStore");
    adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, 1, "clientId");
    controlRequest = new BlobStoreControlAdminRequest((short) 0, BlobStoreControlAction.StartStore, adminRequest);
    stream = channel.sendAndReceive(controlRequest).getInputStream();
    adminResponse = AdminResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Start store admin request should succeed", ServerErrorCode.NoError, adminResponse.getError());
    List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
    for (ReplicaId replicaId : replicaIds) {
      // forcibly mark replicas and disks as up.
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      mockReplicaId.markReplicaDownStatus(false);
      ((MockDiskId) mockReplicaId.getDiskId()).setDiskState(HardwareState.AVAILABLE, false);
    }

    // put a blob on a restarted store , which should succeed
    putRequest2 =
        new PutRequest(1, "clientId2", blobId2, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, null);
    putResponseStream = channel.sendAndReceive(putRequest2).getInputStream();
    response2 = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals("Put blob on restarted store should succeed", ServerErrorCode.NoError, response2.getError());
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
    stream = channel.sendAndReceive(getRequest2).getInputStream();
    GetResponse resp2 = GetResponse.readFrom(stream, clusterMap);
    InputStream responseStream = resp2.getInputStream();
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);
    byte[] actualBlobData = getBlobData(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", data, actualBlobData);
    releaseNettyBufUnderneathStream(stream);
    // delete a blob on a restarted store , which should succeed
    deleteRequest = new DeleteRequest(1, "clientId2", blobId2, System.currentTimeMillis());
    stream = channel.sendAndReceive(deleteRequest).getInputStream();
    deleteResponse = DeleteResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Delete blob on restarted store should succeed", ServerErrorCode.NoError,
        deleteResponse.getError());

    router.close();
    connectionPool.shutdown();
  }

  static void endToEndReplicationWithMultiNodeSinglePartitionTest(String routerDatacenter,
      int interestedDataNodePortNumber, Port dataNode1Port, Port dataNode2Port, Port dataNode3Port, MockCluster cluster,
      SSLConfig clientSSLConfig1, SSLSocketFactory clientSSLSocketFactory1, MockNotificationSystem notificationSystem,
      Properties routerProps, boolean testEncryption) {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainText port
    try {
      // The header size of a LogSegment. This shouldn't be here since it breaks the interface of Log. But to satisfy the test cases
      // we will use this number here.
      // This also means we only have one log segment for this partition. If we put more operations to the partition and it excceeds
      // the log segment capacity, this number will have to be increased.
      int expectedTokenSize = 18;
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
            new BlobProperties(1000, "serviceid1", null, null, false, TestUtils.TTL_SECS, cluster.time.milliseconds(),
                accountId, containerId, testEncryption, null, null, null, null));
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
      DataInputStream putResponseStream = channel1.sendAndReceive(putRequest).getInputStream();
      PutResponse response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());
      // put blob 2
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobIdList.get(1), propertyList.get(1), ByteBuffer.wrap(usermetadata),
              Unpooled.wrappedBuffer(dataList.get(1)), propertyList.get(1).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(1) != null ? ByteBuffer.wrap(encryptionKeyList.get(1)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(1), blobIdList.get(1),
          encryptionKeyList.get(1) != null ? ByteBuffer.wrap(encryptionKeyList.get(1)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(1));
      putResponseStream = channel2.sendAndReceive(putRequest2).getInputStream();
      PutResponse response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());
      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobIdList.get(2), propertyList.get(2), ByteBuffer.wrap(usermetadata),
              Unpooled.wrappedBuffer(dataList.get(2)), propertyList.get(2).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(2) != null ? ByteBuffer.wrap(encryptionKeyList.get(2)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(2), blobIdList.get(2),
          encryptionKeyList.get(2) != null ? ByteBuffer.wrap(encryptionKeyList.get(2)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(2));
      putResponseStream = channel3.sendAndReceive(putRequest3).getInputStream();
      PutResponse response3 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response3.getError());

      // put blob 4
      putRequest = new PutRequest(1, "client1", blobIdList.get(3), propertyList.get(3), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(3)), propertyList.get(3).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(3) != null ? ByteBuffer.wrap(encryptionKeyList.get(3)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(3), blobIdList.get(3),
          encryptionKeyList.get(3) != null ? ByteBuffer.wrap(encryptionKeyList.get(3)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(3));
      putResponseStream = channel1.sendAndReceive(putRequest).getInputStream();
      response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());

      // put blob 5
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(4), propertyList.get(4), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(4)), propertyList.get(4).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(4) != null ? ByteBuffer.wrap(encryptionKeyList.get(4)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(4), blobIdList.get(4),
          encryptionKeyList.get(4) != null ? ByteBuffer.wrap(encryptionKeyList.get(4)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(4));
      putResponseStream = channel2.sendAndReceive(putRequest2).getInputStream();
      response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      // put blob 6
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(5), propertyList.get(5), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(5)), propertyList.get(5).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(5) != null ? ByteBuffer.wrap(encryptionKeyList.get(5)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(5), blobIdList.get(5),
          encryptionKeyList.get(5) != null ? ByteBuffer.wrap(encryptionKeyList.get(5)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(5));
      putResponseStream = channel3.sendAndReceive(putRequest3).getInputStream();
      response3 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response3.getError());
      // wait till replication can complete
      notificationSystem.awaitBlobCreations(blobIdList.get(0).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(3).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());
      notificationSystem.awaitBlobCreations(blobIdList.get(5).getID());

      checkTtlUpdateStatus(channel3, clusterMap, blobIdFactory, blobIdList.get(5), dataList.get(5), false,
          getExpiryTimeMs(propertyList.get(5)));
      updateBlobTtl(channel3, blobIdList.get(5), cluster.time.milliseconds());
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
      DataInputStream stream = channel2.sendAndReceive(getRequest1).getInputStream();
      GetResponse resp1 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals(ServerErrorCode.NoError, resp1.getError());
      Assert.assertEquals(ServerErrorCode.NoError, resp1.getPartitionResponseInfoList().get(0).getErrorCode());
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
      stream = channel1.sendAndReceive(getRequest2).getInputStream();
      GetResponse resp2 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals(ServerErrorCode.NoError, resp2.getError());
      Assert.assertEquals(ServerErrorCode.NoError, resp2.getPartitionResponseInfoList().get(0).getErrorCode());
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        assertArrayEquals(usermetadata, userMetadataOutput.array());
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(1),
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp2.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
      } catch (MessageFormatException e) {
        fail();
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob
      ids.clear();
      ids.add(blobIdList.get(0));
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      stream = channel3.sendAndReceive(getRequest3).getInputStream();
      GetResponse resp3 = GetResponse.readFrom(stream, clusterMap);
      try {
        BlobData blobData = MessageFormatRecord.deserializeBlob(resp3.getInputStream());
        byte[] blobout = getBlobData(blobData);
        assertArrayEquals(dataList.get(0), blobout);
        if (testEncryption) {
          assertNotNull("MessageMetadata should not have been null",
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
          Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKeyList.get(0),
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
        } else {
          assertNull("MessageMetadata should have been null",
              resp3.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
        }
      } catch (MessageFormatException e) {
        fail();
      }
      releaseNettyBufUnderneathStream(stream);
      // get blob all
      ids.clear();
      ids.add(blobIdList.get(0));
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
      stream = channel1.sendAndReceive(getRequest4).getInputStream();
      GetResponse resp4 = GetResponse.readFrom(stream, clusterMap);
      try {
        BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp4.getInputStream(), blobIdFactory);
        byte[] blobout = getBlobData(blobAll.getBlobData());
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
      releaseNettyBufUnderneathStream(stream);
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
      stream = channel3.sendAndReceive(getRequest5).getInputStream();
      GetResponse resp5 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals(ServerErrorCode.NoError, resp5.getError());
      Assert.assertEquals(ServerErrorCode.BlobNotFound, resp5.getPartitionResponseInfoList().get(0).getErrorCode());
      releaseNettyBufUnderneathStream(stream);
      // delete a blob and ensure it is propagated
      DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobIdList.get(0), System.currentTimeMillis());
      expectedTokenSize += getUpdateRecordSize(blobIdList.get(0), SubRecord.Type.DELETE);
      DataInputStream deleteResponseStream = channel1.sendAndReceive(deleteRequest).getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(deleteResponseStream);
      releaseNettyBufUnderneathStream(deleteResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());

      notificationSystem.awaitBlobDeletions(blobIdList.get(0).getID());
      ids = new ArrayList<BlobId>();
      ids.add(blobIdList.get(0));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest6 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      stream = channel3.sendAndReceive(getRequest6).getInputStream();
      GetResponse resp6 = GetResponse.readFrom(stream, clusterMap);
      Assert.assertEquals(ServerErrorCode.NoError, resp6.getError());
      Assert.assertEquals(ServerErrorCode.BlobDeleted, resp6.getPartitionResponseInfoList().get(0).getErrorCode());
      releaseNettyBufUnderneathStream(stream);
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
      putResponseStream = channel2.sendAndReceive(putRequest2).getInputStream();
      response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      // put blob 8
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(7), propertyList.get(7), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(7)), propertyList.get(7).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(7) != null ? ByteBuffer.wrap(encryptionKeyList.get(7)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(7), blobIdList.get(7),
          encryptionKeyList.get(7) != null ? ByteBuffer.wrap(encryptionKeyList.get(7)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(7));
      putResponseStream = channel3.sendAndReceive(putRequest3).getInputStream();
      response3 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response3.getError());

      // put blob 9
      putRequest2 = new PutRequest(1, "client1", blobIdList.get(8), propertyList.get(8), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(8)), propertyList.get(8).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(8) != null ? ByteBuffer.wrap(encryptionKeyList.get(8)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(8), blobIdList.get(8),
          encryptionKeyList.get(8) != null ? ByteBuffer.wrap(encryptionKeyList.get(8)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(8));
      putResponseStream = channel2.sendAndReceive(putRequest2).getInputStream();
      response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      // put blob 10
      putRequest3 = new PutRequest(1, "client1", blobIdList.get(9), propertyList.get(9), ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(dataList.get(9)), propertyList.get(9).getBlobSize(), BlobType.DataBlob,
          encryptionKeyList.get(9) != null ? ByteBuffer.wrap(encryptionKeyList.get(9)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(9), blobIdList.get(9),
          encryptionKeyList.get(9) != null ? ByteBuffer.wrap(encryptionKeyList.get(9)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(9));
      putResponseStream = channel3.sendAndReceive(putRequest3).getInputStream();
      response3 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response3.getError());

      // put blob 11
      putRequest2 =
          new PutRequest(1, "client1", blobIdList.get(10), propertyList.get(10), ByteBuffer.wrap(usermetadata),
              Unpooled.wrappedBuffer(dataList.get(10)), propertyList.get(10).getBlobSize(), BlobType.DataBlob,
              encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null);
      expectedTokenSize += getPutRecordSize(propertyList.get(10), blobIdList.get(10),
          encryptionKeyList.get(10) != null ? ByteBuffer.wrap(encryptionKeyList.get(10)) : null,
          ByteBuffer.wrap(usermetadata), dataList.get(10));
      putResponseStream = channel2.sendAndReceive(putRequest2).getInputStream();
      response2 = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      checkTtlUpdateStatus(channel2, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), false,
          getExpiryTimeMs(propertyList.get(10)));
      updateBlobTtl(channel2, blobIdList.get(10), cluster.time.milliseconds());
      expectedTokenSize += getUpdateRecordSize(blobIdList.get(10), SubRecord.Type.TTL_UPDATE);
      checkTtlUpdateStatus(channel2, clusterMap, blobIdFactory, blobIdList.get(10), dataList.get(10), true,
          Utils.Infinite_Time);

      cluster.reinitServer(0);
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
      for (File toDelete : Objects.requireNonNull(mountFile.listFiles())) {
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

      cluster.reinitServer(0);
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

  static void undeleteCornerCasesTest(MockCluster cluster, PortType portType, SSLConfig clientSSLConfig1,
      SSLConfig clientSSLConfig2, SSLConfig clientSSLConfig3, SSLSocketFactory clientSSLSocketFactory1,
      SSLSocketFactory clientSSLSocketFactory2, SSLSocketFactory clientSSLSocketFactory3,
      MockNotificationSystem notificationSystem, Properties routerProps, boolean testEncryption) {
    MockClusterMap clusterMap = cluster.getClusterMap();
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[31870];
    byte[] encryptionKey = new byte[100];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties properties =
        new BlobProperties(31870, "serviceid1", accountId, containerId, testEncryption, cluster.time.milliseconds());
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);
    if (testEncryption) {
      TestUtils.RANDOM.nextBytes(encryptionKey);
    }
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    Map<String, List<DataNodeId>> dataNodesPerDC =
        clusterMap.getDataNodes().stream().collect(Collectors.groupingBy(DataNodeId::getDatacenterName));
    Map<String, Pair<SSLConfig, SSLSocketFactory>> sslSettingPerDC = new HashMap<>();
    sslSettingPerDC.put("DC1", new Pair<>(clientSSLConfig1, clientSSLSocketFactory1));
    sslSettingPerDC.put("DC2", new Pair<>(clientSSLConfig2, clientSSLSocketFactory2));
    sslSettingPerDC.put("DC3", new Pair<>(clientSSLConfig3, clientSSLSocketFactory3));

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    DataNodeId dataNodeId = dataNodesPerDC.get("DC1").get(0);
    Router router = null;
    try {
      Properties routerProperties = getRouterProps("DC1");
      routerProperties.putAll(routerProps);
      VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProperties);
      AccountService accountService = new InMemAccountService(false, true);
      router = new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, new MockNotificationSystem(clusterMap),
          getSSLFactoryIfRequired(routerVerifiableProps), accountService).getRouter();

      // channels to all datanodes
      List<ConnectedChannel> channels = new ArrayList<>();
      for (Map.Entry<String, List<DataNodeId>> entry : dataNodesPerDC.entrySet()) {
        Pair<SSLConfig, SSLSocketFactory> pair = sslSettingPerDC.get(entry.getKey());
        for (DataNodeId node : entry.getValue()) {
          ConnectedChannel connectedChannel =
              getBlockingChannelBasedOnPortType(portType, node, pair.getSecond(), pair.getFirst());
          connectedChannel.connect();
          channels.add(connectedChannel);
        }
      }

      //////////////////////////////////////////////////////
      // Corner case 1: When only one datacenter has delete
      //////////////////////////////////////////////////////
      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      ConnectedChannel channel =
          getBlockingChannelBasedOnPortType(portType, dataNodeId, clientSSLSocketFactory1, clientSSLConfig1);
      channel.connect();

      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);

      DataInputStream putResponseStream = channel.sendAndReceive(putRequest).getInputStream();

      PutResponse response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());

      notificationSystem.awaitBlobCreations(blobId1.toString());

      // Now stop the replications this partition.
      PartitionId partitionId = blobId1.getPartition();
      controlReplicationForPartition(channels, partitionId, false);

      // Now send the delete to two data nodes in the same DC
      List<DataNodeId> toBeDeleteDataNodes = dataNodesPerDC.values().stream().findFirst().get();
      Pair<SSLConfig, SSLSocketFactory> pair = sslSettingPerDC.get(toBeDeleteDataNodes.get(0).getDatacenterName());
      ConnectedChannel channel1 =
          getBlockingChannelBasedOnPortType(portType, toBeDeleteDataNodes.get(0), pair.getSecond(), pair.getFirst());
      channel1.connect();
      ConnectedChannel channel2 =
          getBlockingChannelBasedOnPortType(portType, toBeDeleteDataNodes.get(1), pair.getSecond(), pair.getFirst());
      channel2.connect();
      DeleteRequest deleteRequest1 = new DeleteRequest(1, "deleteClient", blobId1, System.currentTimeMillis());
      DataInputStream stream = channel1.sendAndReceive(deleteRequest1).getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());
      DeleteRequest deleteRequest2 =
          new DeleteRequest(1, "deleteClient", blobId1, deleteRequest1.getDeletionTimeInMs());
      stream = channel2.sendAndReceive(deleteRequest2).getInputStream();
      deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());

      // Now send the undelete operation through router, and it should fail because of not deleted error.
      Future<Void> future = router.undeleteBlob(blobId1.toString(), "service");
      try {
        future.get();
        fail("Undelete blob " + blobId1.toString() + " should fail");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof RouterException);
        assertEquals(RouterErrorCode.BlobNotDeleted, ((RouterException) e.getCause()).getErrorCode());
      }

      // Now see if either data node 1 or data node 2 has undelete or not, if so, undelete would replicate. If not,
      // delete would replicate.
      List<PartitionRequestInfo> partitionRequestInfoList = getPartitionRequestInfoListFromBlobId(blobId1);
      boolean hasUndelete = false;
      for (ConnectedChannel connectedChannel : new ConnectedChannel[]{channel1, channel2}) {
        GetRequest getRequest =
            new GetRequest(1, "clientId1", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                GetOption.Include_All);
        stream = channel1.sendAndReceive(getRequest).getInputStream();
        GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
        Assert.assertEquals(ServerErrorCode.NoError, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
        MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
        hasUndelete =
            getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getLifeVersion() == (short) 1;
        if (hasUndelete) {
          break;
        }
      }
      releaseNettyBufUnderneathStream(stream);
      // Now restart the replication
      controlReplicationForPartition(channels, partitionId, true);
      if (hasUndelete) {
        notificationSystem.awaitBlobUndeletes(blobId1.toString());
      } else {
        notificationSystem.awaitBlobDeletions(blobId1.toString());
      }

      for (ConnectedChannel connectedChannel : channels) {
        GetRequest getRequest =
            new GetRequest(1, "clientId1", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                GetOption.Include_All);
        stream = connectedChannel.sendAndReceive(getRequest).getInputStream();
        GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
        releaseNettyBufUnderneathStream(stream);
        Assert.assertEquals(ServerErrorCode.NoError, getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
        MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
        if (hasUndelete) {
          Assert.assertEquals((short) 1,
              getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getLifeVersion());
          assertTrue(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isUndeleted());
          assertFalse(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isDeleted());
        } else {
          Assert.assertEquals((short) 0,
              getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getLifeVersion());
          assertTrue(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isDeleted());
          assertFalse(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isUndeleted());
        }
      }

      /////////////////////////////////////////////////////////////
      // Corner case 2: two data nodes have different life versions
      ////////////////////////////////////////////////////////////
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      putRequest =
          new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putResponseStream = channel.sendAndReceive(putRequest).getInputStream();
      response = PutResponse.readFrom(putResponseStream);
      releaseNettyBufUnderneathStream(putResponseStream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());
      notificationSystem.awaitBlobCreations(blobId2.toString());

      // Now delete this blob on all servers.
      DeleteRequest deleteRequest = new DeleteRequest(1, "deleteClient", blobId2, System.currentTimeMillis());
      stream = channel.sendAndReceive(deleteRequest).getInputStream();
      deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());
      notificationSystem.awaitBlobDeletions(blobId2.toString());

      // Now stop the replication
      partitionId = blobId2.getPartition();
      controlReplicationForPartition(channels, partitionId, false);

      // Now send the undelete to two data nodes in the same DC and then send delete
      UndeleteRequest undeleteRequest = new UndeleteRequest(1, "undeleteClient", blobId2, System.currentTimeMillis());
      stream = channel1.sendAndReceive(undeleteRequest).getInputStream();
      UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, undeleteResponse.getError());
      Assert.assertEquals((short) 1, undeleteResponse.getLifeVersion());
      undeleteRequest = new UndeleteRequest(1, "undeleteClient", blobId2, undeleteRequest.getOperationTimeMs());
      stream = channel2.sendAndReceive(undeleteRequest).getInputStream();
      undeleteResponse = UndeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, undeleteResponse.getError());
      Assert.assertEquals((short) 1, undeleteResponse.getLifeVersion());

      deleteRequest1 = new DeleteRequest(1, "deleteClient", blobId2, System.currentTimeMillis());
      stream = channel1.sendAndReceive(deleteRequest1).getInputStream();
      deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());
      deleteRequest2 = new DeleteRequest(1, "deleteClient", blobId2, deleteRequest1.getDeletionTimeInMs());
      stream = channel2.sendAndReceive(deleteRequest2).getInputStream();
      deleteResponse = DeleteResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, deleteResponse.getError());

      // Now send the undelete operation through router, and it should fail because of lifeVersion conflict error.
      future = router.undeleteBlob(blobId2.toString(), "service");
      try {
        future.get();
        fail("Undelete blob " + blobId2.toString() + " should fail");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof RouterException);
        assertEquals(RouterErrorCode.LifeVersionConflict, ((RouterException) e.getCause()).getErrorCode());
      }

      // Now restart the replication
      controlReplicationForPartition(channels, partitionId, true);
      notificationSystem.awaitBlobUndeletes(blobId2.toString());

      // Now after replication is resumed, the undelete of lifeversion 2 will eventually be replicated to all servers.
      partitionRequestInfoList = getPartitionRequestInfoListFromBlobId(blobId2);
      for (ConnectedChannel connectedChannel : channels) {
        // Even if the notificationSystem acknowledged the undelete, it might be triggered by undelete at lifeversion 1.
        // So check in a loop with a time out.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (true) {
          GetRequest getRequest =
              new GetRequest(1, "clientId1", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                  GetOption.Include_All);
          stream = connectedChannel.sendAndReceive(getRequest).getInputStream();
          GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
          Assert.assertEquals(ServerErrorCode.NoError,
              getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
          MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
          if (getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getLifeVersion() == 2) {
            assertTrue(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isUndeleted());
            assertFalse(getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).isDeleted());
            break;
          } else {
            Thread.sleep(1000);
            if (System.currentTimeMillis() > deadline) {
              throw new TimeoutException(
                  "Fail to get blob " + blobId2 + " at lifeversion 2 at " + connectedChannel.getRemoteHost());
            }
          }
        }
      }
      releaseNettyBufUnderneathStream(stream);
      for (ConnectedChannel connectedChannel : channels) {
        connectedChannel.disconnect();
      }
      channel1.disconnect();
      channel2.disconnect();
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      if (router != null) {
        try {
          router.close();
        } catch (Exception e) {
        }
      }
    }
  }

  static void undeleteRecoveryTest(Port targetPort, MockCluster cluster, SSLConfig clientSSLConfig,
      SSLSocketFactory clientSSLSocketFactory) {
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      byte[] userMetadata = new byte[1000];
      byte[] data = new byte[31870];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);

      BlobProperties properties =
          new BlobProperties(31870, "serviceid1", accountId, containerId, false, cluster.time.milliseconds());
      TestUtils.RANDOM.nextBytes(userMetadata);
      TestUtils.RANDOM.nextBytes(data);
      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();

      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);

      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(data),
              properties.getBlobSize(), BlobType.DataBlob, null);
      ConnectedChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      DataInputStream stream = channel.sendAndReceive(putRequest).getInputStream();
      PutResponse response = PutResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, response.getError());

      for (int i = 0; i < 2; i++) {
        // delete blob 1
        deleteBlob(channel, blobId1, cluster.time.milliseconds());
        // undelete blob 1
        undeleteBlob(channel, blobId1, cluster.time.milliseconds(), (short) (i + 1));
      }

      // put blob 2 that is expired (Add additional 5 secs to avoid Blob_Update_Not_Allowed failure as TtlUpdate op time
      // is also cluster.time.milliseconds(). Theoretically, it should succeed as op time = expiry time - buffer time.
      // However, the index value converts ms to sec when putting a blob, so the milliseconds part of initial put blob
      // time is wiped out and makes op time > expiry time - buffer time. Adding some time should avoid this failure.)
      long ttl = 24 * 60 * 60 + 5;
      BlobProperties propertiesExpired =
          new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, ttl, cluster.time.milliseconds(), accountId,
              containerId, false, null, null, null, null);
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          propertiesExpired.getAccountId(), propertiesExpired.getContainerId(), partitionIds.get(0), false,
          BlobId.BlobDataType.DATACHUNK);
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, propertiesExpired, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob, null);
      stream = channel.sendAndReceive(putRequest2).getInputStream();
      PutResponse response2 = PutResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, response2.getError());

      for (int i = 0; i < 2; i++) {
        // delete blob 2
        deleteBlob(channel, blobId2, cluster.time.milliseconds());
        // undelete blob 2
        undeleteBlob(channel, blobId2, cluster.time.milliseconds(), (short) (i + 1));
      }

      // ttl update blob 2
      updateBlobTtl(channel, blobId2, cluster.time.milliseconds());
      cluster.time.sleep(ttl + 10000);

      // Now stops the server and remove all the index files for this partition and test its recovery.
      channel.disconnect();
      AmbryServer server = cluster.getServers().get(0);
      server.shutdown();
      server.awaitShutdown();

      MockDataNodeId dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", channel.getRemotePort());
      for (ReplicaId replica : partitionIds.get(0).getReplicaIds()) {
        if (replica.getDataNodeId().equals(dataNode)) {
          for (File file : Objects.requireNonNull(
              new File(replica.getReplicaPath()).listFiles((file, filename) -> filename.endsWith("index")))) {
            file.delete();
          }
        }
      }
      cluster.reinitServer(0);
      channel = getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();

      // Now verify that we can fetch blob1 and blob2.
      for (BlobId blobId : new BlobId[]{blobId1, blobId2}) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);

        while (true) {
          // get blob properties
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionIds.get(0), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                  GetOption.None);
          stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
          if (getResponse.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.NoError) {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(getResponse.getInputStream());
            assertEquals(31870, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
            assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
            assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
            releaseNettyBufUnderneathStream(stream);
            break;
          } else {
            Thread.sleep(1000);
            if (System.currentTimeMillis() > deadline) {
              throw new TimeoutException("Fail to get blob " + blobId + " at  " + channel.getRemoteHost());
            }
          }
        }
      }
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Test ReplicateBlob under different conditions.
   * 1. test the data correctness of the on-demand replication on the target DataNode.
   * 2. test the interaction between the regular replication and on-demand replication.
   * If writeRepair is true, even the local store has the BlobID, ReplicateBlob is still executed and will repair the final state.
   * If writeRepair is false, if the local store has the BlobID, ReplicateBlob does nothing and returns success status.
   */
  static void replicateBlobCaseTest(MockCluster cluster, SSLConfig clientSSLConfig, boolean testEncryption,
      MockNotificationSystem notificationSystem, boolean writeRepair) {
    List<MockDataNodeId> allNodes = cluster.getAllDataNodes();
    MockClusterMap clusterMap = cluster.getClusterMap();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = partitionIds.get(0);

    ArrayList<String> dataCenterList = new ArrayList<>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = cluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    /*
     * sourceDataNode is the data node we send PutBlob, TtlUpdate, Delete requests.
     * targetDataNode is the data node we run ReplicateBlob. targetDataNode replicates Blobs from the sourceDataNode.
     * comparisonDataNode uses the regular replication thread to replicate the Blobs from peer replicas.
     */
    DataNodeId sourceDataNode = dataNodes.get(0);
    DataNodeId targetDataNode = dataNodes.get(1);
    DataNodeId comparisonDataNode = dataNodes.get(2);

    try {
      // Stop replication for this partition on all the servers.
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, false);

      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      short dataSize = 31870;
      byte[] userMetadata = new byte[1000];
      byte[] data = new byte[dataSize];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      TestUtils.RANDOM.nextBytes(userMetadata);
      TestUtils.RANDOM.nextBytes(data);
      TestUtils.RANDOM.nextBytes(encryptionKey);

      // property with 7days expiration
      BlobProperties propertiesWithTtl =
          new BlobProperties(dataSize, "serviceid1", "ownerid", "image/png", false, TestUtils.TTL_SECS,
              cluster.time.milliseconds(), accountId, containerId, testEncryption, null, null, null, null);
      long ttlUpdateBlobExpiryTimeMs = getExpiryTimeMs(propertiesWithTtl);
      // property of expired blob
      BlobProperties propertiesExpired =
          new BlobProperties(dataSize, "serviceid1", "ownerid", "jpeg", false, 0, cluster.time.milliseconds(),
              accountId, containerId, testEncryption, null, null, null, null);
      // property of blob with the infinite TTL
      BlobProperties properties = new BlobProperties(dataSize, "serviceid1", accountId, containerId, testEncryption,
          cluster.time.milliseconds());

      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId4 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId5 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId6 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId7 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId8 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId9 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId10 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId11 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId12 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId13 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId14 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId15 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);

      Port sourcePort = new Port(sourceDataNode.getHttp2Port(), PortType.HTTP2);
      Port targetPort = new Port(targetDataNode.getHttp2Port(), PortType.HTTP2);
      Port comparisonPort = new Port(comparisonDataNode.getHttp2Port(), PortType.HTTP2);
      ConnectedChannel sourceChannel =
          getBlockingChannelBasedOnPortType(sourcePort, "localhost", null, clientSSLConfig);
      ConnectedChannel targetChannel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", null, clientSSLConfig);
      ConnectedChannel comparisonChannel =
          getBlockingChannelBasedOnPortType(comparisonPort, "localhost", null, clientSSLConfig);
      sourceChannel.connect();
      targetChannel.connect();
      comparisonChannel.connect();

      /*
       * Stage 1: Test the correctness of the ReplicateBlob. Replicate blobs from sourceDataNode to the targetDataNode
       * On the sourceDataNode and the targetDataNode, the blob is under different states.
       */
      // 1. On the sourceDataNode, putBlob. On the targetDataNode, blob doesn't exist.
      // ReplicateBlob replicates the blob to the target.
      BlobId blobId = blobId1;
      PutRequest putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 2. On the sourceDataNode, putBlob. On the targetDataNode, the blob already exists.
      // ReplicateBlob to both the sourceDataNode and the targetDataNode. All succeed.
      blobId = blobId2;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      replicateBlob(sourceChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 3, On the sourceDataNode, putBlob. On the targetDataNode, the blob is ttlUpdated.
      // ReplicateBlob still succeeds
      blobId = blobId3;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      updateBlobTtl(targetChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);

      // 4. On the sourceDataNode, putBlob. On the targetDataNode, the blob is deleted.
      // ReplicateBlob still succeeds
      blobId = blobId4;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 5. On the sourceDatanode, the blob is ttlUpdated. On the targetDataNode, it doesn't exist.
      // ReplicateBlob will replicate both the putBlob and the TtlUpdate
      blobId = blobId5;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      updateBlobTtl(sourceChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicate to the targetDataNode
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);

      // 6. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, putBlob.
      // ReplicateBlob replicates the ttlUpdate.
      blobId = blobId6;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      updateBlobTtl(sourceChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      if (writeRepair) {
        checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      } else {
        checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      }

      // 7. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, the blob is also ttlUpdated already.
      // ReplicateBlob still succeeds.
      blobId = blobId7;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      updateBlobTtl(sourceChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      updateBlobTtl(targetChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);

      // 8. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, the blob is deleted
      // ReplicateBlob still succeeds. Nothing is changed on the local store.
      blobId = blobId8;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      updateBlobTtl(sourceChannel, blobId, cluster.time.milliseconds());
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 9. On the sourceDataNode, the blob is deleted. On the targetDataNode, it doesn't exist.
      // ReplicateBlob replicates both the putBlob and the deleteBlob.
      blobId = blobId9;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      deleteBlob(sourceChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 10. On the sourceDataNode, the blob is deleted. On the targetDataNode, it exists.
      // ReplicateBlob replicates the deleteBlob
      blobId = blobId10;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      deleteBlob(sourceChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      if (writeRepair) {
        getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
      } else {
        getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
      }

      // 11. On the sourceDataNode, the blob is deleted. On the targetDataNode, it's ttlUpdated.
      // ReplicateBlob replicates the deleteBlob.
      blobId = blobId11;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      deleteBlob(sourceChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      updateBlobTtl(targetChannel, blobId, cluster.time.milliseconds());
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      if (writeRepair) {
        getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
      } else {
        getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      }

      // 12. On the sourceDataNode, the blob is deleted. On the targetDataNode, it's also deleted.
      // ReplicateBlob still succeeds
      blobId = blobId12;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      deleteBlob(sourceChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 13. On the sourceDataNode, the blob is expired. On the targetDataNode, it doesn't exist.
      // ReplicateBlob replicate the Blob even it's expired.
      blobId = blobId13;
      putRequest = new PutRequest(1, "client1", blobId, propertiesExpired, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      cluster.time.sleep(10000);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobExpired, propertiesExpired, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 14. On the sourceDataNode, the blob is expired. On the targetDataNode, it's deleted.
      // ReplicateBlob succeeds but nothing is changed on the local store.
      blobId = blobId14;
      putRequest = new PutRequest(1, "client1", blobId, propertiesExpired, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      cluster.time.sleep(10000);
      putRequest = new PutRequest(1, "client1", blobId, propertiesExpired, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.NoError);
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesExpired, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 15. On the sourceDataNode, the blob doesn't exist.
      // ReplicateBlob fails with Blob_Not_Found status
      blobId = blobId15;
      replicateBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BlobNotFound);

      /*
       * Stage 2: Update the blobs on either the sourceDataNode or the targetDataNode.
       * Enable regular replication. sourceDataNode and targetDataNode should be synced up after the regular replication.
       * Other replicas should catch up as well.
               sourceDataNode  targetDataNode  target_after_ReplicateBlob     extra_operation        after_replication
                                               write_repair/non_write_repair
        blob1     putBlob         not_exist       putBlob                   ttlUpdate_on_source       ttlUpdated
        blob2     putBlob         putBlob         putBlob                   ttlUpdate_on_target       ttlUpdated
        blob3     putBlob         ttlUpdated      ttlUpdated                deleteBlob_on_source      deleted, can be not_found
        blob4     putBlob         deleted         deleted                                             deleted, can be not_found
        blob5     ttlUpdated      not_exist       ttlUpdated                                          ttlUpdated
        blob6     ttlUpdated      putBlob         ttlUpdated/putBlob        deleteBlob_on_target      deleted, can be not_found
        blob7     ttlUpdated      ttlUpdated      ttlUpdated                                          ttlUpdated
        blob8     ttlUpdated      deleted         deleted                                             deleted, can be not_found
        blob9     deleted         not_exist       deleted                   undeleteBlob_on_target    undeleted
        blob10    deleted         putBlob         deleted/putBlob           undeleteBlob_on_source    undeleted
        blob11    deleted         ttlUpdated      deleted/ttlUpdated                                  deleted/not_found
        blob12    deleted         deleted         deleted                                             deleted/not_found
        blob13    expired         not_exist       expired                                             expired, can be not_found
        blob14    expired         deleted         deleted                                             deleted, can be expired/not_found
        blob15    not_exist       not_exist       error: Blob_Not_Found                               Not_Found
        blob16                                                              putBlob_on_source         putBlob
        blob17                                                              purBlob_on_target         putBob
       */

      // TtlUpdate blob1 on the sourceDataNode
      updateBlobTtl(sourceChannel, blobId1, cluster.time.milliseconds());
      // TtlUpdate blob2 on the targetDataNode
      updateBlobTtl(targetChannel, blobId2, cluster.time.milliseconds());
      // Delete blob3 on the sourceDataNode
      deleteBlob(sourceChannel, blobId3, cluster.time.milliseconds(), ServerErrorCode.NoError);
      // Delete blob6 on the targetDataNode
      deleteBlob(targetChannel, blobId6, cluster.time.milliseconds(), ServerErrorCode.NoError);
      // Undelete blob9 on the targetDataNode
      undeleteBlob(targetChannel, blobId9, cluster.time.milliseconds(), (short) 1);
      // Undelete blob10 on the sourceDataNode
      undeleteBlob(sourceChannel, blobId10, cluster.time.milliseconds(), (short) 1);
      // create blob16 on the sourceDataNode
      BlobId blobId16 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", blobId16, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      // create blob17 on the targetDataNode
      BlobId blobId17 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", blobId17, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);

      // Now start the replication on all the replicas
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, true);

      // wait for the replication to catch up
      notificationSystem.awaitBlobCreations(blobId16.getID());
      notificationSystem.awaitBlobCreations(blobId17.getID());

      // verify the status. sourceDataNode and targetDataNode are synced up.
      // the comparisonDataNode replicated the blobs from the peers.
      for (int i = 0; i < 3; i++) {
        ConnectedChannel channel;
        if (i == 0) {
          channel = sourceChannel;
        } else if (i == 1) {
          channel = targetChannel;
        } else {
          channel = comparisonChannel;
        }
        // blob1: TtlUpdate with Infinite
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId1, data, true, Utils.Infinite_Time);
        getBlobAndVerify(blobId1, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob2: TtlUpdate with Infinite
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId2, data, true, Utils.Infinite_Time);
        getBlobAndVerify(blobId2, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob3: deleted. For the comparisonDataNode, regular replication won't replicate deleted blob. It can be either NOT_FOUND or DELETED.
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId3);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId3, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob4: deleted. ComparisonChannel can be NOT_FOUND
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId4);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId4, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob5: TtlUpdate with Infinite
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId5, data, true, Utils.Infinite_Time);
        getBlobAndVerify(blobId5, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob6: deleted. For the comparisonDataNode, regular replication won't replicate deleted blob. It can be either NOT_FOUND or DELETED.
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId6);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId6, channel, ServerErrorCode.BlobDeleted, propertiesExpired, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob7: TtlUpdate with Infinite
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId7, data, true, Utils.Infinite_Time);
        getBlobAndVerify(blobId7, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob8: deleted.
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId8);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId8, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob9: undeleted
        getBlobAndVerify(blobId9, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob10: undeleted
        getBlobAndVerify(blobId10, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob11: deleted or not_found
        if (channel == comparisonChannel) {
          if (writeRepair) {
            getBlobAndVerify(blobId11, channel, ServerErrorCode.BlobNotFound, propertiesExpired, userMetadata, data,
                encryptionKey, clusterMap, blobIdFactory, testEncryption);
          } else {
            ArrayList<BlobId> ids = new ArrayList<>();
            ids.add(blobId11);
            ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
            PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
            partitionRequestInfoList.add(partitionRequestInfo);
            GetRequest getRequest =
                new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
            DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
            GetResponse resp = GetResponse.readFrom(stream, clusterMap);
            ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
            assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobNotFound);
            releaseNettyBufUnderneathStream(stream);
          }
        } else {
          getBlobAndVerify(blobId11, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob12: deleted
        if (channel == comparisonChannel) {
          getBlobAndVerify(blobId12, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        } else {
          getBlobAndVerify(blobId12, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob13: expired
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId13);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobExpired || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId13, channel, ServerErrorCode.BlobExpired, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob14: deleted
        if (channel == comparisonChannel) {
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(blobId14);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.BlobDeleted || error == ServerErrorCode.BlobExpired
              || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
        } else {
          getBlobAndVerify(blobId14, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption);
        }
        // blob15: not found
        getBlobAndVerify(blobId15, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        // blob16: newly created with infinite ttl
        getBlobAndVerify(blobId16, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption);
        // blob17: newly created with infinite ttl
        getBlobAndVerify(blobId17, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption);

        channel.disconnect();
      } // for loop to verify the three DataNode
    } catch (Exception e) {
      e.printStackTrace();
      assertNull(e);
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
   * Test Replicate delete record tombstone.
   */
  static void replicateDeleteTomeStoneTest(MockCluster cluster, SSLConfig clientSSLConfig, boolean testEncryption,
      MockNotificationSystem notificationSystem) {
    List<MockDataNodeId> allNodes = cluster.getAllDataNodes();
    MockClusterMap clusterMap = cluster.getClusterMap();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = partitionIds.get(0);

    ArrayList<String> dataCenterList = new ArrayList<>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = cluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    /*
     * sourceDataNode is the data node we send PutBlob, TtlUpdate, Delete requests.
     * targetDataNode is the data node we run ReplicateBlob. targetDataNode replicates Blobs from the sourceDataNode.
     * comparisonDataNode is used to verify the data after the replication threads sync all the nodes.
     */
    DataNodeId sourceDataNode = dataNodes.get(0);
    DataNodeId targetDataNode = dataNodes.get(1);
    DataNodeId comparisonNode = dataNodes.get(2);

    try {
      // Stop replication for this partition on all the servers.
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, false);

      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      short dataSize = 31870;
      byte[] userMetadata = new byte[1000];
      byte[] data = new byte[dataSize];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      TestUtils.RANDOM.nextBytes(userMetadata);
      TestUtils.RANDOM.nextBytes(data);
      TestUtils.RANDOM.nextBytes(encryptionKey);

      // property of blob with the infinite TTL
      BlobProperties properties = new BlobProperties(dataSize, "serviceid1", accountId, containerId, testEncryption,
          cluster.time.milliseconds());

      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      // the blob only has the PutBlob record
      BlobId putBlobId;
      // the blob is deleted but not compacted
      BlobId deletedBlobId;
      // the blob is deleted and compacted.
      BlobId compactedBlobId1;
      BlobId compactedBlobId2;
      // two new blob created after the on-demand replication is done.
      BlobId newBlobId1;
      BlobId newBlobId2;

      Port sourcePort = new Port(sourceDataNode.getHttp2Port(), PortType.HTTP2);
      Port targetPort = new Port(targetDataNode.getHttp2Port(), PortType.HTTP2);
      Port comparisonPort = new Port(comparisonNode.getHttp2Port(), PortType.HTTP2);
      ConnectedChannel sourceChannel =
          getBlockingChannelBasedOnPortType(sourcePort, "localhost", null, clientSSLConfig);
      ConnectedChannel targetChannel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", null, clientSSLConfig);
      ConnectedChannel comparisonChannel =
          getBlockingChannelBasedOnPortType(comparisonPort, "localhost", null, clientSSLConfig);
      sourceChannel.connect();
      targetChannel.connect();
      comparisonChannel.connect();

      /*
       * Stage 1: Test the ReplicateBlob under different conditions.
       */
      // 1. On the sourceDataNode putBlob. On the targetDataNode, blob doesn't exist.
      putBlobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      PutRequest putRequest = new PutRequest(1, "client1", putBlobId, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      // ReplicateBlob will replicate the putBlob to the target host.
      replicateBlob(targetChannel, putBlobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(putBlobId, targetChannel, ServerErrorCode.NoError, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);

      // 2. On the sourceDataNode blob is deleted but NOT compacted. On the targetDataNode, blob doesn't exist.
      deletedBlobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", deletedBlobId, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      deleteBlob(sourceChannel, deletedBlobId, cluster.time.milliseconds());
      // ReplicateBlob will replicate the PutBlob and deleteBlob to the target host.
      replicateBlob(targetChannel, deletedBlobId, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(deletedBlobId, targetChannel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      // We have replicated both the PutBlob and the delete Blob. Can read the Blob back with GetOption.Include_All
      getBlobAndVerify(deletedBlobId, targetChannel, ServerErrorCode.NoError, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);

      // 3. On the sourceDataNode blob is deleted and compacted. On the targetDataNode, blob doesn't exist.
      compactedBlobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      // Force delete on a blob which doesn't exist. It is used to simulate the compacted tombstone.
      short lifeVersion = 0;
      ForceDeleteAdminRequest forceDeleteAdminRequest = new ForceDeleteAdminRequest(compactedBlobId1, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      DataInputStream stream = sourceChannel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      AdminResponse adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, adminResponse.getError());
      // verify compactedBlobId1 only has a delete tombstone.
      {
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(compactedBlobId1);
        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest = new GetRequest(1, "clientId1", MessageFormatFlags.BlobInfo, partitionRequestInfoList,
            GetOption.Include_All);
        stream = sourceChannel.sendAndReceive(getRequest).getInputStream();
        GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
        releaseNettyBufUnderneathStream(stream);
        Assert.assertEquals(ServerErrorCode.BlobDeleted,
            getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
      }
      // ReplicateBlob only replicate the delete record to the target host.
      replicateBlob(targetChannel, compactedBlobId1, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(compactedBlobId1, targetChannel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      getBlobAndVerify(compactedBlobId1, targetChannel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      // On target node, ReplicateBlob writes the delete record to the target. No put blob on the target.
      // Even we read with GetOption.Include_All and MessageFormatFlags.BlobInfo, it returns Blob_Deleted
      {
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(compactedBlobId1);
        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest = new GetRequest(1, "clientId1", MessageFormatFlags.BlobInfo, partitionRequestInfoList,
            GetOption.Include_All);
        stream = targetChannel.sendAndReceive(getRequest).getInputStream();
        GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
        releaseNettyBufUnderneathStream(stream);
        Assert.assertEquals(ServerErrorCode.BlobDeleted,
            getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
      }

      // 4. On the sourceDataNode blob is deleted and compacted. On the targetDataNode, blob exists
      compactedBlobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      // Force delete compactedBlobId2 on the source host
      lifeVersion = 0;
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(compactedBlobId2, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      stream = sourceChannel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, adminResponse.getError());
      // verify compactedBlobId2 only has a delete tombstone.
      {
        ArrayList<BlobId> ids = new ArrayList<>();
        ids.add(compactedBlobId2);
        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest = new GetRequest(1, "clientId1", MessageFormatFlags.BlobInfo, partitionRequestInfoList,
            GetOption.Include_All);
        stream = sourceChannel.sendAndReceive(getRequest).getInputStream();
        GetResponse getResponse = GetResponse.readFrom(stream, clusterMap);
        releaseNettyBufUnderneathStream(stream);
        Assert.assertEquals(ServerErrorCode.BlobDeleted,
            getResponse.getPartitionResponseInfoList().get(0).getErrorCode());
      }
      // PutBlob on the target
      putRequest = new PutRequest(1, "client1", compactedBlobId2, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      // ReplicateBlob does nothing since the target node already has the PutBlob. Target still has its original PutBlob.
      replicateBlob(targetChannel, compactedBlobId2, sourceDataNode, ServerErrorCode.NoError);
      getBlobAndVerify(compactedBlobId2, targetChannel, ServerErrorCode.NoError, properties, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);

      // create a new Blob on the sourceDataNode
      newBlobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", newBlobId1, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      // create a new blob on the targetDataNode
      newBlobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", newBlobId2, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);

      // Now start the replication on all the replicas
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, true);

      // wait for the replication to catch up
      notificationSystem.awaitBlobCreations(newBlobId1.getID());
      notificationSystem.awaitBlobCreations(newBlobId2.getID());

      /*
       * Stage 2: Enable regular replication.
       * sourceDataNode and targetDataNode should be synced up after the regular replication.
       * Other replicas should catch up as well.
                       sourceDataNode targetDataNode  AfterReplicateBlob  afterReplication   ComparisonDataNode
        putBlobId         put            not_exist       put                  put                put
        deletedBlobId     put/delete     not_exist       put/delete           put/delete         not_found
        compactedBlobId1  delete         not_exist       delete               delete             not_found
        compactedBlobId2  delete         put             put                  put/delete         put/delete or not_found
        newBlobId1        put            not_exist       put                  put                put
        newBlobId2        not_exist      put             put                  put                put
       */
      for (int i = 0; i < 3; i++) {
        ConnectedChannel channel;
        if (i == 0) {
          channel = sourceChannel;
        } else if (i == 1) {
          channel = targetChannel;
        } else {
          channel = comparisonChannel;
        }

        // putBlobId
        getBlobAndVerify(putBlobId, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption, GetOption.None);

        // deletedBlobId
        if (channel != comparisonChannel) {
          getBlobAndVerify(deletedBlobId, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(deletedBlobId, channel, ServerErrorCode.NoError, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(deletedBlobId, channel, ServerErrorCode.BlobNotFound, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        // compactedBlobId1
        if (channel != comparisonChannel) {
          getBlobAndVerify(compactedBlobId1, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(compactedBlobId1, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(compactedBlobId1, channel, ServerErrorCode.BlobNotFound, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        // compactedBlobId2
        if (channel == sourceChannel) {
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else if (channel == targetChannel) {
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.NoError, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          // depending how quickly the target replicates the delete record from the source node,
          // the comparison node can have either not_found or deleted.
          ArrayList<BlobId> ids = new ArrayList<>();
          ids.add(compactedBlobId2);
          ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList,
              GetOption.Include_Deleted_Blobs);
          stream = channel.sendAndReceive(getRequest).getInputStream();
          GetResponse resp = GetResponse.readFrom(stream, clusterMap);
          ServerErrorCode error = resp.getPartitionResponseInfoList().get(0).getErrorCode();
          assertTrue(error == ServerErrorCode.NoError || error == ServerErrorCode.BlobNotFound);
          releaseNettyBufUnderneathStream(stream);
          if (error == ServerErrorCode.NoError) {
            // verify data if No_Error.
            getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
                encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
            getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.NoError, properties, userMetadata, data,
                encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
          }
        }

        // newBlobId1
        getBlobAndVerify(newBlobId1, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption, GetOption.None);

        // newBlobId2
        getBlobAndVerify(newBlobId2, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption, GetOption.None);
      } // for loop to verify the three DataNode

      sourceChannel.disconnect();
      targetChannel.disconnect();
      comparisonChannel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      assertNull(e);
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
   * Test ReplicateBlob V2 code under different conditions.
   * 1. test the data correctness of the on-demand replication on the target DataNode.
   * 2. test the interaction between the regular replication and on-demand replication.
   */
  static void replicateBlobV2CaseTest(MockCluster cluster, SSLConfig clientSSLConfig, boolean testEncryption,
      MockNotificationSystem notificationSystem) {
    List<MockDataNodeId> allNodes = cluster.getAllDataNodes();
    MockClusterMap clusterMap = cluster.getClusterMap();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = partitionIds.get(0);

    ArrayList<String> dataCenterList = new ArrayList<>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = cluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    /*
     * sourceDataNode is the data node we send PutBlob, TtlUpdate, Delete requests.
     * targetDataNode is the data node we run ReplicateBlob. targetDataNode replicates Blobs from the sourceDataNode.
     * thirdDataNode uses the regular replication thread to replicate the Blobs from peer replicas.
     */
    DataNodeId sourceDataNode = dataNodes.get(0);
    DataNodeId targetDataNode = dataNodes.get(1);
    DataNodeId thirdDataNode = dataNodes.get(2);

    try {
      // Stop replication for this partition on all the servers.
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, false);

      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      short dataSize = 31870;
      byte[] userMetadata = new byte[1000];
      byte[] data = new byte[dataSize];
      byte[] encryptionKey = new byte[100];
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      TestUtils.RANDOM.nextBytes(userMetadata);
      TestUtils.RANDOM.nextBytes(data);
      TestUtils.RANDOM.nextBytes(encryptionKey);

      // property with 7days expiration
      BlobProperties propertiesWithTtl =
          new BlobProperties(dataSize, "serviceid1", "ownerid", "image/png", false, TestUtils.TTL_SECS,
              cluster.time.milliseconds(), accountId, containerId, testEncryption, null, null, null, null);
      long ttlUpdateBlobExpiryTimeMs = getExpiryTimeMs(propertiesWithTtl);
      // property of blob with the infinite TTL
      BlobProperties properties = new BlobProperties(dataSize, "serviceid1", accountId, containerId, testEncryption,
          cluster.time.milliseconds());
      cluster.time.sleep(1000);

      short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
      BlobId blobId0 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId4 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId5 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId6 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId7 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId8 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId9 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId10 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId11 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId12 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId13 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      BlobId blobId14 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);

      Port sourcePort = new Port(sourceDataNode.getHttp2Port(), PortType.HTTP2);
      Port targetPort = new Port(targetDataNode.getHttp2Port(), PortType.HTTP2);
      Port thirdPort = new Port(thirdDataNode.getHttp2Port(), PortType.HTTP2);
      ConnectedChannel sourceChannel =
          getBlockingChannelBasedOnPortType(sourcePort, "localhost", null, clientSSLConfig);
      ConnectedChannel targetChannel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", null, clientSSLConfig);
      ConnectedChannel thirdChannel = getBlockingChannelBasedOnPortType(thirdPort, "localhost", null, clientSSLConfig);
      sourceChannel.connect();
      targetChannel.connect();
      thirdChannel.connect();

      BlobId blobId;
      long operationTime;
      PutRequest putRequest;

      /*
       * Stage 0: Verify the ODP's notification system
       */
      // On the sourceDataNode putBlob and TtlUpdate. On the targetDataNode, blob doesn't exist.
      // BlobId0 is only used to test notification System to verify ODP notification works.
      blobId = blobId0;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicateType is RequestOrResponse.TtlUpdateRequest, it replicates PutBlob and then repair the TtlUpdate
      final BlobId asyncBlob = blobId;
      final long asyncOperationTime = operationTime;
      Runnable asyncODP = () -> {
        try {
          Thread.sleep(2000);
          replicateTtlUpdate(targetChannel, asyncBlob, sourceDataNode, asyncOperationTime, Utils.Infinite_Time,
              ServerErrorCode.NoError);
        } catch (InterruptedException e) {

        } catch (IOException e) {
          assertTrue("ReplicateTtlUpdate failure " + e.getMessage(), false);
        }
      };
      new Thread(asyncODP).start();
      // wait for targetDataNode to finish
      notificationSystem.awaitBlobReplicates(blobId.getID());
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // the TtlUpdate Operation Time will be same as the PutBlob time since we apply TtlUpdate from the GetRequest.
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs(),
          Utils.Infinite_Time);
      cluster.time.sleep(1000);

      /*
       * Stage 1: Test the correctness of the ReplicateBlob. Replicate blobs from sourceDataNode to the targetDataNode
       * On the sourceDataNode and the targetDataNode, the blob is under different states.
       */
      //
      // sourceDataNode has PutBlob
      //
      // 1. On the sourceDataNode, putBlob. On the targetDataNode, blob doesn't exist. Replicate PutBlob
      blobId = blobId1;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      // replicateType is RequestOrResponse.PutRequest, return Bad_Request
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      cluster.time.sleep(1000);

      // 2. On the sourceDataNode, putBlob. On the thirdDataNode, it has the PutBlob and TtlUpdate.
      // On the targetDataNode, blob doesn't exist. Replicate TtlUpdate even source only has the PutBlob
      blobId = blobId2;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, thirdChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(thirdChannel, blobId, operationTime);
      checkTtlUpdateStatus(thirdChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicateType is RequestOrResponse.TtlUpdateRequest, it replicates PutBlob and then repair the TtlUpdate
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // cannot verify the TtlUpdate operationTime or lifeVersion with GetBlob. GetBlob returns the creation time.
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, operationTime, Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // 3. On the sourceDataNode, putBlob. On the thirdDataNode, putBlob and DeleteBlob
      // On the targetDataNode, blob doesn't exist. Repair DeleteRecord even sourceDataNode doesn't have DeleteRecord.
      blobId = blobId3;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, thirdChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      deleteBlob(thirdChannel, blobId, operationTime, ServerErrorCode.NoError);
      // replicateType is RequestOrResponse.DeleteRequest. It repair the PutRecord and the DeleteRecord.
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      checkDeleteRecord(targetChannel, clusterMap, blobId, operationTime);
      cluster.time.sleep(1000);

      // 4. On the sourceDataNode, putBlob. ThirdDataNode has DeleteRecord.
      // On the targetDataNode, blob doesn't exist. Replicate TtlUpdate even source only has the PutBlob.
      blobId = blobId4;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, thirdChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      deleteBlob(thirdChannel, blobId, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, thirdChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      // replicateType is RequestOrResponse.TtlUpdateRequest, it replicates PutBlob and then repair the TtlUpdate
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, operationTime, Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // 5. On the sourceDataNode, putBlob. ThirdDataNode has PutBlob and TtlUpdate
      // On the targetDataNode, blob doesn't exist. Repair DeleteRecord even sourceData doesn't have DeleteRecord.
      blobId = blobId5;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, thirdChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(thirdChannel, blobId, operationTime);
      checkTtlUpdateStatus(thirdChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicateType is RequestOrResponse.DeleteRequest. It repair the PutBlob and DeleteRecord.
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      checkDeleteRecord(targetChannel, clusterMap, blobId, operationTime);
      cluster.time.sleep(1000);

      //
      // sourceDataNode has PutBlob and TtlUpdate
      //
      // 6. On the sourceDataNode, the blob is ttlUpdated. On the targetDataNode, it doesn't exist.
      // ReplicateBlob will replicate both the putBlob and the TtlUpdate
      blobId = blobId6;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicate to the targetDataNode
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, -1, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // the TtlUpdate Operation Time will be same as the PutBlob time since we apply TtlUpdate from the GetRequest.
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs(),
          Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // 7. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, putBlob.
      // Run different Repair Types
      blobId = blobId7;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      // replicateType is RequestOrResponse.PutRequest, Bad_Request
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      // replicateType is RequestOrResponseType.TtlUpdateRequest, so will replicate the TtlUpdate.
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, operationTime, Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // 8. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, the blob is also ttlUpdated already.
      // Run three different Repair Types
      blobId = blobId8;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      updateBlobTtl(targetChannel, blobId, operationTime);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // replicateType is RequestOrResponse.PutRequest, do nothing
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      // replicateType is RequestOrResponseType.TtlUpdateRequest, do nothing
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, operationTime, Utils.Infinite_Time);
      // replicateType is RequestOrResponseType.DeleteRequest, will write the delete record even the source replica doesn't have it.
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
      checkDeleteRecord(targetChannel, clusterMap, blobId, operationTime);
      cluster.time.sleep(1000);

      // 9. On the sourceDatanode blob is ttlUpdated. On the targetDataNode, the blob is deleted
      // ReplicateBlob still succeeds. Nothing is changed on the local store.
      blobId = blobId9;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      long oldOperationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, oldOperationTime);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, oldOperationTime, ServerErrorCode.NoError);
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      // replicateType is RequestOrResponse.PutRequest, do nothing
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      // replicateType is RequestOrResponseType.TtlUpdateRequest, do nothing
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      // replicateType is RequestOrResponseType.DeleteRequest, do nothing
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      // opeartion time should be the oldOperationTime
      checkDeleteRecord(targetChannel, clusterMap, blobId, oldOperationTime);
      cluster.time.sleep(1000);

      //
      // souceDataNode has PutBlob, TtlUpdate and DeleteRecord.
      //
      // 10. On the sourceDataNode, the blob is deleted. On the targetDataNode, it doesn't exist.
      // ReplicateBlob replicates PutBlob, TtlUpdate and Delete.
      blobId = blobId10;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      long ttlOperationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, ttlOperationTime);
      cluster.time.sleep(1000);
      long delOperationTime = cluster.time.milliseconds();
      deleteBlob(sourceChannel, blobId, delOperationTime, ServerErrorCode.NoError);
      // replicateType is RequestOrResponseType.TtlUpdateRequest, will replicate the PutBlob, TtlUpdate and Delete since it doesn't exist
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      // The TtlUpdate and Delete operation is same as the source replica's PutBlob creation time.
      // It's not same as the source replica's TtlUpdate or Delete operation time. But it's ok.
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs(),
          Utils.Infinite_Time);
      checkDeleteRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs());
      cluster.time.sleep(1000);

      // 11. On the sourceDataNode, the blob is deleted. TargetDataNode has the PutBlob.
      // ReplicateBlob replicates the deleteBlob
      blobId = blobId11;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      ttlOperationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, ttlOperationTime);
      cluster.time.sleep(1000);
      delOperationTime = cluster.time.milliseconds();
      deleteBlob(sourceChannel, blobId, delOperationTime, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      // ReplicatePutBlob do nothing
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      // ReplicateDeleteRecord: it won't have the TtlUpdate Record.
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, delOperationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkDeleteRecord(targetChannel, clusterMap, blobId,
          delOperationTime); // should be delOperationTime not operationTime
      cluster.time.sleep(1000);

      // 12. On the sourceDataNode, the blob is deleted. On the targetDataNode, it's ttlUpdated.
      // Three ReplicateBlob Types
      blobId = blobId12;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      delOperationTime = cluster.time.milliseconds();
      deleteBlob(sourceChannel, blobId, delOperationTime, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      cluster.time.sleep(1000);
      ttlOperationTime = cluster.time.milliseconds();
      updateBlobTtl(targetChannel, blobId, ttlOperationTime);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // ReplicatePut do nothing
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // ReplicateTtlUpdate, it already has the TtlUpdate. Do nothing.
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // the timestamp should be the ttlOperationTime
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, ttlOperationTime, Utils.Infinite_Time);
      // ReplicateDelete, it repair the DeleteRecord.
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, delOperationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkDeleteRecord(targetChannel, clusterMap, blobId, delOperationTime);
      cluster.time.sleep(1000);

      // 13. On the sourceDataNode, the blob is deleted. On the targetDataNode, it's also deleted.
      // ReplicateBlob still succeeds
      blobId = blobId13;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      delOperationTime = cluster.time.milliseconds();
      deleteBlob(sourceChannel, blobId, delOperationTime, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      deleteBlob(targetChannel, blobId, delOperationTime, ServerErrorCode.NoError);
      // do nothing for the three repair types
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.NoError);
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      // operationTime should be delOperationTime
      checkDeleteRecord(targetChannel, clusterMap, blobId, delOperationTime);
      cluster.time.sleep(1000);

      // 14. On the sourceDataNode, the blob doesn't exist.
      // ReplicateBlob with DeleteOperation is successful
      blobId = blobId14;
      operationTime = cluster.time.milliseconds();
      replicatePutBlob(targetChannel, blobId, sourceDataNode, ServerErrorCode.BadRequest);
      replicateTtlUpdate(targetChannel, blobId, sourceDataNode, operationTime, Utils.Infinite_Time,
          ServerErrorCode.BlobNotFound);
      replicateDeleteBlob(targetChannel, blobId, sourceDataNode, operationTime, ServerErrorCode.NoError);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      cluster.time.sleep(1000);

      // 15. On the sourceDataNode blob is deleted and compacted. On the targetDataNode, blob doesn't exist.
      BlobId compactedBlobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      // Force delete compactedBlobId1 on source replica. It is used to simulate the compacted tombstone.
      short lifeVersion = 0;
      ForceDeleteAdminRequest forceDeleteAdminRequest = new ForceDeleteAdminRequest(compactedBlobId1, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid"));
      DataInputStream stream = sourceChannel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      AdminResponse adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, adminResponse.getError());
      // verify compactedBlobId1 only has a delete tombstone.
      getBlobAndVerify(compactedBlobId1, sourceChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
          data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      MessageInfo sourceDelInfo = getDeleteRecord(sourceChannel, clusterMap, compactedBlobId1);
      delOperationTime = sourceDelInfo.getOperationTimeMs();
      // ReplicateBlob only replicate the delete record to the target host.
      replicateDeleteBlob(targetChannel, compactedBlobId1, sourceDataNode, delOperationTime, ServerErrorCode.NoError);
      getBlobAndVerify(compactedBlobId1, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
          data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      checkDeleteRecord(targetChannel, clusterMap, compactedBlobId1, delOperationTime);

      // 16. On the sourceDataNode blob is deleted and compacted. On the targetDataNode, blob exists
      BlobId compactedBlobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      // Force delete compactedBlobId2 on the source host
      lifeVersion = 0;
      forceDeleteAdminRequest = new ForceDeleteAdminRequest(compactedBlobId2, lifeVersion,
          new AdminRequest(AdminRequestOrResponseType.ForceDelete, partitionIds.get(0), 1, "clientid2"));
      stream = sourceChannel.sendAndReceive(forceDeleteAdminRequest).getInputStream();
      adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals(ServerErrorCode.NoError, adminResponse.getError());
      // verify compactedBlobId2 only has a delete tombstone.
      getBlobAndVerify(compactedBlobId2, sourceChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
          data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      sourceDelInfo = getDeleteRecord(sourceChannel, clusterMap, compactedBlobId2);
      delOperationTime = sourceDelInfo.getOperationTimeMs();
      // PutBlob on the target
      putRequest = new PutRequest(1, "client1", compactedBlobId2, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      // ReplicateBlob only replicate the delete record to the target host.
      replicateDeleteBlob(targetChannel, compactedBlobId2, sourceDataNode, delOperationTime, ServerErrorCode.NoError);
      getBlobAndVerify(compactedBlobId2, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      checkDeleteRecord(targetChannel, clusterMap, compactedBlobId2, delOperationTime);

      /*
       * Stage 2: Enable regular replication.
       * sourceDataNode, targetDataNode and thirdDataNode should be synced up after the regular replication.
                        sourceDataNode targetDataNode   thirdDataNode   targetAfterODR  sourceA/target/thirdAfter
        blobId1              put           not_exist        not_exist         not_exist       put
        blobId2              put           not_exist        put/ttl           put/ttl         ttl
        blobId3              put           not_exist        put/delete        put/delete      delete
        blobId4              put           not_exist        put/delete        put/ttl         delete
        blobId5              put           not_exist        put/ttl           put/delete      delete

        blobId6            put/ttl         not_exist        not_exist         put/ttl         ttl
        blobId7            put/ttl         put              not_exist         put/ttl         ttl
        blobId8            put/ttl         put/ttl          not_exist         put/ttl/del     deleted or not_exist
        blobId9            put/ttl         put/del          not_exist         put/del         deleted or not_exist

        blobId10           put/ttl/del     not_exist        not_exist         put/ttl/del     deleted or not_exist
        blobId11           put/ttl/del     put              not_exist         put/del         deleted or not_exist
        blobId12           put/del         put/ttl          not_exist         put/ttl/del     deleted or not_exist
        blobId13           put/del         put/del          not_exist         put/del         deleted or not_exist

        blobId14           not_exist       not_exist        deleted           not_exist       not_exist
        compactedBlobId1   tombstone       not_exist        not_exist         del             not_exist or del
        compactedBlobId2   tombstone       put              not_exist         put/del         not_exist or del
       */

      // create a new Blob on the sourceDataNode which is TtlUpdated.
      BlobId newBlobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", newBlobId1, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      long newBlobTtlOperationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, newBlobId1, newBlobTtlOperationTime);
      cluster.time.sleep(1000);

      // create a new blob on the targetDataNode which is deleted.
      BlobId newBlobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", newBlobId2, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      long newBlobDeleteOperationTime = cluster.time.milliseconds();
      deleteBlob(targetChannel, newBlobId2, newBlobDeleteOperationTime, ServerErrorCode.NoError);
      // create the same blob on sourceDataNode as well. So regular replication will replicate the Delete.
      putRequest = new PutRequest(1, "client1", newBlobId2, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      cluster.time.sleep(1000);

      // create a new blob on the thirdDataNode
      BlobId newBlobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
          properties.getAccountId(), properties.getContainerId(), partitionId, testEncryption,
          BlobId.BlobDataType.DATACHUNK);
      putRequest = new PutRequest(1, "client1", newBlobId3, properties, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, thirdChannel, ServerErrorCode.NoError);
      cluster.time.sleep(1000);

      // Now start the replication on all the replicas
      controlReplicationForPartition(allNodes, partitionId, clientSSLConfig, null, true);

      // wait for the replication to catch up
      notificationSystem.awaitBlobUpdates(newBlobId1.getID(), UpdateType.TTL_UPDATE);
      notificationSystem.awaitBlobDeletions(newBlobId2.getID());
      notificationSystem.awaitBlobCreations(newBlobId3.getID());

      for (int i = 0; i < 3; i++) {
        ConnectedChannel channel;
        if (i == 0) {
          channel = sourceChannel;
        } else if (i == 1) {
          channel = targetChannel;
        } else {
          channel = thirdChannel;
        }

        getBlobAndVerify(blobId1, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);

        getBlobAndVerify(blobId2, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId2, data, true, Utils.Infinite_Time);

        getBlobAndVerify(blobId3, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);

        getBlobAndVerify(blobId4, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);

        getBlobAndVerify(blobId5, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);

        getBlobAndVerify(blobId6, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId6, data, true, Utils.Infinite_Time);

        getBlobAndVerify(blobId7, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
            encryptionKey, clusterMap, blobIdFactory, testEncryption);
        checkTtlUpdateStatus(channel, clusterMap, blobIdFactory, blobId7, data, true, Utils.Infinite_Time);

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId8, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId8, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId8, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId9, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId9, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId9, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId10, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId10, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId10, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId11, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId11, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId11, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId12, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId12, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId12, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel != thirdChannel) {
          getBlobAndVerify(blobId13, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(blobId13, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(blobId13, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel == targetChannel) {
          getBlobAndVerify(blobId14, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        } else {
          getBlobAndVerify(blobId14, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel == targetChannel || channel == sourceChannel) {
          getBlobAndVerify(compactedBlobId1, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
              data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(compactedBlobId1, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata,
              data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        if (channel == sourceChannel) {
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
              data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else if (channel == targetChannel) {
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata,
              data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_Deleted_Blobs);
        } else {
          getBlobAndVerify(compactedBlobId2, channel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata,
              data, encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
        }

        // newBlobId1
        getBlobAndVerify(newBlobId1, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption, GetOption.None);
        if (channel == sourceChannel) {
          checkTtlUpdateRecord(channel, clusterMap, newBlobId1, newBlobTtlOperationTime, Utils.Infinite_Time);
        } else {
          // It the local replica doesn't have the PutBlob, when the replication thread replicate the PutBlob,
          // the ttlupdate operation time is same as the creation time.
          checkTtlUpdateRecord(channel, clusterMap, newBlobId1, properties.getCreationTimeInMs(), Utils.Infinite_Time);
        }

        // newBlobId2
        if (channel == targetChannel || channel == sourceChannel) {
          // If the local store has the PutBlob, when we replicate the TtlUpdate in the background thread, we preserve the operationTime.
          getBlobAndVerify(newBlobId2, channel, ServerErrorCode.BlobDeleted, properties, userMetadata, data,
              encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
          checkDeleteRecord(channel, clusterMap, newBlobId2, newBlobDeleteOperationTime);
        } else {
          // third channel can be NOT_FOUND or Deleted.
        }

        getBlobAndVerify(newBlobId3, channel, ServerErrorCode.NoError, properties, userMetadata, data, encryptionKey,
            clusterMap, blobIdFactory, testEncryption, GetOption.None);
      } // for loop to verify the three DataNode

      sourceChannel.disconnect();
      targetChannel.disconnect();
      thirdChannel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      assertNull(e);
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

  private static void controlReplicationForPartition(List<ConnectedChannel> channels, PartitionId partitionId,
      boolean enable) throws Exception {
    for (ConnectedChannel connectedChannel : channels) {
      AdminRequest adminRequest =
          new AdminRequest(AdminRequestOrResponseType.ReplicationControl, partitionId, 1, "clientid2");
      ReplicationControlAdminRequest controlRequest =
          new ReplicationControlAdminRequest(Collections.emptyList(), enable, adminRequest);
      DataInputStream stream = connectedChannel.sendAndReceive(controlRequest).getInputStream();
      AdminResponse adminResponse = AdminResponse.readFrom(stream);
      releaseNettyBufUnderneathStream(stream);
      Assert.assertEquals("Stop store admin request should succeed", ServerErrorCode.NoError, adminResponse.getError());
    }
  }

  /**
   * Enable or disable replication on the replicas
   * @param allNodes the nodes to send the replication control requests.
   * @param partitionId the partition that the control request is targeting.
   * @param clientSSLConfig the client {@link SSLConfig}
   * @param clientSSLSocketFactory the {@link SSLSocketFactory}
   * @param enable if true, enable the replication. If false, disable the replication
   * @throws Exception any exception
   */
  public static void controlReplicationForPartition(List<MockDataNodeId> allNodes, PartitionId partitionId,
      SSLConfig clientSSLConfig, SSLSocketFactory clientSSLSocketFactory, boolean enable) throws Exception {
    List<ConnectedChannel> channels = new ArrayList<>();
    for (DataNodeId node : allNodes) {
      Port port = new Port(node.getHttp2Port(), PortType.HTTP2);
      ConnectedChannel channel =
          getBlockingChannelBasedOnPortType(port, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      channels.add(channel);
    }
    controlReplicationForPartition(channels, partitionId, enable);
    for (ConnectedChannel channel : channels) {
      channel.disconnect();
    }
  }

  /**
   * Enable or disable replication on the replicas
   * @param channels the connection channel to the replicas.
   * @param partitionIds the partitions that the control requests are targeting.
   * @param enable if true, enable the replication. If false, disable the replication
   * @throws Exception any exception
   */
  public static void controlReplicationForPartition(List<ConnectedChannel> channels, List<PartitionId> partitionIds,
      boolean enable) throws Exception {
    for (PartitionId id : partitionIds) {
      controlReplicationForPartition(channels, id, enable);
    }
  }

  /**
   * Enable or disable the BlobStore on the replica
   * @param channel the connection channel to the replica.
   * @param partitionId the partitions that the control request is targeting.
   * @param enable if true, enable the blob store. If false, disable the blob store
   * @throws Exception any exception
   */
  public static void controlBlobStore(ConnectedChannel channel, PartitionId partitionId, boolean enable)
      throws Exception {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, partitionId, 1, "clientid");
    BlobStoreControlAdminRequest controlRequest = new BlobStoreControlAdminRequest((short) 0,
        enable ? BlobStoreControlAction.StartStore : BlobStoreControlAction.StopStore, adminRequest);
    DataInputStream stream = channel.sendAndReceive(controlRequest).getInputStream();
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("start/Stop store admin request should succeed", ServerErrorCode.NoError,
        adminResponse.getError());
  }

  private static List<PartitionRequestInfo> getPartitionRequestInfoListFromBlobId(BlobId blobId) {
    return Collections.singletonList(
        new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId)));
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
          + MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(properties)
          + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(usermetadata)
          + MessageFormatRecord.Blob_Format_V3.getBlobRecordSize(data.length);
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
    DataInputStream stream = channel.sendAndReceive(getRequest3).getInputStream();
    GetResponse resp = GetResponse.readFrom(stream, clusterMap);
    Assert.assertEquals(ServerErrorCode.NoError, resp.getError());
    BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = getBlobData(blobData);
    assertArrayEquals(dataToCheck, blobout);
    if (encryptionKey != null) {
      assertNotNull("EncryptionKey should not have been null",
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
      Assert.assertArrayEquals("EncryptionKey mismatch", encryptionKey,
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0).getEncryptionKey().array());
    } else {
      assertNull("EncryptionKey should have been null",
          resp.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
    }
    releaseNettyBufUnderneathStream(stream);
  }

  /**
   * get the Blob and verify the BlobProperties, userMetadata and content.
   */
  private static void getBlobAndVerify(BlobId blobId, ConnectedChannel channel, ServerErrorCode expectedErrorCode,
      BlobProperties expectedProperties, byte[] expectedUserMetadata, byte[] expectedData, byte[] expectedEncryptionKey,
      MockClusterMap clusterMap, BlobIdFactory blobIdFactory, boolean testEncryption, GetOption getOption)
      throws Exception {
    ArrayList<BlobId> listIds = new ArrayList<>();
    listIds.add(blobId);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), listIds);
    partitionRequestInfoList.add(partitionRequestInfo);

    // get blob all
    GetRequest getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, getOption);
    DataInputStream stream = channel.sendAndReceive(getRequest).getInputStream();
    GetResponse response = GetResponse.readFrom(stream, clusterMap);

    Assert.assertEquals(ServerErrorCode.NoError, response.getError());
    Assert.assertEquals(expectedErrorCode, response.getPartitionResponseInfoList().get(0).getErrorCode());
    if (expectedErrorCode != ServerErrorCode.NoError) {
      releaseNettyBufUnderneathStream(stream);
      return;
    }

    InputStream responseStream = response.getInputStream();
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, blobIdFactory);

    // verify the BlobProperties
    BlobProperties propertyOutput = blobAll.getBlobInfo().getBlobProperties();
    assertTrue(expectedProperties.contentEquals(propertyOutput));

    // verify the user metadata
    byte[] userMetadataOutput = blobAll.getBlobInfo().getUserMetadata();
    assertArrayEquals(expectedUserMetadata, userMetadataOutput);

    // verify the content
    byte[] actualBlobData = getBlobData(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", expectedData, actualBlobData);
    if (testEncryption) {
      assertNotNull("EncryptionKey should not ne null", blobAll.getBlobEncryptionKey());
      assertArrayEquals("EncryptionKey mismatch", expectedEncryptionKey, blobAll.getBlobEncryptionKey().array());
    } else {
      assertNull("EncryptionKey should have been null", blobAll.getBlobEncryptionKey());
    }
    releaseNettyBufUnderneathStream(stream);
  }

  /**
   * get the Blob and verify the BlobProperties, userMetadata and content.
   */
  private static void getBlobAndVerify(BlobId blobId, ConnectedChannel channel, ServerErrorCode expectedErrorCode,
      BlobProperties expectedProperties, byte[] expectedUserMetadata, byte[] expectedData, byte[] expectedEncryptionKey,
      MockClusterMap clusterMap, BlobIdFactory blobIdFactory, boolean testEncryption) throws Exception {
    getBlobAndVerify(blobId, channel, expectedErrorCode, expectedProperties, expectedUserMetadata, expectedData,
        expectedEncryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.None);
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
   * putBlob and verify the return status
   * @param putRequest the {@link PutRequest}
   * @param channel the {@link ConnectedChannel} to send the request and receive the response
   * @param expectedErrorCode expected server error code.
   * @throws IOException
   */
  static void putBlob(PutRequest putRequest, ConnectedChannel channel, ServerErrorCode expectedErrorCode)
      throws IOException {
    DataInputStream putResponseStream = channel.sendAndReceive(putRequest).getInputStream();
    PutResponse response = PutResponse.readFrom(putResponseStream);
    releaseNettyBufUnderneathStream(putResponseStream);
    Assert.assertEquals(expectedErrorCode, response.getError());
  }

  /**
   * Updates the TTL of the given {@code blobId}
   * @param channel the {@link ConnectedChannel} to make the {@link GetRequest} on.
   * @param blobId the ID of the blob to TTL update
   * @param ts the operation time
   * @param expectedError expected error code
   * @throws IOException
   */
  static void updateBlobTtl(ConnectedChannel channel, BlobId blobId, long ts, ServerErrorCode expectedError)
      throws IOException {
    TtlUpdateRequest ttlUpdateRequest = new TtlUpdateRequest(1, "updateBlobTtl", blobId, Utils.Infinite_Time, ts);
    DataInputStream stream = channel.sendAndReceive(ttlUpdateRequest).getInputStream();
    TtlUpdateResponse ttlUpdateResponse = TtlUpdateResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Unexpected ServerErrorCode for TtlUpdateRequest", expectedError, ttlUpdateResponse.getError());
  }

  public static void updateBlobTtl(ConnectedChannel channel, BlobId blobId, long ts) throws IOException {
    updateBlobTtl(channel, blobId, ts, ServerErrorCode.NoError);
  }

  static void deleteBlob(ConnectedChannel channel, BlobId blobId, long ts) throws Exception {
    deleteBlob(channel, blobId, ts, ServerErrorCode.NoError);
  }

  static void deleteBlob(ConnectedChannel channel, BlobId blobId, long ts, ServerErrorCode expectedErrorCode)
      throws Exception {
    DeleteRequest deleteRequest = new DeleteRequest(1, "deleteClient", blobId, ts);
    DataInputStream stream = channel.sendAndReceive(deleteRequest).getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Unexpected ServerErrorCode for DeleteRequest", expectedErrorCode, deleteResponse.getError());
  }

  static void undeleteBlob(ConnectedChannel channel, BlobId blobId, long ts, short lifeVersion) throws Exception {
    undeleteBlob(channel, blobId, ts, ServerErrorCode.NoError, lifeVersion);
  }

  static void undeleteBlob(ConnectedChannel channel, BlobId blobId, long ts, ServerErrorCode expectedErrorCode,
      short lifeVersion) throws Exception {
    UndeleteRequest undeleteRequest = new UndeleteRequest(1, "undeleteClient", blobId, ts);
    DataInputStream stream = channel.sendAndReceive(undeleteRequest).getInputStream();
    UndeleteResponse undeleteResponse = UndeleteResponse.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals("Unexpected ServerErrorCode for UndeleteRequest", expectedErrorCode, undeleteResponse.getError());
    if (undeleteResponse.getError() == ServerErrorCode.NoError
        || undeleteResponse.getError() == ServerErrorCode.BlobAlreadyUndeleted) {
      Assert.assertEquals(lifeVersion, undeleteResponse.getLifeVersion());
    }
  }

  /**
   * replicate the blob and verify the return status
   * @param channel the {@link ConnectedChannel} to send the request and receive the response
   * @param blobId the blob id
   * @param sourceDataNode the source {@link DataNodeId} to replicate the blob from
   * @param expectedErrorCode the expected server error code
   * @throws IOException any IO exception
   */
  static void replicateBlob(ConnectedChannel channel, BlobId blobId, DataNodeId sourceDataNode,
      ServerErrorCode expectedErrorCode) throws IOException {
    ReplicateBlobRequest replicateBlobRequest =
        new ReplicateBlobRequest(1, "replicateBlobClient", blobId, sourceDataNode.getHostname(),
            sourceDataNode.getPort());
    DataInputStream replicateBlobResponseStream = channel.sendAndReceive(replicateBlobRequest).getInputStream();
    ReplicateBlobResponse replicateBlobResponse = ReplicateBlobResponse.readFrom(replicateBlobResponseStream);
    releaseNettyBufUnderneathStream(replicateBlobResponseStream);
    Assert.assertEquals(expectedErrorCode, replicateBlobResponse.getError());
  }

  static void replicateBlobV2(ConnectedChannel channel, BlobId blobId, DataNodeId sourceDataNode,
      RequestOrResponseType operationType, long operationTime, short lifeVersion, long expirationTime,
      ServerErrorCode expectedErrorCode) throws IOException {
    ReplicateBlobRequest replicateBlobRequest =
        new ReplicateBlobRequest(1, "replicateBlobClient", blobId, sourceDataNode.getHostname(),
            sourceDataNode.getPort(), operationType, operationTime, lifeVersion, expirationTime);
    DataInputStream replicateBlobResponseStream = channel.sendAndReceive(replicateBlobRequest).getInputStream();
    ReplicateBlobResponse replicateBlobResponse = ReplicateBlobResponse.readFrom(replicateBlobResponseStream);
    releaseNettyBufUnderneathStream(replicateBlobResponseStream);
    Assert.assertEquals(expectedErrorCode, replicateBlobResponse.getError());
  }

  static void replicatePutBlob(ConnectedChannel channel, BlobId blobId, DataNodeId sourceDataNode,
      ServerErrorCode expectedErrorCode) throws IOException {
    replicateBlobV2(channel, blobId, sourceDataNode, RequestOrResponseType.PutRequest, 0, (short) 0, 0,
        expectedErrorCode);
  }

  static void replicateTtlUpdate(ConnectedChannel channel, BlobId blobId, DataNodeId sourceDataNode, long operationTime,
      long expirationTime, ServerErrorCode expectedErrorCode) throws IOException {
    short lifeVersion = -1;
    replicateBlobV2(channel, blobId, sourceDataNode, RequestOrResponseType.TtlUpdateRequest, operationTime, lifeVersion,
        expirationTime, expectedErrorCode);
  }

  static void replicateDeleteBlob(ConnectedChannel channel, BlobId blobId, DataNodeId sourceDataNode,
      long operationTime, ServerErrorCode expectedErrorCode) throws IOException {
    short lifeVersion = -1;
    replicateBlobV2(channel, blobId, sourceDataNode, RequestOrResponseType.DeleteRequest, operationTime, lifeVersion, 0,
        expectedErrorCode);
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
    DataInputStream stream = channel.sendAndReceive(request).getInputStream();
    GetResponse response = GetResponse.readFrom(stream, clusterMap);
    BlobProperties blobProperties = MessageFormatRecord.deserializeBlobProperties(response.getInputStream());
    if (!ttlUpdated) {
      assertEquals("TTL does not match", expectedExpiryTimeMs, getExpiryTimeMs(blobProperties));
    }
    MessageInfo messageInfo = response.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    assertEquals("Blob ID not as expected", blobId, messageInfo.getStoreKey());
    assertEquals("TTL update state not as expected", ttlUpdated, messageInfo.isTtlUpdated());
    assertEquals("Expiry time is not as expected", expectedExpiryTimeMs, messageInfo.getExpirationTimeInMs());
    releaseNettyBufUnderneathStream(stream);
    // blob all
    request = new GetRequest(1, "checkTtlUpdateStatus", MessageFormatFlags.All, requestInfos, GetOption.None);
    stream = channel.sendAndReceive(request).getInputStream();
    response = GetResponse.readFrom(stream, clusterMap);
    InputStream responseStream = response.getInputStream();
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(responseStream, storeKeyFactory);
    byte[] actualBlobData = getBlobData(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", expectedBlobData, actualBlobData);
    messageInfo = response.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    assertEquals("Blob ID not as expected", blobId, messageInfo.getStoreKey());
    assertEquals("TTL update state not as expected", ttlUpdated, messageInfo.isTtlUpdated());
    assertEquals("Expiry time is not as expected", expectedExpiryTimeMs, messageInfo.getExpirationTimeInMs());
    releaseNettyBufUnderneathStream(stream);
  }

  static void checkTtlUpdateRecord(ConnectedChannel channel, ClusterMap clusterMap, BlobId blobId,
      long expectedOperationTime, long expectedExpiryTimeMs) throws IOException {
    // Use BlobIndexAdminRequest to verify we have the right TtlUpdate record
    BlobIndexAdminRequest blobIndexAdminRequest = new BlobIndexAdminRequest(blobId,
        new AdminRequest(AdminRequestOrResponseType.BlobIndex, blobId.getPartition(), 1, "clientid2"));
    DataInputStream stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
    AdminResponseWithContent adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals(new String(adminResponseWithContent.getContent()), ServerErrorCode.NoError,
        adminResponseWithContent.getError());
    byte[] jsonBytes = adminResponseWithContent.getContent();
    ObjectMapper objectMapper = new ObjectMapper();
    StoreKeyJacksonConfig.setupObjectMapper(objectMapper, new BlobIdFactory(clusterMap));
    Map<String, MessageInfo> messages =
        objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
        });
    // find the TtlUpdate record
    MessageInfo ttlUpdateRecord = null;
    for (MessageInfo mg : messages.values()) {
      if (mg.isTtlUpdated() && !mg.isDeleted()) {
        ttlUpdateRecord = mg;
        break;
      }
    }
    assertEquals(blobId.getAccountId(), ttlUpdateRecord.getAccountId());
    assertEquals(blobId.getContainerId(), ttlUpdateRecord.getContainerId());
    assertEquals(false, ttlUpdateRecord.isDeleted());
    assertEquals(true, ttlUpdateRecord.isTtlUpdated());
    assertEquals(false, ttlUpdateRecord.isUndeleted());
    assertEquals(false, ttlUpdateRecord.isExpired());
    assertEquals(Utils.getTimeInMsToTheNearestSec(expectedExpiryTimeMs), ttlUpdateRecord.getExpirationTimeInMs());
    assertEquals(0, ttlUpdateRecord.getLifeVersion());
    assertEquals(Utils.getTimeInMsToTheNearestSec(expectedOperationTime), ttlUpdateRecord.getOperationTimeMs());
    assertEquals(null, ttlUpdateRecord.getCrc());
  }

  static MessageInfo getDeleteRecord(ConnectedChannel channel, ClusterMap clusterMap, BlobId blobId)
      throws IOException {
    // Use BlobIndexAdminRequest to verify we have the right delete record
    BlobIndexAdminRequest blobIndexAdminRequest = new BlobIndexAdminRequest(blobId,
        new AdminRequest(AdminRequestOrResponseType.BlobIndex, blobId.getPartition(), 1, "clientid2"));
    DataInputStream stream = channel.sendAndReceive(blobIndexAdminRequest).getInputStream();
    AdminResponseWithContent adminResponseWithContent = AdminResponseWithContent.readFrom(stream);
    releaseNettyBufUnderneathStream(stream);
    Assert.assertEquals(new String(adminResponseWithContent.getContent()), ServerErrorCode.NoError,
        adminResponseWithContent.getError());
    byte[] jsonBytes = adminResponseWithContent.getContent();
    ObjectMapper objectMapper = new ObjectMapper();
    StoreKeyJacksonConfig.setupObjectMapper(objectMapper, new BlobIdFactory(clusterMap));
    Map<String, MessageInfo> messages =
        objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
        });
    MessageInfo deleteRecordInfo = null;
    // find the delete record
    for (MessageInfo mg : messages.values()) {
      if (mg.isDeleted()) {
        deleteRecordInfo = mg;
        break;
      }
    }
    return deleteRecordInfo;
  }

  static void checkDeleteRecord(ConnectedChannel channel, ClusterMap clusterMap, BlobId blobId, long operationTime)
      throws IOException {
    MessageInfo deleteRecordInfo = getDeleteRecord(channel, clusterMap, blobId);

    assertTrue(deleteRecordInfo != null);
    assertEquals(blobId.getAccountId(), deleteRecordInfo.getAccountId());
    assertEquals(blobId.getContainerId(), deleteRecordInfo.getContainerId());
    assertEquals(true, deleteRecordInfo.isDeleted());
    // deleteRecordInfo.isTtlUpdated() can be true or false depending on if we have TtlUpdate before it.
    assertEquals(false, deleteRecordInfo.isUndeleted());
    assertEquals(false, deleteRecordInfo.isExpired());
    // expiration time can be valid value or -1 depending on the previous operation.
    assertEquals(0, deleteRecordInfo.getLifeVersion());
    assertEquals(Utils.getTimeInMsToTheNearestSec(operationTime), deleteRecordInfo.getOperationTimeMs());
    assertEquals(null, deleteRecordInfo.getCrc());
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
      channel = new Http2BlockingChannel(hostName, targetPort.getPort(), sslConfig,
          new Http2ClientConfig(new VerifiableProperties(new Properties())),
          new Http2ClientMetrics(new MetricRegistry()));
    }
    return channel;
  }

  /**
   * Returns BlockingChannel, SSLBlockingChannel or Http2BlockingChannel depending on whether the port type is PlainText,
   * SSL or HTTP2 port for the given targetPort
   * @param portType The type of port to connect to
   * @param dataNodeId To which {@link MockDataNodeId} to connect
   * @param sslSocketFactory the {@link SSLSocketFactory}.
   * @param sslConfig the {@link SSLConfig}.
   * @return ConnectedChannel
   */
  private static ConnectedChannel getBlockingChannelBasedOnPortType(PortType portType, DataNodeId dataNodeId,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    ConnectedChannel channel = null;
    String hostName = dataNodeId.getHostname();
    if (portType == PortType.PLAINTEXT) {
      channel = new BlockingChannel(hostName, dataNodeId.getPort(), 10000, 10000, 10000, 2000);
    } else if (portType == PortType.SSL) {
      channel =
          new SSLBlockingChannel(hostName, dataNodeId.getSSLPort(), new MetricRegistry(), 10000, 10000, 10000, 4000,
              sslSocketFactory, sslConfig);
    } else if (portType == PortType.HTTP2) {
      channel = new Http2BlockingChannel(hostName, dataNodeId.getHttp2Port(), sslConfig,
          new Http2ClientConfig(new VerifiableProperties(new Properties())),
          new Http2ClientMetrics(new MetricRegistry()));
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
  public static PutMessageFormatInputStream getPutMessageInputStreamForBlob(BlobId blobId, int blobSize,
      Map<BlobId, Integer> blobIdToSizeMap, short accountId, short containerId) throws Exception {
    int userMetaDataSize = 100;
    byte[] userMetadata = new byte[userMetaDataSize];
    TestUtils.RANDOM.nextBytes(userMetadata);
    byte[] data = new byte[blobSize];
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, Utils.Infinite_Time, accountId, containerId,
            false, null, null, null, null);
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
  public static List<BlobId> createBlobIds(int blobCount, ClusterMap clusterMap, short accountId, short containerId,
      PartitionId partitionId) {
    List<BlobId> blobIds = new ArrayList<>(blobCount);
    for (int i = 0; i < blobCount; i++) {
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          clusterMap.getLocalDatacenterId(), accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
      blobIds.add(blobId);
    }
    return blobIds;
  }

  /*
   * Release the Netty ByteBuf if the stream is NettyByteBufDataInputStream
   * @param stream the input stream.
   */
  public static void releaseNettyBufUnderneathStream(DataInputStream stream) {
    if (stream instanceof NettyByteBufDataInputStream) {
      // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
      ((NettyByteBufDataInputStream) stream).getBuffer().release();
    }
  }

  /**
   * Test the background RepairRequestsSender and the repair requests handlers.
   */
  static void repairRequestTest(MockCluster cluster, SSLConfig clientSSLConfig, boolean testEncryption,
      MockNotificationSystem notificationSystem) throws Exception {
    List<MockDataNodeId> allNodes = cluster.getAllDataNodes();
    MockClusterMap clusterMap = cluster.getClusterMap();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);

    /*
     * sourceDataNode is the source datanode which successfully acks the requests.
     * targetDataNode is the datanode which was temporarily unavailable and then gets back to fix the requests.
     * thirdDataNode is the third data node which was temporarily unavailable for a while.
     */
    DataNodeId sourceDataNode = allNodes.get(0);
    DataNodeId targetDataNode = allNodes.get(1);
    DataNodeId thirdDataNode = allNodes.get(2);
    PartitionId partitionId1 = partitionIds.get(0);
    PartitionId partitionId2 = partitionIds.get(1);
    ConnectedChannel sourceChannel = null;
    ConnectedChannel targetChannel = null;
    ConnectedChannel thirdChannel = null;

    // Open the db connection and use it to generate repair requests.
    MysqlRepairRequestsDb db = createRepairRequestsConnection(sourceDataNode.getDatacenterName(), cluster.time);

    // open connection channel to all data nodes.
    List<ConnectedChannel> channels = new ArrayList<>();
    for (DataNodeId node : allNodes) {
      Port port = new Port(node.getHttp2Port(), PortType.HTTP2);
      ConnectedChannel channel = getBlockingChannelBasedOnPortType(port, "localhost", null, clientSSLConfig);
      channel.connect();
      channels.add(channel);
      if (channel.getRemoteHost().equals(sourceDataNode.getHostname())
          && channel.getRemotePort() == sourceDataNode.getPortToConnectTo().getPort()) {
        sourceChannel = channel;
      } else if (channel.getRemoteHost().equals(targetDataNode.getHostname())
          && channel.getRemotePort() == targetDataNode.getPortToConnectTo().getPort()) {
        targetChannel = channel;
      } else if (channel.getRemoteHost().equals(thirdDataNode.getHostname())
          && channel.getRemotePort() == thirdDataNode.getPortToConnectTo().getPort()) {
        thirdChannel = channel;
      }
    }

    // stop replication on all data nodes
    controlReplicationForPartition(channels, partitionIds, false);
    // stop blobs store on all data nodes except for the sourceChannel and the targetChannel. So we can monitor the ODR on the target node.
    for (ConnectedChannel channel : channels) {
      if (channel == sourceChannel || channel == targetChannel) {
        continue;
      }
      for (PartitionId id : partitionIds) {
        controlBlobStore(channel, id, false);
      }
    }

    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    short dataSize = 31870;
    byte[] userMetadata = new byte[1000];
    byte[] data = new byte[dataSize];
    byte[] encryptionKey = new byte[100];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);
    TestUtils.RANDOM.nextBytes(encryptionKey);

    // property with 7days expiration
    BlobProperties propertiesWithTtl =
        new BlobProperties(dataSize, "serviceid1", "ownerid", "image/png", false, TestUtils.TTL_SECS,
            cluster.time.milliseconds(), accountId, containerId, testEncryption, null, null, null, null);
    long ttlUpdateBlobExpiryTimeMs = getExpiryTimeMs(propertiesWithTtl);
    // property of blob with the infinite TTL
    BlobProperties properties =
        new BlobProperties(dataSize, "serviceid1", accountId, containerId, testEncryption, cluster.time.milliseconds());
    cluster.time.sleep(1000);

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionId1, testEncryption,
        BlobId.BlobDataType.DATACHUNK);
    BlobId blobId2 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionId1, testEncryption,
        BlobId.BlobDataType.DATACHUNK);
    BlobId blobId3 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionId1, testEncryption,
        BlobId.BlobDataType.DATACHUNK);
    BlobId expiredId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionId2, testEncryption,
        BlobId.BlobDataType.DATACHUNK);
    int numOfRecordsCannotFix = 15; // have set query pagination count to 7.
    int numOfSuccessFix = 27;
    int randomTestNum = 39;

    BlobId blobId;
    long operationTime;
    PutRequest putRequest;

    try {
      /*
       * Stage 1: Test the correctness of the ReplicateBlob. Replicate blobs from sourceDataNode to the targetDataNode
       * On the sourceDataNode and the targetDataNode, the blob is under different states.
       */
      //
      // sourceDataNode has PutBlob and TtlUpdate
      //
      // 1. On the sourceDataNode, the blob is ttlUpdated. On the targetDataNode, it doesn't exist.
      // Add TtlUpdate RepairRequest to the db.
      // ReplicateBlob will replicate both the putBlob and the TtlUpdate
      blobId = blobId1;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      addRepairRequestForTtlUpdate(db, sourceDataNode, blobId, operationTime, Utils.Infinite_Time);
      // wait for the replication to catch up
      notificationSystem.awaitBlobReplicates(blobId.getID());
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // the TtlUpdate Operation Time will be same as the PutBlob time since we apply TtlUpdate from the GetRequest.
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs(),
          Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // 2. On the sourceDataNode, the blob is deleted. On the targetDataNode, it doesn't exist.
      // Add Delete RepairRequest to the db.
      // ReplicateBlob will replicate both the putBlob and the delete
      blobId = blobId2;
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, false, ttlUpdateBlobExpiryTimeMs);
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      deleteBlob(sourceChannel, blobId, operationTime, ServerErrorCode.NoError);
      addRepairRequestForDelete(db, sourceDataNode, blobId, operationTime);
      // wait for the replication to catch up
      notificationSystem.awaitBlobReplicates(blobId.getID());
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      checkDeleteRecord(targetChannel, clusterMap, blobId, propertiesWithTtl.getCreationTimeInMs());
      cluster.time.sleep(1000);

      // 3. source doesn't have the PutBlob. Add Delete RepairRequest to the db.
      // ODR will repair the tombstone.
      blobId = blobId3;
      getBlobAndVerify(blobId, sourceChannel, ServerErrorCode.BlobNotFound, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      addRepairRequestForDelete(db, sourceDataNode, blobId, operationTime);
      // wait for the replication to catch up
      notificationSystem.awaitBlobReplicates(blobId.getID());
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.BlobDeleted, propertiesWithTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption, GetOption.Include_All);
      checkDeleteRecord(targetChannel, clusterMap, blobId, operationTime);
      cluster.time.sleep(1000);

      // 4. On the sourceDataNode, the blob is ttlUpdated. On the targetDataNode, it has the blob.
      // When the Repair starts, the blob is expired.
      // But ODR inherits the original operation time when it was first called. So BlobStore.updateTtl won't reject it.
      blobId = expiredId;
      // propertiesWithExpiredTtl will expire is 2s.
      BlobProperties propertiesWithExpiredTtl =
          new BlobProperties(dataSize, "serviceid1", "ownerid", "image/png", false, TimeUnit.SECONDS.toSeconds(2),
              cluster.time.milliseconds(), accountId, containerId, testEncryption, null, null, null, null);
      // write to both source and target nodes.
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithExpiredTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, sourceChannel, ServerErrorCode.NoError);
      putRequest = new PutRequest(1, "client1", blobId, propertiesWithExpiredTtl, ByteBuffer.wrap(userMetadata),
          Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
          testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
      putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
      long expiryTimeMs = getExpiryTimeMs(propertiesWithExpiredTtl);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, false, expiryTimeMs);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, false, expiryTimeMs);
      // now after 1s, update Ttl on the source. It is successful.
      cluster.time.sleep(1000);
      operationTime = cluster.time.milliseconds();
      updateBlobTtl(sourceChannel, blobId, operationTime);
      checkTtlUpdateStatus(sourceChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      // make the blob expired
      cluster.time.sleep(10000);
      // if we update TtlUpdate right now on target with current operation time, it'll fail.
      updateBlobTtl(targetChannel, blobId, cluster.time.milliseconds(), ServerErrorCode.BlobUpdateNotAllowed);
      // But the repairRequest inherit the operation time from the source replica when the request was first sent.
      // so it should be successful.
      addRepairRequestForTtlUpdate(db, sourceDataNode, blobId, operationTime, Utils.Infinite_Time);

      // wait for the replication to catch up
      notificationSystem.awaitBlobReplicates(blobId.getID());
      getBlobAndVerify(blobId, targetChannel, ServerErrorCode.NoError, propertiesWithExpiredTtl, userMetadata, data,
          encryptionKey, clusterMap, blobIdFactory, testEncryption);
      checkTtlUpdateStatus(targetChannel, clusterMap, blobIdFactory, blobId, data, true, Utils.Infinite_Time);
      checkTtlUpdateRecord(targetChannel, clusterMap, blobId, operationTime, Utils.Infinite_Time);
      cluster.time.sleep(1000);

      // Test pagination with records which cannot be fixed as well as records can be fixed.
      // Inject records can not be fixed.
      for (int i = 0; i < numOfRecordsCannotFix; i++) {
        blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            properties.getAccountId(), properties.getContainerId(), partitionId1, testEncryption,
            BlobId.BlobDataType.DATACHUNK);
        operationTime = cluster.time.milliseconds();
        // will fail to repair. we emit the metrics and repair continues to run for other requests
        addRepairRequestForTtlUpdate(db, sourceDataNode, blobId, operationTime, Utils.Infinite_Time);
        cluster.time.sleep(1);
      }
      // Inject records can be fixed.
      for (int i = 0; i < numOfSuccessFix; i++) {
        blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            properties.getAccountId(), properties.getContainerId(), partitionId1, testEncryption,
            BlobId.BlobDataType.DATACHUNK);
        operationTime = cluster.time.milliseconds();
        putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
            Unpooled.wrappedBuffer(data), properties.getBlobSize(), BlobType.DataBlob,
            testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
        putBlob(putRequest, targetChannel, ServerErrorCode.NoError);
        addRepairRequestForDelete(db, sourceDataNode, blobId, operationTime);
        cluster.time.sleep(1);
      }
      // Wait for good records are fixed.
      DataSource dataSource = db.getDataSource();
      long numberOfRows = 0;
      String rowQuerySql = "SELECT COUNT(*) as total FROM ambry_repair_requests";
      do {
        try (Connection connection = dataSource.getConnection()) {
          try (PreparedStatement statement = connection.prepareStatement(rowQuerySql)) {
            try (ResultSet result = statement.executeQuery()) {
              while (result.next()) {
                numberOfRows = result.getLong("total");
              }
            }
          }
        }
      } while (numberOfRows != numOfRecordsCannotFix);

      /*
       * Stage 2: Start the third data node. Now three data nodes are running.
       * Randomly generate failure requests. Each node will pick up some RepairRequests and fix them.
       */
      for (PartitionId id : partitionIds) {
        controlBlobStore(thirdChannel, id, true);
      }
      List<BlobId> blobIds = new ArrayList<>();
      for (int i = 0; i < randomTestNum; i++) {
        PartitionId partitionId = TestUtils.RANDOM.nextInt(2) == 0 ? partitionId1 : partitionId2;
        blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            propertiesWithTtl.getAccountId(), propertiesWithTtl.getContainerId(), partitionId, testEncryption,
            BlobId.BlobDataType.SIMPLE);
        putRequest = new PutRequest(1, "client1", blobId, propertiesWithTtl, ByteBuffer.wrap(userMetadata),
            Unpooled.wrappedBuffer(data), propertiesWithTtl.getBlobSize(), BlobType.DataBlob,
            testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
        int successOne = TestUtils.RANDOM.nextInt(3);
        ConnectedChannel successChannel =
            successOne == 0 ? sourceChannel : (successOne == 1 ? targetChannel : thirdChannel);
        DataNodeId successNode = successOne == 0 ? sourceDataNode : (successOne == 1 ? targetDataNode : thirdDataNode);
        putBlob(putRequest, successChannel, ServerErrorCode.NoError);
        cluster.time.sleep(1000);
        operationTime = cluster.time.milliseconds();
        if (TestUtils.RANDOM.nextInt(2) == 0) {
          deleteBlob(successChannel, blobId, operationTime, ServerErrorCode.NoError);
          addRepairRequestForDelete(db, successNode, blobId, operationTime);
        } else {
          updateBlobTtl(successChannel, blobId, operationTime);
          addRepairRequestForTtlUpdate(db, successNode, blobId, operationTime, Utils.Infinite_Time);
        }
        blobIds.add(blobId);
        cluster.time.sleep(1000);
      }
      for (BlobId id : blobIds) {
        notificationSystem.awaitBlobReplicates(id.getID());
      }

      // although we check the notification. DB update is in the RepairRequestsSender not in the handler threads.
      // wait for some time more.
      // We should still have one record which is case 4 error case.
      do {
        try (Connection connection = dataSource.getConnection()) {
          try (PreparedStatement statement = connection.prepareStatement(rowQuerySql)) {
            try (ResultSet result = statement.executeQuery()) {
              while (result.next()) {
                numberOfRows = result.getLong("total");
              }
            }
          }
        }
      } while (numberOfRows != numOfRecordsCannotFix);

      resetRepairRequestsDb(db);

      // Test is done.
      // restart the blobs stores
      for (ConnectedChannel channel : channels) {
        if (channel == sourceChannel || channel == targetChannel || channel == thirdChannel) {
          continue;
        }
        for (PartitionId id : partitionIds) {
          controlBlobStore(channel, id, true);
        }
      }
      // now restart replication on all data nodes
      controlReplicationForPartition(channels, partitionIds, true);
      // close all the connections
      for (ConnectedChannel channel : channels) {
        channel.disconnect();
      }
    } catch (Exception e) {
      e.printStackTrace();
      assertNull(e);
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

  /*
   * Create a db connection to the RepairRequests db and cleanup the db.
   * @param localDc : name of the local data center.
   */
  static MysqlRepairRequestsDb createRepairRequestsConnection(String localDc, Time time) throws Exception {
    try {
      Properties properties = Utils.loadPropsFromResource("repairRequests_mysql.properties");
      properties.setProperty(MysqlRepairRequestsDbConfig.LIST_MAX_RESULTS, Integer.toString(100));
      properties.setProperty(MysqlRepairRequestsDbConfig.LOCAL_POOL_SIZE, "5");

      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      MetricRegistry metrics = new MetricRegistry();
      MysqlRepairRequestsDbFactory factory =
          new MysqlRepairRequestsDbFactory(verifiableProperties, metrics, localDc, time);
      MysqlRepairRequestsDb repairRequestsDb = factory.getRepairRequestsDb();

      resetRepairRequestsDb(repairRequestsDb);
      return repairRequestsDb;
    } catch (SQLException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  static MysqlRepairRequestsDb resetRepairRequestsDb(MysqlRepairRequestsDb repairRequestsDb) throws Exception {
    try {
      // cleanup the database
      DataSource dataSource = repairRequestsDb.getDataSource();

      Connection connection = dataSource.getConnection();
      Statement statement = connection.createStatement();
      statement.executeUpdate("DELETE FROM ambry_repair_requests;");

      return repairRequestsDb;
    } catch (SQLException e) {
      throw e;
    }
  }

  /*
   * Add a Delete RepairRequestRecord to the RepairRequests db
   * @param db the testing RepairRequestsDb
   * @param sourceNode the source data node to replicate the blob from
   * @param blobId the blob id in string
   * @param operationTime the operation time of the delete.
   */
  static void addRepairRequestForDelete(RepairRequestsDb db, DataNodeId sourceNode, BlobId blobId, long operationTime)
      throws Exception {
    RepairRequestRecord record =
        new RepairRequestRecord(blobId.toString(), (int) blobId.getPartition().getId(), sourceNode.getHostname(),
            sourceNode.getPort(), RepairRequestRecord.OperationType.DeleteRequest, operationTime, (short) -1, 0);
    db.putRepairRequests(record);
  }

  /*
   * Add a TtlUpdate RepairRequestRecord to the RepairRequests db
   * @param db the testing RepairRequestsDb
   * @param sourceNode the source data node to replicate the blob from
   * @param blobId the blob id in string
   * @param operationTime the operation time of the ttlUpdate
   * @param expiration the Ttl expiration time
   */
  static void addRepairRequestForTtlUpdate(RepairRequestsDb db, DataNodeId sourceNode, BlobId blobId,
      long operationTime, long expiration) throws Exception {
    RepairRequestRecord record =
        new RepairRequestRecord(blobId.toString(), (int) blobId.getPartition().getId(), sourceNode.getHostname(),
            sourceNode.getPort(), RepairRequestRecord.OperationType.TtlUpdateRequest, operationTime, (short) -1,
            expiration);
    db.putRepairRequests(record);
  }
}
