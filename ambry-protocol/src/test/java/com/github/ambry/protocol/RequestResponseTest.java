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
package com.github.ambry.protocol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.Send;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.LogInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreFileChunk;
import com.github.ambry.store.StoreFileInfo;
import com.github.ambry.store.StoreLogInfo;
import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;


class MockFindTokenHelper extends FindTokenHelper {
  public MockFindTokenHelper() {
    super();
  }

  @Override
  public FindTokenFactory getFindTokenFactoryFromReplicaType(ReplicaType replicaType) {
    return new MockFindTokenFactory();
  }
}

class MockFindTokenFactory implements FindTokenFactory {

  public MockFindTokenFactory() {

  }

  @Override
  public FindToken getFindToken(DataInputStream stream) throws IOException {
    return new MockFindToken(stream);
  }

  @Override
  public FindToken getNewFindToken() {
    return new MockFindToken(0, 0);
  }
}

class MockFindToken implements FindToken {
  short version;
  FindTokenType type;
  int index;
  long bytesRead;

  public MockFindToken(int index, long bytesRead) {
    this.version = 0;
    this.type = FindTokenType.IndexBased;
    this.index = index;
    this.bytesRead = bytesRead;
  }

  public MockFindToken(DataInputStream stream) throws IOException {
    this.version = stream.readShort();
    this.type = FindTokenType.values()[stream.readShort()];
    this.index = stream.readInt();
    this.bytesRead = stream.readLong();
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2 * Short.BYTES + Integer.BYTES + Long.BYTES);
    byteBuffer.putShort(version);
    byteBuffer.putShort((short) type.ordinal());
    byteBuffer.putInt(index);
    byteBuffer.putLong(bytesRead);
    return byteBuffer.array();
  }

  public int getIndex() {
    return index;
  }

  public long getBytesRead() {
    return this.bytesRead;
  }

  @Override
  public FindTokenType getType() {
    return type;
  }

  @Override
  public short getVersion() {
    return version;
  }
}

class InvalidVersionPutRequest extends PutRequest {
  static final short Put_Request_Invalid_version = 0;

  public InvalidVersionPutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, ByteBuf blob, long blobSize, BlobType blobType) {
    super(correlationId, clientId, blobId, properties, usermetadata, blob, blobSize, blobType, null);
    versionId = Put_Request_Invalid_version;
  }
}

/**
 * Tests for different requests and responses in the protocol.
 */
@RunWith(Parameterized.class)
public class RequestResponseTest {
  private static short versionSaved;
  private final boolean useByteBufContent;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  @BeforeClass
  public static void saveVersionToUse() {
    versionSaved = GetResponse.CURRENT_VERSION;
  }

  @After
  public void resetVersionToUse() {
    GetResponse.CURRENT_VERSION = versionSaved;
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  public RequestResponseTest(boolean useByteBufContent) {
    this.useByteBufContent = useByteBufContent;
  }

  /**
   * Tests serialization and deserialization of Put requests in different versions.
   * @param clusterMap the cluster map to use.
   * @param correlationId the correlation id associated with the request.
   * @param clientId the client id associated with the request.
   * @param blobId the blob id in the request.
   * @param blobProperties the {@link BlobProperties} associated with the request.
   * @param userMetadata the user metadata associated with the request.
   * @param blobType the {@link BlobType} associated with the request.
   * @param blob the blob content.
   * @param blobSize the size of the blob.
   * @param blobKey the encryption key of the blob.
   */
  private void testPutRequest(MockClusterMap clusterMap, int correlationId, String clientId, BlobId blobId,
      BlobProperties blobProperties, byte[] userMetadata, BlobType blobType, byte[] blob, int blobSize, byte[] blobKey)
      throws IOException {
    doPutRequestTest((short) -1, clusterMap, correlationId, clientId, blobId, blobProperties,
        userMetadata, blobType, blob, blobSize, null, null);
    doPutRequestTest((short) -1, clusterMap, correlationId, clientId, blobId, blobProperties,
        userMetadata, blobType, blob, blobSize, new byte[0], null);
    doPutRequestTest((short) -1, clusterMap, correlationId, clientId, blobId, blobProperties,
        userMetadata, blobType, blob, blobSize, blobKey, blobKey);
  }

  /**
   * Test Put requests in a specific version.
   * @param testVersion the version to use for the Put requests. If -1, uses the default version.
   * @param clusterMap the cluster map to use.
   * @param correlationId the correlation id associated with the request.
   * @param clientId the client id associated with the request.
   * @param blobId the blob id in the request.
   * @param blobProperties the {@link BlobProperties} associated with the request.
   * @param userMetadata the user metadata associated with the request.
   * @param blobType the {@link BlobType} associated with the request.
   * @param blob the blob content.
   * @param blobSize the size of the blob.
   * @param blobKey the encryption key of the blob.
   * @param expectedKey the expected encryption key from the deserialized Put request.
   */
  private void doPutRequestTest(short testVersion, MockClusterMap clusterMap, int correlationId, String clientId,
      BlobId blobId, BlobProperties blobProperties, byte[] userMetadata, BlobType blobType, byte[] blob, int blobSize,
      byte[] blobKey, byte[] expectedKey) throws IOException {
    // This PutRequest is created just to get the size.
    PutRequest putRequest =
        new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            Unpooled.wrappedBuffer(blob), blobSize, blobType, blobKey == null ? null : ByteBuffer.wrap(blobKey));
    int sizeInBytes = (int) putRequest.sizeInBytes();
    putRequest.release();
    // Initialize channel write limits in such a way that writeTo() may or may not be able to write out all the
    // data at once.
    int channelWriteLimits[] =
        {sizeInBytes, 2 * sizeInBytes, sizeInBytes / 2, sizeInBytes / (TestUtils.RANDOM.nextInt(sizeInBytes - 1) + 1)};
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    DataInputStream requestStream;
    for (int allocationSize : channelWriteLimits) {
      for (boolean useCompositeBlob : new boolean[]{true, false}) {
        PutRequest request = null;
        switch (testVersion) {
          case InvalidVersionPutRequest.Put_Request_Invalid_version:
            request = new InvalidVersionPutRequest(correlationId, clientId, blobId, blobProperties,
                ByteBuffer.wrap(userMetadata), Unpooled.wrappedBuffer(blob), sizeInBlobProperties, BlobType.DataBlob);
            requestStream = serAndPrepForRead(request, -1, true);
            try {
              PutRequest.readFrom(requestStream, clusterMap);
              Assert.fail("Deserialization of PutRequest with invalid version should have thrown an exception.");
            } catch (IllegalStateException e) {
            }
            request.release();
            break;
          default:
            if (request == null) {
              ByteBuf blobBuf = Unpooled.wrappedBuffer(blob);
              if (useCompositeBlob) {
                // break this into three ByteBuf and make a composite blob;
                int start = 0, end = blob.length / 3;
                ByteBuf blob1 = PooledByteBufAllocator.DEFAULT.heapBuffer(end - start);
                blob1.writeBytes(blob, start, end - start);
                start = end;
                end += blob.length / 3;
                ByteBuf blob2 = PooledByteBufAllocator.DEFAULT.heapBuffer(end - start);
                blob2.writeBytes(blob, start, end - start);
                start = end;
                end = blob.length;
                ByteBuf blob3 = PooledByteBufAllocator.DEFAULT.heapBuffer(end - start);
                blob3.writeBytes(blob, start, end - start);
                blobBuf = PooledByteBufAllocator.DEFAULT.compositeHeapBuffer(3);
                ((CompositeByteBuf) blobBuf).addComponent(true, blob1);
                ((CompositeByteBuf) blobBuf).addComponent(true, blob2);
                ((CompositeByteBuf) blobBuf).addComponent(true, blob3);
              }
              request = new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
                  blobBuf, blobSize, blobType, blobKey == null ? null : ByteBuffer.wrap(blobKey));
            }
            requestStream = serAndPrepForRead(request, allocationSize, true);
            PutRequest deserializedPutRequest = PutRequest.readFrom(requestStream, clusterMap);
            Assert.assertEquals(blobId, deserializedPutRequest.getBlobId());
            Assert.assertEquals(sizeInBlobProperties, deserializedPutRequest.getBlobProperties().getBlobSize());
            Assert.assertEquals(userMetadata.length, deserializedPutRequest.getUsermetadata().remaining());
            byte[] deserializedUserMetadata = new byte[userMetadata.length];
            deserializedPutRequest.getUsermetadata().get(deserializedUserMetadata);
            Assert.assertArrayEquals(userMetadata, deserializedUserMetadata);
            Assert.assertEquals(blobSize, deserializedPutRequest.getBlobSize());
            Assert.assertEquals(blobType, deserializedPutRequest.getBlobType());
            if (expectedKey == null) {
              Assert.assertNull(deserializedPutRequest.getBlobEncryptionKey());
            } else {
              Assert.assertArrayEquals(expectedKey, deserializedPutRequest.getBlobEncryptionKey().array());
            }
            byte[] blobRead = new byte[blobSize];
            deserializedPutRequest.getBlobStream().read(blobRead);
            Assert.assertArrayEquals(blob, blobRead);
            request.release();
            break;
        }
      }
    }
  }

  @Test
  public void putRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();

    int correlationId = 5;
    String clientId = "client";
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM),
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    byte[] userMetadata = new byte[50];
    TestUtils.RANDOM.nextBytes(userMetadata);
    int blobKeyLength = TestUtils.RANDOM.nextInt(4096);
    byte[] blobKey = new byte[blobKeyLength];
    TestUtils.RANDOM.nextBytes(blobKey);
    int blobSize = 100;
    byte[] blob = new byte[blobSize];
    TestUtils.RANDOM.nextBytes(blob);

    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            TestUtils.RANDOM.nextBoolean(), null, null, null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize, blobKey);
    doPutRequestTest(InvalidVersionPutRequest.Put_Request_Invalid_version, clusterMap, correlationId, clientId, blobId,
        blobProperties, userMetadata, BlobType.DataBlob, blob, blobSize, blobKey, null);

    // Put Request with size in blob properties different from the data size and blob type: Data blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            TestUtils.RANDOM.nextBoolean(), null, null, null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize, blobKey);

    // Put Request with size in blob properties different from the data size and blob type: Metadata blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            TestUtils.RANDOM.nextBoolean(), null, null, null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.MetadataBlob,
        blob, blobSize, blobKey);

    // Put Request with empty user metadata.
    byte[] emptyUserMetadata = new byte[0];
    blobProperties = new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), TestUtils.RANDOM.nextBoolean(),
        null, null, null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, emptyUserMetadata, BlobType.DataBlob,
        blob, blobSize, blobKey);

    // Response test
    PutResponse response = new PutResponse(1234, clientId, ServerErrorCode.NoError);
    DataInputStream responseStream = serAndPrepForRead(response, -1, false);
    PutResponse deserializedPutResponse = PutResponse.readFrom(responseStream);
    Assert.assertEquals(deserializedPutResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedPutResponse.getError(), ServerErrorCode.NoError);
    response.release();
  }

  @Test
  public void testReadFromV5() throws IOException {
    int correlationId = 1234;
    String clientId = "client";
    PartitionId partitionId = new MockPartitionId();
    BlobId blobId =
        new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, (short) 1, (short) 1, partitionId, false,
            BlobId.BlobDataType.DATACHUNK);
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", (short) 2222, (short) 3333, true);
    ByteBuffer userMetadata = ByteBuffer.wrap("testMetadata".getBytes());
    ByteBuf blobData = Unpooled.wrappedBuffer("testBlobData".getBytes());
    long blobSize = blobData.readableBytes();
    BlobType blobType = BlobType.DataBlob;
    byte[] encryptionKey = new byte[]{1, 2, 3, 4, 5};

    // Compose and serialize the PutRequest with V5 enabled.
    PutRequest request =
        new PutRequest(correlationId, clientId, blobId, blobProperties, userMetadata, blobData, blobSize, blobType,
            ByteBuffer.wrap(encryptionKey), true);
    ByteBuf content = request.content();

    // Read back binary.  The content binary contains extra fields(total size and operation type).
    // Those extra fields must be read from the stream before calling readFrom().
    try {
      NettyByteBufDataInputStream inputStream = new NettyByteBufDataInputStream(content);
      inputStream.readLong();   // Skip the total size.
      inputStream.readShort();  // skip the operation type (PutOperation).
      PutRequest newPutRequest = PutRequest.readFrom(inputStream, new MockClusterMap());

      Assert.assertEquals(correlationId, newPutRequest.correlationId);
      Assert.assertEquals(clientId, newPutRequest.clientId);
      Assert.assertEquals(blobSize, newPutRequest.blobSize);
      Assert.assertTrue(newPutRequest.isCompressed());
      Assert.assertEquals("testServiceID", newPutRequest.getBlobProperties().getServiceId());
    } finally {
      content.release();
    }
  }

  @Test
  public void getRequestResponseTest() throws IOException {
    short oldGetResponseVersion = GetResponse.CURRENT_VERSION;
    short oldMessageInfoVersion = MessageInfoAndMetadataListSerde.AUTO_VERSION;
    testGetRequestResponse(GetResponse.CURRENT_VERSION, MessageInfoAndMetadataListSerde.VERSION_6);
    testGetRequestResponse(GetResponse.CURRENT_VERSION, MessageInfoAndMetadataListSerde.VERSION_5);
    testGetRequestResponse(GetResponse.GET_RESPONSE_VERSION_V_5, MessageInfoAndMetadataListSerde.VERSION_6);
    testGetRequestResponse(GetResponse.GET_RESPONSE_VERSION_V_5, MessageInfoAndMetadataListSerde.VERSION_5);
    testGetRequestResponse(GetResponse.GET_RESPONSE_VERSION_V_4, MessageInfoAndMetadataListSerde.VERSION_5);
    testGetRequestResponse(GetResponse.GET_RESPONSE_VERSION_V_3, MessageInfoAndMetadataListSerde.VERSION_5);
    GetResponse.CURRENT_VERSION = oldGetResponseVersion;
    MessageInfoAndMetadataListSerde.AUTO_VERSION = oldMessageInfoVersion;
  }

  private void testGetRequestResponse(short getVersionToUse, short messageInfoAutoVersion) throws IOException {
    GetResponse.CURRENT_VERSION = getVersionToUse;
    MessageInfoAndMetadataListSerde.AUTO_VERSION = messageInfoAutoVersion;
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>();
    blobIdList.add(id1);
    PartitionRequestInfo partitionRequestInfo1 = new PartitionRequestInfo(new MockPartitionId(), blobIdList);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.add(partitionRequestInfo1);
    GetRequest getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    DataInputStream requestStream = serAndPrepForRead(getRequest, -1, true);
    GetRequest deserializedGetRequest = GetRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedGetRequest.getClientId(), "clientId");
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().size(), 1);
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().get(0).getBlobIds().size(), 1);
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().get(0).getBlobIds().get(0), id1);
    getRequest.release();

    // Test GetResponse with InputStream
    long operationTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
    byte[] encryptionKey = TestUtils.getRandomBytes(256);
    MessageInfo messageInfo =
        new MessageInfo(id1, 1000, false, false, true, 1000, null, accountId, containerId, operationTimeMs, (short) 1);
    MessageMetadata messageMetadata = new MessageMetadata(ByteBuffer.wrap(encryptionKey));
    ArrayList<MessageInfo> messageInfoList = new ArrayList<>();
    ArrayList<MessageMetadata> messageMetadataList = new ArrayList<>();
    messageInfoList.add(messageInfo);
    messageMetadataList.add(messageMetadata);
    PartitionResponseInfo partitionResponseInfo =
        new PartitionResponseInfo(clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0),
            messageInfoList, messageMetadataList);
    List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
    partitionResponseInfoList.add(partitionResponseInfo);
    byte[] buf = TestUtils.getRandomBytes(1000);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(buf);
    GetResponse response =
        new GetResponse(1234, "clientId", partitionResponseInfoList, byteStream, ServerErrorCode.NoError);
    requestStream = serAndPrepForRead(response, -1, false);
    GetResponse deserializedGetResponse = GetResponse.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedGetResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedGetResponse.getError(), ServerErrorCode.NoError);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().size(), 1);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().size(), 1);
    MessageInfo msgInfo = deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    Assert.assertEquals(msgInfo.getSize(), 1000);
    Assert.assertEquals(msgInfo.getStoreKey(), id1);
    Assert.assertEquals(msgInfo.getExpirationTimeInMs(), 1000);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageMetadataList().size(),
        1);
    if (GetResponse.getCurrentVersion() >= GetResponse.GET_RESPONSE_VERSION_V_4) {
      MessageMetadata messageMetadataInResponse =
          deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0);
      Assert.assertEquals(messageMetadata.getEncryptionKey().rewind(), messageMetadataInResponse.getEncryptionKey());
    } else {
      Assert.assertNull(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageMetadataList().get(0));
    }
    if (GetResponse.getCurrentVersion() >= GetResponse.GET_RESPONSE_VERSION_V_3) {
      Assert.assertEquals("AccountId mismatch ", accountId, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", containerId, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", operationTimeMs, msgInfo.getOperationTimeMs());
    } else {
      Assert.assertEquals("AccountId mismatch ", UNKNOWN_ACCOUNT_ID, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", UNKNOWN_CONTAINER_ID, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", Utils.Infinite_Time, msgInfo.getOperationTimeMs());
    }
    if (messageInfoAutoVersion >= MessageInfoAndMetadataListSerde.VERSION_6) {
      Assert.assertTrue(msgInfo.isUndeleted());
      Assert.assertEquals("LifeVersion mismatch", (short) 1, msgInfo.getLifeVersion());
    } else {
      Assert.assertFalse(msgInfo.isUndeleted());
      Assert.assertEquals("LifeVersion mismatch", (short) 0, msgInfo.getLifeVersion());
    }
    response.release();
    // Test GetResponse with Send
    for (boolean useComposite : new boolean[]{false, true}) {
      for (boolean withContent : new boolean[]{false, true}) {
        operationTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
        encryptionKey = TestUtils.getRandomBytes(256);
        messageInfo =
            new MessageInfo(id1, 1000, false, false, true, 1000, null, accountId, containerId, operationTimeMs,
                (short) 1);
        messageMetadata = new MessageMetadata(ByteBuffer.wrap(encryptionKey));
        messageInfoList.clear();
        messageMetadataList.clear();
        messageInfoList.add(messageInfo);
        messageMetadataList.add(messageMetadata);
        partitionResponseInfo =
            new PartitionResponseInfo(clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0),
                messageInfoList, messageMetadataList);
        partitionResponseInfoList.clear();
        partitionResponseInfoList.add(partitionResponseInfo);
        Send send;
        if (withContent) {
          send = new SendWithContent(1000, useComposite);
        } else {
          send = new SendWithoutContent(1000, useComposite);
        }
        response = new GetResponse(1234, "clientId", partitionResponseInfoList, send, ServerErrorCode.NoError);
        requestStream = serAndPrepForRead(response, -1, false);
        deserializedGetResponse = GetResponse.readFrom(requestStream, clusterMap);
        Assert.assertEquals(deserializedGetResponse.getCorrelationId(), 1234);
        Assert.assertEquals(deserializedGetResponse.getError(), ServerErrorCode.NoError);
        Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().size(), 1);
        Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().size(),
            1);
        msgInfo = deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
        Assert.assertEquals(msgInfo.getSize(), 1000);
        Assert.assertEquals(msgInfo.getStoreKey(), id1);
        Assert.assertEquals(msgInfo.getExpirationTimeInMs(), 1000);
        Assert.assertEquals(
            deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageMetadataList().size(), 1);
        response.release();
      }
    }
  }

  @Test
  public void deleteRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    short[] versions = new short[]{DeleteRequest.DELETE_REQUEST_VERSION_1, DeleteRequest.DELETE_REQUEST_VERSION_2};
    for (short version : versions) {
      long deletionTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
      int correlationId = TestUtils.RANDOM.nextInt();
      DeleteRequest deleteRequest;
      if (version == DeleteRequest.DELETE_REQUEST_VERSION_1) {
        deleteRequest = new DeleteRequestV1(correlationId, "client", id1);
      } else {
        deleteRequest = new DeleteRequest(correlationId, "client", id1, deletionTimeMs);
      }
      DataInputStream requestStream = serAndPrepForRead(deleteRequest, -1, true);
      DeleteRequest deserializedDeleteRequest = DeleteRequest.readFrom(requestStream, clusterMap);
      Assert.assertEquals(deserializedDeleteRequest.getClientId(), "client");
      Assert.assertEquals(deserializedDeleteRequest.getBlobId(), id1);
      if (version == DeleteRequest.DELETE_REQUEST_VERSION_2) {
        Assert.assertEquals("AccountId mismatch ", id1.getAccountId(), deserializedDeleteRequest.getAccountId());
        Assert.assertEquals("ContainerId mismatch ", id1.getContainerId(), deserializedDeleteRequest.getContainerId());
        Assert.assertEquals("DeletionTime mismatch ", deletionTimeMs, deserializedDeleteRequest.getDeletionTimeInMs());
      } else {
        Assert.assertEquals("AccountId mismatch ", accountId, deserializedDeleteRequest.getAccountId());
        Assert.assertEquals("ContainerId mismatch ", containerId, deserializedDeleteRequest.getContainerId());
        Assert.assertEquals("DeletionTime mismatch ", Utils.Infinite_Time,
            deserializedDeleteRequest.getDeletionTimeInMs());
      }
      deleteRequest.release();
      DeleteResponse response = new DeleteResponse(correlationId, "client", ServerErrorCode.NoError);
      requestStream = serAndPrepForRead(response, -1, false);
      DeleteResponse deserializedDeleteResponse = DeleteResponse.readFrom(requestStream);
      Assert.assertEquals(deserializedDeleteResponse.getCorrelationId(), correlationId);
      Assert.assertEquals(deserializedDeleteResponse.getError(), ServerErrorCode.NoError);
      response.release();
    }
  }

  /**
   * Test for undelete request and response.
   * @throws IOException
   */
  @Test
  public void undeleteRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    int correlationId = TestUtils.RANDOM.nextInt();
    long operationTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
    UndeleteRequest undeleteRequest = new UndeleteRequest(correlationId, "client", id1, operationTimeMs);
    DataInputStream requestStream = serAndPrepForRead(undeleteRequest, -1, true);
    UndeleteRequest deserializedUndeleteRequest = UndeleteRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedUndeleteRequest.getClientId(), "client");
    Assert.assertEquals(deserializedUndeleteRequest.getBlobId(), id1);
    Assert.assertEquals("AccountId mismatch ", id1.getAccountId(), deserializedUndeleteRequest.getAccountId());
    Assert.assertEquals("ContainerId mismatch ", id1.getContainerId(), deserializedUndeleteRequest.getContainerId());
    Assert.assertEquals("OperationTimeMs mismatch ", operationTimeMs, deserializedUndeleteRequest.getOperationTimeMs());
    undeleteRequest.release();
    UndeleteResponse response = null;
    try {
      response = new UndeleteResponse(correlationId, "client", ServerErrorCode.NoError);
      Assert.fail("No Error is not a valid error node for this response");
    } catch (IllegalArgumentException e) {
      // do nothing
    }
    response = new UndeleteResponse(correlationId, "client", ServerErrorCode.BlobDeleted);
    requestStream = serAndPrepForRead(response, -1, false);
    UndeleteResponse deserializedUndeleteResponse = UndeleteResponse.readFrom(requestStream);
    Assert.assertEquals(deserializedUndeleteResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedUndeleteResponse.getError(), ServerErrorCode.BlobDeleted);
    Assert.assertEquals(deserializedUndeleteResponse.getLifeVersion(), UndeleteResponse.INVALID_LIFE_VERSION);
    response.release();

    try {
      response = new UndeleteResponse(correlationId, "client", UndeleteResponse.INVALID_LIFE_VERSION);
      Assert.fail("Not an valid life version");
    } catch (IllegalArgumentException e) {
      // do nothing
    }
    short lifeVersion = 1;
    response = new UndeleteResponse(correlationId, "client", lifeVersion);
    requestStream = serAndPrepForRead(response, -1, false);
    deserializedUndeleteResponse = UndeleteResponse.readFrom(requestStream);
    Assert.assertEquals(deserializedUndeleteResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedUndeleteResponse.getError(), ServerErrorCode.NoError);
    Assert.assertEquals(deserializedUndeleteResponse.getLifeVersion(), lifeVersion);
    response.release();
  }

  @Test
  public void replicaMetadataRequestTest() throws IOException {
    short oldMessageInfoVersion = MessageInfoAndMetadataListSerde.AUTO_VERSION;
    for (ReplicaType replicaType : ReplicaType.values()) {
      doReplicaMetadataRequestTest(ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6,
          ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, MessageInfoAndMetadataListSerde.VERSION_5,
          replicaType);
      doReplicaMetadataRequestTest(ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6,
          ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, MessageInfoAndMetadataListSerde.VERSION_6,
          replicaType);
      doReplicaMetadataRequestTest(ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_4,
          ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, MessageInfoAndMetadataListSerde.VERSION_5,
          replicaType);
      doReplicaMetadataRequestTest(ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5,
          ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, MessageInfoAndMetadataListSerde.VERSION_5,
          replicaType);
      doReplicaMetadataRequestTest(ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5,
          ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, MessageInfoAndMetadataListSerde.VERSION_6,
          replicaType);
    }
    MessageInfoAndMetadataListSerde.AUTO_VERSION = oldMessageInfoVersion;
  }

  @Test
  public void doFileCopyMetaDataRequestTest() throws IOException {
    short requestVersionToUse = 1;
    FileCopyGetMetaDataRequest request =
        new FileCopyGetMetaDataRequest(requestVersionToUse, 111, "id1",
            new MockPartitionId(), "host3");
    DataInputStream requestStream = serAndPrepForRead(request, -1, true);
    FileCopyGetMetaDataRequest fileMetadataRequestFromBytes =
        FileCopyGetMetaDataRequest.readFrom(requestStream, new MockClusterMap());
    Assert.assertEquals("host3", fileMetadataRequestFromBytes.getHostName());
    Assert.assertEquals(0l, fileMetadataRequestFromBytes.getPartitionId().getId());
    Assert.assertEquals("0", fileMetadataRequestFromBytes.getPartitionId().toPathString());
    request.release();

    try {
      // Sending null partition id. Expected to throw exception.
      new FileCopyGetMetaDataRequest(requestVersionToUse, 111, "id1",
          null, "host3");
      Assert.fail("Should have thrown exception");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      // Sending empty host name. Expected to throw exception.
      new FileCopyGetMetaDataRequest(requestVersionToUse, 111, "id1",
          new MockPartitionId(), "");
      Assert.fail("Should have thrown exception");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void doFileInfoObjectParsingTest() throws IOException {
    FileInfo fileInfo = new StoreFileInfo("/tmp", 1000L);
    ByteBuf byteBuf = Unpooled.buffer();
    fileInfo.writeTo(byteBuf);
    DataInputStream stream = new NettyByteBufDataInputStream(byteBuf);
    FileInfo transformedFileInfo = StoreFileInfo.readFrom(stream);

    assert transformedFileInfo != null;
    Assert.assertEquals(fileInfo.getFileName(), transformedFileInfo.getFileName());
    Assert.assertEquals(fileInfo.getFileSize(), transformedFileInfo.getFileSize());
    byteBuf.release();
  }

  @Test
  public void doLogInfoParsingTest() throws IOException {
    LogInfo logInfo = new StoreLogInfo(new StoreFileInfo("0_index", 1000L),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("0_1_index", 1010L))),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("1_1_bloom", 1020L))));

    ByteBuf byteBuf = Unpooled.buffer();
    logInfo.writeTo(byteBuf);
    DataInputStream stream = new NettyByteBufDataInputStream(byteBuf);
    LogInfo transformedLogInfo = StoreLogInfo.readFrom(stream);

    assert transformedLogInfo != null;
    assert transformedLogInfo.getLogSegment() != null;
    Assert.assertEquals(logInfo.getLogSegment().getFileName(), transformedLogInfo.getLogSegment().getFileName());
    Assert.assertEquals(logInfo.getLogSegment().getFileSize(), transformedLogInfo.getLogSegment().getFileSize());
    byteBuf.release();
  }

  /**
   * Test the {@link StoreFileChunk#toBuffer} method.
   * @throws IOException
   */
  @Test
  public void testStoreFileChunkToBuffer() throws IOException {
    String fileName = "testFile";
    long fileSize = 12345L;
    ByteBuf byteBuf = Unpooled.buffer();
    byteBuf.writeBytes(fileName.getBytes());
    byteBuf.writeLong(fileSize);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    // Create a StoreFileChunk object and write it to a byte buffer
    StoreFileChunk storeFileChunk = new StoreFileChunk(fileStream, fileStream.available());
    ByteBuffer storeFileChunkByteBuffer = storeFileChunk.toBuffer();
    ByteBuf storeFileChunkBuf = Unpooled.wrappedBuffer(storeFileChunkByteBuffer);

    // Assert that the byte buffer has the correct values
    Assert.assertEquals(fileName, storeFileChunkBuf.readCharSequence(fileName.length(), StandardCharsets.UTF_8));
    Assert.assertEquals(fileSize, storeFileChunkBuf.readLong());
    Assert.assertEquals(fileStream.available(), storeFileChunkBuf.readableBytes());
    byteBuf.release();
  }

  /**
   * Test the {@link StoreFileChunk#from} method.
   * @throws IOException
   */
  @Test
  public void testStoreFileChunkFrom() throws IOException {
    String fileName = "testFile";
    long fileSize = 12345L;
    ByteBuf byteBuf = Unpooled.buffer();
    byteBuf.writeBytes(fileName.getBytes());
    byteBuf.writeLong(fileSize);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    // Read the byte buffer and create a StoreFileChunk object
    StoreFileChunk storeFileChunk = StoreFileChunk.from(byteBuf.nioBuffer());

    // Assert that the file chunk object has the correct values
    Assert.assertEquals(storeFileChunk.getChunkLength(), fileStream.available());
    Assert.assertTrue(areStreamsEqual(fileStream, storeFileChunk.getStream()));
    byteBuf.release();
  }

  private boolean areStreamsEqual(InputStream stream1, InputStream stream2) throws IOException {
    if (stream1 == stream2) {
      return true;
    }
    if (stream1 == null || stream2 == null) {
      return false;
    }

    int byte1, byte2;
    while ((byte1 = stream1.read()) != -1) {
      byte2 = stream2.read();
      if (byte1 != byte2) {
        return false;
      }
    }
    return stream2.read() == -1;
  }

  @Test
  public void doFileCopyMetaDataResponseTest() throws IOException{
    short requestVersionToUse = 1;
    LogInfo logInfo1 = new StoreLogInfo(new StoreFileInfo("0_log", 1000L),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("0_1_index", 1010L))),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("0_1_bloom", 1020L))));

    LogInfo logInfo2 = new StoreLogInfo(new StoreFileInfo("1_log", 1050L),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("1_1_index", 1030L))),
        new ArrayList<>(Collections.singletonList(new StoreFileInfo("1_1_bloom", 1040L))));
    List<LogInfo> logInfoList = new ArrayList<>(Arrays.asList(logInfo1, logInfo2));

    FileCopyGetMetaDataResponse response = new FileCopyGetMetaDataResponse(requestVersionToUse, 111, "id1", 2,
        logInfoList, "snapshotId", ServerErrorCode.NoError, null);

    DataInputStream requestStream1 = serAndPrepForRead(response, -1, false);
    FileCopyGetMetaDataResponse response1 = FileCopyGetMetaDataResponse.readFrom(requestStream1);

    Assert.assertEquals(111, response1.getCorrelationId());
    Assert.assertEquals(1, response1.versionId);
    Assert.assertEquals(ServerErrorCode.NoError, response1.getError());
    Assert.assertEquals(2, response1.getNumberOfLogfiles());
    Assert.assertEquals(2, response1.getLogInfos().size());
    Assert.assertEquals("snapshotId", response1.getSnapshotId());

    Assert.assertEquals("0_log", response1.getLogInfos().get(0).getLogSegment().getFileName());
    Assert.assertEquals(1000, (long)response1.getLogInfos().get(0).getLogSegment().getFileSize());
    Assert.assertEquals(1, response1.getLogInfos().get(0).getIndexSegments().size());
    Assert.assertEquals(1, response1.getLogInfos().get(0).getBloomFilters().size());

    Assert.assertEquals("1_log", response1.getLogInfos().get(1).getLogSegment().getFileName());
    Assert.assertEquals(1050, (long)response1.getLogInfos().get(1).getLogSegment().getFileSize());
    Assert.assertEquals(1, response1.getLogInfos().get(1).getIndexSegments().size());
    Assert.assertEquals(1, response1.getLogInfos().get(1).getBloomFilters().size());

    Assert.assertEquals("0_1_index", response1.getLogInfos().get(0).getIndexSegments().get(0).getFileName());
    Assert.assertEquals(1010, (long)response1.getLogInfos().get(0).getIndexSegments().get(0).getFileSize());

    Assert.assertEquals("0_1_bloom", response1.getLogInfos().get(0).getBloomFilters().get(0).getFileName());
    Assert.assertEquals(1020, (long)response1.getLogInfos().get(0).getBloomFilters().get(0).getFileSize());

    Assert.assertEquals("1_1_index", response1.getLogInfos().get(1).getIndexSegments().get(0).getFileName());
    Assert.assertEquals(1030, (long)response1.getLogInfos().get(1).getIndexSegments().get(0).getFileSize());

    Assert.assertEquals("1_1_bloom", response1.getLogInfos().get(1).getBloomFilters().get(0).getFileName());
    Assert.assertEquals(1040, (long)response1.getLogInfos().get(1).getBloomFilters().get(0).getFileSize());
    response.release();

    response = new FileCopyGetMetaDataResponse(requestVersionToUse, 111, "id1", 2,
        logInfoList, "snapshotId", ServerErrorCode.IOError, null);
    DataInputStream requestStream2 = serAndPrepForRead(response, -1, false);
    FileCopyGetMetaDataResponse response2 = FileCopyGetMetaDataResponse.readFrom(requestStream2);
    Assert.assertEquals(ServerErrorCode.IOError, response2.getError());
    response.release();
  }

  @Test
  public void doFileCopyDataVerificationRequestTest() throws IOException {
    short requestVersionToUse = FileCopyDataVerificationRequest.CURRENT_VERSION;
    FileCopyDataVerificationRequest request = new FileCopyDataVerificationRequest(requestVersionToUse, 111,
        "id1", new MockPartitionId(), "0_0_log", Collections.singletonList(new Pair<>(0, 1000)));
    DataInputStream requestStream = serAndPrepForRead(request, -1, true);
    FileCopyDataVerificationRequest deserialisedRequest = FileCopyDataVerificationRequest.readFrom(requestStream, new MockClusterMap());

    Assert.assertEquals("0_0_log", deserialisedRequest.getFileName());
    Assert.assertEquals(0, deserialisedRequest.getPartitionId().getId());
    Assert.assertEquals("0", deserialisedRequest.getPartitionId().toPathString());
    Assert.assertEquals(111, deserialisedRequest.getCorrelationId());

    Assert.assertEquals(1, deserialisedRequest.getRanges().size());
    Assert.assertEquals(0, deserialisedRequest.getRanges().get(0).getFirst().longValue());
    Assert.assertEquals(1000, deserialisedRequest.getRanges().get(0).getSecond().longValue());

    Assert.assertEquals(requestVersionToUse, deserialisedRequest.getVersionId());
    request.release();

    try {
      // Sending CURRENT_VERSION + 1 as version. Expected to throw exception.
      new FileCopyDataVerificationRequest((short) (FileCopyDataVerificationRequest.CURRENT_VERSION + 1), 111, "id1",
          new MockPartitionId(), "0_0_log",  new ArrayList<>());
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null partition id. Expected to throw exception.
      new FileCopyDataVerificationRequest(requestVersionToUse, 111, "id1", null, "0_0_log",
          Collections.singletonList(new Pair<>(0, 1000)));
      Assert.fail("Should have failed");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      // Sending empty file name. Expected to throw exception.
      new FileCopyDataVerificationRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",
          Collections.singletonList(new Pair<>(0, 1000)));
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null ranges. Expected to throw exception.
      new FileCopyDataVerificationRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",  null);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending empty list as ranges. Expected to throw exception.
      new FileCopyDataVerificationRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",  new ArrayList<>());
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending invalid ranges [100, 50]. Expected to throw exception.
      new FileCopyDataVerificationRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",
          Collections.singletonList(new Pair<>(100, 50)));
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void doFileCopyDataVerificationResponseTest() throws IOException {
    short requestVersionToUse = FileCopyDataVerificationResponse.CURRENT_VERSION;
    FileCopyDataVerificationResponse response = new FileCopyDataVerificationResponse(requestVersionToUse, 111, "id1",
            ServerErrorCode.NoError, Collections.singletonList("checksum1"));
    DataInputStream requestStream = serAndPrepForRead(response, -1, false);
    FileCopyDataVerificationResponse deserialisedResponse = FileCopyDataVerificationResponse.readFrom(requestStream);

    Assert.assertEquals(111, deserialisedResponse.getCorrelationId());
    Assert.assertEquals(ServerErrorCode.NoError, deserialisedResponse.getError());
    Assert.assertEquals("id1", deserialisedResponse.getClientId());
    Assert.assertEquals(1, deserialisedResponse.getChecksums().size());
    Assert.assertEquals("checksum1", deserialisedResponse.getChecksums().get(0));
    Assert.assertEquals(requestVersionToUse, deserialisedResponse.getVersionId());
    response.release();

    try {
      // Sending CURRENT_VERSION + 1 as version. Expected to throw exception.
      new FileCopyDataVerificationResponse((short) (FileCopyDataVerificationResponse.CURRENT_VERSION + 1), 111, "id1",
          ServerErrorCode.NoError, Collections.singletonList("checksum1"));
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null clientId. Expected to throw exception.
      new FileCopyDataVerificationResponse(requestVersionToUse, 111, null, ServerErrorCode.NoError,
          Collections.singletonList("checksum1"));
      Assert.fail("Should have failed");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      // Sending empty checksums. Expected to throw exception.
      new FileCopyDataVerificationResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError,
          new ArrayList<>());
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null checksums. Expected to throw exception.
      new FileCopyDataVerificationResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, null);
      Assert.fail("Should have failed");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void doFileCopyChunkRequestTest() throws IOException {
    short requestVersionToUse = FileCopyGetChunkRequest.CURRENT_VERSION;
    FileCopyGetChunkRequest request = new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1",
        new MockPartitionId(), "0_0_log", "host1", "snapshotId", 1000,
        100, false);
    DataInputStream requestStream = serAndPrepForRead(request, -1, true);
    FileCopyGetChunkRequest deserialisedRequest = FileCopyGetChunkRequest.readFrom(requestStream, new MockClusterMap());

    Assert.assertEquals("0_0_log", deserialisedRequest.getFileName());
    Assert.assertEquals(100, deserialisedRequest.getChunkLengthInBytes());
    Assert.assertEquals(0, deserialisedRequest.getPartitionId().getId());
    Assert.assertEquals("0", deserialisedRequest.getPartitionId().toPathString());
    Assert.assertEquals(111, deserialisedRequest.getCorrelationId());
    Assert.assertEquals(1000, deserialisedRequest.getStartOffset());
    Assert.assertEquals(requestVersionToUse, deserialisedRequest.getVersionId());
    Assert.assertEquals(false, deserialisedRequest.isChunked());
    request.release();

    try {
      // Sending CURRENT_VERSION + 1 as version. Expected to throw exception.
      new FileCopyGetChunkRequest((short) (FileCopyGetChunkRequest.CURRENT_VERSION + 1),
          111, "id1", new MockPartitionId(), "0_0_log", "host1",
          "snapshotId", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null partition id. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", null, "0_0_log",
          "host1", "snapshotId", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (NullPointerException e){
      //expected
    }
    try {
      // Sending empty file name. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",
          "host1", "snapshotId", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending empty hostName. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",
          "", "snapshotId", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending empty snapshotId. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(), "",
          "host1", "", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending negative start offset. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_log", "host1", "snapshotId", -1, 0, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending negative chunk size. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_log", "host1", "snapshotId", 1000, -1, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending 0 chunk size. Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_log", "host1", "snapshotId", 1000, 0, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending filename "some_random_file". Expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "some_random_file", "host1", "snapshotId", 1000, 100, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e){
      //expected
    }
    try {
      // Sending request for logsegment `0_0_log`. Not expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_log", "host1", "snapshotId", 1000, 100, false);
    } catch (IllegalArgumentException e){
      Assert.fail("Not expected to fail");
    }
    try {
      // Sending request for logsegment `0_0_bloom`. Not expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_bloom", "host1", "snapshotId", 1000, 100, false);
    } catch (IllegalArgumentException e){
      Assert.fail("Not expected to fail");
    }
    try {
      // Sending request for logsegment `0_0_index`. Not expected to throw exception.
      new FileCopyGetChunkRequest(requestVersionToUse, 111, "id1", new MockPartitionId(),
          "0_0_index", "host1", "snapshotId", 1000, 100, false);
    } catch (IllegalArgumentException e){
      Assert.fail("Not expected to fail");
    }
  }

  @Test
  public void doFileCopyChunkResponseTest() throws IOException {
    short requestVersionToUse = FileCopyGetChunkResponse.CURRENT_VERSION;
    String fileName = "testFile";
    long fileSize = 12345L;
    ByteBuf byteBuf = Unpooled.buffer();
    byteBuf.writeBytes(fileName.getBytes());
    byteBuf.writeLong(fileSize);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    FileCopyGetChunkResponse response = new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1",
        ServerErrorCode.NoError, new MockPartitionId(), "file1", fileStream, 0L, fileSize, false);
    DataInputStream responseStream = serAndPrepForRead(response, -1, false);
    FileCopyGetChunkResponse deserialisedResponse = FileCopyGetChunkResponse.readFrom(responseStream, new MockClusterMap());

    Assert.assertEquals("file1", deserialisedResponse.getFileName());
    Assert.assertEquals(0L, deserialisedResponse.getStartOffset());
    Assert.assertEquals(fileSize, deserialisedResponse.getChunkSizeInBytes());
    Assert.assertEquals(111, deserialisedResponse.getCorrelationId());
    Assert.assertEquals(ServerErrorCode.NoError, deserialisedResponse.getError());
    Assert.assertEquals(0, deserialisedResponse.getPartitionId().getId());
    Assert.assertEquals("0", deserialisedResponse.getPartitionId().toPathString());
    Assert.assertEquals(requestVersionToUse, deserialisedResponse.getVersionId());
    response.release();
    byteBuf.release();

    try {
      // Sending CURRENT_VERSION + 1 as version. Expected to throw exception.
      new FileCopyGetChunkResponse((short) (FileCopyGetChunkResponse.CURRENT_VERSION + 1),
          111, "id1", ServerErrorCode.NoError, null, "file1", fileStream, 0L, fileSize, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null partition id. Expected to throw exception.
      new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, null, "file1",
          fileStream, 0L, fileSize, false);
      Assert.fail("Should have failed");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      // Sending empty file name. Expected to throw exception.
      new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, new MockPartitionId(), "",
          fileStream, 0L, fileSize, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending null file stream. Expected to throw exception.
      new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, new MockPartitionId(),
          "file1", null, 0L, fileSize, false);
      Assert.fail("Should have failed");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      // Sending negative start offset. Expected to throw exception.
      new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, new MockPartitionId(),
          "file1", fileStream, -1, fileSize, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      // Sending negative chunk size. Expected to throw exception.
      new FileCopyGetChunkResponse(requestVersionToUse, 111, "id1", ServerErrorCode.NoError, new MockPartitionId(),
          "file1", fileStream, 0L, -1, false);
      Assert.fail("Should have failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  private void doReplicaMetadataRequestTest(short responseVersionToUse, short requestVersionToUse,
      short messageInfoToUse, ReplicaType replicaType) throws IOException {
    MessageInfoAndMetadataListSerde.AUTO_VERSION = messageInfoToUse;
    MockClusterMap clusterMap = new MockClusterMap();
    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<ReplicaMetadataRequestInfo>();
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
        new ReplicaMetadataRequestInfo(new MockPartitionId(), new MockFindToken(0, 1000), "localhost", "path",
            replicaType, requestVersionToUse);
    replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
    ReplicaMetadataRequest request =
        new ReplicaMetadataRequest(1, "id", replicaMetadataRequestInfoList, 1000, requestVersionToUse);
    DataInputStream requestStream = serAndPrepForRead(request, -1, true);
    ReplicaMetadataRequest replicaMetadataRequestFromBytes =
        ReplicaMetadataRequest.readFrom(requestStream, new MockClusterMap(), new MockFindTokenHelper());
    Assert.assertEquals(replicaMetadataRequestFromBytes.getMaxTotalSizeOfEntriesInBytes(), 1000);
    Assert.assertEquals(replicaMetadataRequestFromBytes.getReplicaMetadataRequestInfoList().size(), 1);
    request.release();

    try {
      new ReplicaMetadataRequest(1, "id", null, 12, requestVersionToUse);
      Assert.fail("Serializing should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do
    }
    try {
      new ReplicaMetadataRequestInfo(new MockPartitionId(), null, "localhost", "path", replicaType,
          requestVersionToUse);
      Assert.fail("Construction should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do
    }

    long operationTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
    int numResponseInfos = 5;
    int numMessagesInEachResponseInfo = 200;
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList = new ArrayList<>();
    for (int j = 0; j < numResponseInfos; j++) {
      List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>();
      int totalSizeOfAllMessages = 0;
      for (int i = 0; i < numMessagesInEachResponseInfo; i++) {
        int msgSize = TestUtils.RANDOM.nextInt(1000) + 1;
        short accountId = Utils.getRandomShort(TestUtils.RANDOM);
        short containerId = Utils.getRandomShort(TestUtils.RANDOM);
        BlobId id = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
            ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
            clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
            BlobId.BlobDataType.DATACHUNK);
        MessageInfo messageInfo =
            new MessageInfo(id, msgSize, false, false, true, Utils.Infinite_Time, null, accountId, containerId,
                operationTimeMs, (short) 1);
        messageInfoList.add(messageInfo);
        totalSizeOfAllMessages += msgSize;
      }
      ReplicaMetadataResponseInfo responseInfo = new ReplicaMetadataResponseInfo(
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), replicaType,
          new MockFindToken(0, 1000), messageInfoList, 1000, responseVersionToUse);
      Assert.assertEquals("Total size of messages not as expected", totalSizeOfAllMessages,
          responseInfo.getTotalSizeOfAllMessages());
      replicaMetadataResponseInfoList.add(responseInfo);
    }
    ReplicaMetadataResponse response =
        new ReplicaMetadataResponse(1234, "clientId", ServerErrorCode.NoError, replicaMetadataResponseInfoList,
            responseVersionToUse);
    requestStream = serAndPrepForRead(response, -1, false);
    ReplicaMetadataResponse deserializedReplicaMetadataResponse =
        ReplicaMetadataResponse.readFrom(requestStream, new MockFindTokenHelper(), clusterMap);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getError(), ServerErrorCode.NoError);
    Assert.assertEquals("ReplicaMetadataResponse list size mismatch ", numResponseInfos,
        deserializedReplicaMetadataResponse.getReplicaMetadataResponseInfoList().size());
    for (int j = 0; j < replicaMetadataResponseInfoList.size(); j++) {
      ReplicaMetadataResponseInfo originalMetadataResponse = replicaMetadataResponseInfoList.get(j);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
          deserializedReplicaMetadataResponse.getReplicaMetadataResponseInfoList().get(j);
      Assert.assertEquals("MsgInfo list size in ReplicaMetadataResponse mismatch ", numMessagesInEachResponseInfo,
          replicaMetadataResponseInfo.getMessageInfoList().size());
      Assert.assertEquals("Total size of messages not as expected",
          originalMetadataResponse.getTotalSizeOfAllMessages(),
          replicaMetadataResponseInfo.getTotalSizeOfAllMessages());
      List<MessageInfo> deserializedMsgInfoList = replicaMetadataResponseInfo.getMessageInfoList();
      for (int i = 0; i < originalMetadataResponse.getMessageInfoList().size(); i++) {
        MessageInfo originalMsgInfo = originalMetadataResponse.getMessageInfoList().get(i);
        MessageInfo msgInfo = deserializedMsgInfoList.get(i);
        Assert.assertEquals("MsgInfo size mismatch ", originalMsgInfo.getSize(), msgInfo.getSize());
        Assert.assertEquals("MsgInfo key mismatch ", originalMsgInfo.getStoreKey(), msgInfo.getStoreKey());
        Assert.assertEquals("MsgInfo expiration value mismatch ", Utils.Infinite_Time, msgInfo.getExpirationTimeInMs());
        if (response.getVersionId() >= ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_3) {
          Assert.assertEquals("AccountId mismatch ", originalMsgInfo.getAccountId(), msgInfo.getAccountId());
          Assert.assertEquals("ContainerId mismatch ", originalMsgInfo.getContainerId(), msgInfo.getContainerId());
          Assert.assertEquals("OperationTime mismatch ", operationTimeMs, msgInfo.getOperationTimeMs());
        } else {
          Assert.assertEquals("AccountId mismatch ", UNKNOWN_ACCOUNT_ID, msgInfo.getAccountId());
          Assert.assertEquals("ContainerId mismatch ", UNKNOWN_CONTAINER_ID, msgInfo.getContainerId());
          Assert.assertEquals("OperationTime mismatch ", Utils.Infinite_Time, msgInfo.getOperationTimeMs());
        }
        if (messageInfoToUse >= MessageInfoAndMetadataListSerde.VERSION_6) {
          Assert.assertTrue(msgInfo.isUndeleted());
          Assert.assertEquals("LifeVersion mismatch", (short) 1, msgInfo.getLifeVersion());
        } else {
          Assert.assertFalse(msgInfo.isUndeleted());
          Assert.assertEquals("LifeVersion mismatch", (short) 0, msgInfo.getLifeVersion());
        }
      }
    }
    response.release();
    // to ensure that the toString() representation does not go overboard, a random bound check is executed here.
    // a rough estimate is that each response info should contribute about 500 chars to the toString() representation
    int maxLength = 100 + numResponseInfos * 500;
    Assert.assertTrue("toString() representation longer than " + maxLength + " characters",
        response.toString().length() < maxLength);
    // test toString() of a ReplicaMetadataResponseInfo without any messages
    ReplicaMetadataResponseInfo responseInfo = new ReplicaMetadataResponseInfo(
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), replicaType,
        new MockFindToken(0, 1000), Collections.emptyList(), 1000, responseVersionToUse);
    Assert.assertTrue("Length of toString() should be > 0", responseInfo.toString().length() > 0);
    // test toString() of a ReplicaMetadataResponse without any ReplicaMetadataResponseInfo
    response = new ReplicaMetadataResponse(1234, "clientId", ServerErrorCode.NoError, Collections.emptyList(),
        responseVersionToUse);
    Assert.assertTrue("Length of toString() should be > 0", response.toString().length() > 0);
    response.release();
  }

  /**
   * ReplicaMetadataRequestResonse compatibility test. Tests {@code ReplicaMetadataResponse#getCompatibleResponseVersion}
   */
  @Test
  public void getCompatibleResponseVersionTest() {
    Assert.assertEquals(
        "Request version Replica_Metadata_Request_Version_V1 should be compatible with REPLICA_METADATA_RESPONSE_VERSION_V_5",
        ReplicaMetadataResponse.getCompatibleResponseVersion(
            ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1),
        ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5);
    Assert.assertEquals(
        "Request version Replica_Metadata_Request_Version_V6 should be compatible with REPLICA_METADATA_RESPONSE_VERSION_V_6",
        ReplicaMetadataResponse.getCompatibleResponseVersion(
            ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2),
        ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6);
    Assert.assertFalse(
        "Request version Replica_Metadata_Request_Version_V1 should not be compatible with REPLICA_METADATA_RESPONSE_VERSION_V_6",
        ReplicaMetadataResponse.getCompatibleResponseVersion(ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1)
            == ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6);
    Assert.assertFalse(
        "Request version Replica_Metadata_Request_Version_V1 should not be compatible with REPLICA_METADATA_RESPONSE_VERSION_V_6",
        ReplicaMetadataResponse.getCompatibleResponseVersion(ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2)
            == ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5);
  }

  /**
   * Tests the ser/de of {@link AdminRequest} and {@link AdminResponse} and checks for equality of fields with
   * reference data.
   * @throws IOException
   */
  @Test
  public void adminRequestResponseTest() throws IOException {
    int correlationId = 1234;
    String clientId = "client";
    for (AdminRequestOrResponseType type : AdminRequestOrResponseType.values()) {
      MockClusterMap clusterMap = new MockClusterMap();
      PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      // with a valid partition id
      AdminRequest adminRequest = new AdminRequest(type, id, correlationId, clientId);
      DataInputStream requestStream = serAndPrepForRead(adminRequest, -1, true);
      deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId, type, id);
      adminRequest.release();
      // with a null partition id
      adminRequest = new AdminRequest(type, null, correlationId, clientId);
      requestStream = serAndPrepForRead(adminRequest, -1, true);
      deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId, type, null);
      adminRequest.release();
      // response
      ServerErrorCode[] values = ServerErrorCode.values();
      int indexToPick = TestUtils.RANDOM.nextInt(values.length);
      ServerErrorCode responseErrorCode = values[indexToPick];
      AdminResponse response = new AdminResponse(correlationId, clientId, responseErrorCode);
      DataInputStream responseStream = serAndPrepForRead(response, -1, false);
      AdminResponse deserializedAdminResponse = AdminResponse.readFrom(responseStream);
      Assert.assertEquals(deserializedAdminResponse.getCorrelationId(), correlationId);
      Assert.assertEquals(deserializedAdminResponse.getClientId(), clientId);
      Assert.assertEquals(deserializedAdminResponse.getError(), responseErrorCode);
      response.release();
    }
  }

  @Test
  public void adminResponseWithContentTest() throws IOException {
    int correlationId = 1;
    String clientId = "ambry-healthchecker";

    // test it with some content
    Map<String, String> returnedMap = new HashMap<>();
    returnedMap.put("key1", "value1");
    returnedMap.put("key2", "value2");
    ObjectMapper objectMapper = new ObjectMapper();
    System.out.println(objectMapper.writeValueAsString(returnedMap));
    byte[] content = objectMapper.writeValueAsBytes(returnedMap);
    AdminResponseWithContent response =
        new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.NoError, content);
    DataInputStream responseStream = serAndPrepForRead(response, -1, false);
    AdminResponseWithContent deserializedAdminResponse = AdminResponseWithContent.readFrom(responseStream);
    Assert.assertEquals(deserializedAdminResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedAdminResponse.getClientId(), clientId);
    Assert.assertEquals(deserializedAdminResponse.getError(), ServerErrorCode.NoError);
    Assert.assertNotNull(deserializedAdminResponse.getContent());

    Map<String, String> deserializedMap =
        objectMapper.readValue(deserializedAdminResponse.getContent(), new TypeReference<Map<String, String>>() {
        });
    Assert.assertEquals(returnedMap, deserializedMap);
    response.release();

    // test it with null content
    response = new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.NoError, null);
    responseStream = serAndPrepForRead(response, -1, false);
    deserializedAdminResponse = AdminResponseWithContent.readFrom(responseStream);
    Assert.assertEquals(deserializedAdminResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedAdminResponse.getClientId(), clientId);
    Assert.assertEquals(deserializedAdminResponse.getError(), ServerErrorCode.NoError);
    Assert.assertNull(deserializedAdminResponse.getContent());
    response.release();

    // test it with empty content
    response = new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.NoError, new byte[0]);
    responseStream = serAndPrepForRead(response, -1, false);
    deserializedAdminResponse = AdminResponseWithContent.readFrom(responseStream);
    Assert.assertEquals(deserializedAdminResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedAdminResponse.getClientId(), clientId);
    Assert.assertEquals(deserializedAdminResponse.getError(), ServerErrorCode.NoError);
    Assert.assertNull(deserializedAdminResponse.getContent());
    response.release();
  }

  /**
   * Tests the ser/de of {@link RequestControlAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void requestControlAdminRequestTest() throws IOException {
    for (RequestOrResponseType requestOrResponseType : RequestOrResponseType.values()) {
      doRequestControlAdminRequestTest(requestOrResponseType, true);
      doRequestControlAdminRequestTest(requestOrResponseType, false);
    }
  }

  /**
   * Tests the ser/de of {@link CatchupStatusAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void catchupStatusAdminRequestTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = 1234;
    String clientId = "client";
    // request
    long acceptableLag = Utils.getRandomLong(TestUtils.RANDOM, 10000);
    short numCaughtUpPerPartition = Utils.getRandomShort(TestUtils.RANDOM);
    AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.CatchupStatus, id, correlationId, clientId);
    CatchupStatusAdminRequest catchupStatusRequest =
        new CatchupStatusAdminRequest(acceptableLag, numCaughtUpPerPartition, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(catchupStatusRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.CatchupStatus, id);
    CatchupStatusAdminRequest deserializedCatchupStatusRequest =
        CatchupStatusAdminRequest.readFrom(requestStream, deserializedAdminRequest);
    Assert.assertEquals("Acceptable lag not as set", acceptableLag,
        deserializedCatchupStatusRequest.getAcceptableLagInBytes());
    Assert.assertEquals("Num caught up per partition not as set", numCaughtUpPerPartition,
        deserializedCatchupStatusRequest.getNumReplicasCaughtUpPerPartition());
    catchupStatusRequest.release();
    // response
    boolean isCaughtUp = TestUtils.RANDOM.nextBoolean();
    ServerErrorCode[] values = ServerErrorCode.values();
    int indexToPick = TestUtils.RANDOM.nextInt(values.length);
    ServerErrorCode responseErrorCode = values[indexToPick];
    AdminResponse adminResponse = new AdminResponse(correlationId, clientId, responseErrorCode);
    CatchupStatusAdminResponse catchupStatusResponse = new CatchupStatusAdminResponse(isCaughtUp, adminResponse);
    DataInputStream responseStream = serAndPrepForRead(catchupStatusResponse, -1, false);
    CatchupStatusAdminResponse deserializedCatchupStatusResponse = CatchupStatusAdminResponse.readFrom(responseStream);
    Assert.assertEquals(deserializedCatchupStatusResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedCatchupStatusResponse.getClientId(), clientId);
    Assert.assertEquals(deserializedCatchupStatusResponse.getError(), responseErrorCode);
    Assert.assertEquals(deserializedCatchupStatusResponse.isCaughtUp(), isCaughtUp);
    catchupStatusResponse.release();
  }

  /**
   * Tests the ser/de of {@link BlobStoreControlAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void blobStoreControlAdminRequestTest() throws IOException {
    for (BlobStoreControlAction requestType : EnumSet.allOf(BlobStoreControlAction.class)) {
      doBlobStoreControlAdminRequestTest(requestType);
    }
  }

  /**
   * Tests the ser/de of {@link ReplicationControlAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void replicationControlAdminRequestTest() throws IOException {
    int numOrigins = TestUtils.RANDOM.nextInt(8) + 2;
    List<String> origins = new ArrayList<>();
    for (int i = 0; i < numOrigins; i++) {
      origins.add(TestUtils.getRandomString(TestUtils.RANDOM.nextInt(8) + 2));
    }
    doReplicationControlAdminRequestTest(origins, true);
    doReplicationControlAdminRequestTest(origins, false);
    doReplicationControlAdminRequestTest(Collections.emptyList(), true);
  }

  @Test
  public void healthCheckAdminRequestTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    int correlationId = 1;
    String clientId = "ambry-healthchecker";
    AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.HealthCheck, null, correlationId, clientId);
    HealthCheckAdminRequest checkRequest = new HealthCheckAdminRequest(adminRequest);
    DataInputStream requestStream = serAndPrepForRead(checkRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.HealthCheck, null);
    HealthCheckAdminRequest.readFrom(requestStream, deserializedAdminRequest);
    checkRequest.release();
  }

  /**
   * Tests the ser/de of {@link BlobIndexAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void blobIndexAdminRequestTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    int correlationId = 1;
    String clientId = "ambry-blobindexer";
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM),
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobIndex, null, correlationId, clientId);
    BlobIndexAdminRequest blobIndexRequest = new BlobIndexAdminRequest(blobId, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(blobIndexRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.BlobIndex, null);
    BlobIndexAdminRequest deserilized =
        BlobIndexAdminRequest.readFrom(requestStream, deserializedAdminRequest, new BlobIdFactory(clusterMap));
    Assert.assertEquals(blobId.getID(), deserilized.getStoreKey().getID());
    blobIndexRequest.release();
  }

  /**
   * Tests the ser/de of {@link ForceDeleteAdminRequest} and checks for equality of fields with reference data.
   * @throws IOException
   */
  @Test
  public void forceDeleteAdminRequestTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    int correlationId = 1;
    String clientId = "ambry-forceDeleter";
    BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM),
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    short lifeVersion = 2;
    AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.ForceDelete, null, correlationId, clientId);
    ForceDeleteAdminRequest forceDeleteAdminRequest = new ForceDeleteAdminRequest(blobId, lifeVersion, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(forceDeleteAdminRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.ForceDelete, null);
    ForceDeleteAdminRequest deserialized =
        ForceDeleteAdminRequest.readFrom(requestStream, deserializedAdminRequest, new BlobIdFactory(clusterMap));
    Assert.assertEquals(blobId.getID(), deserialized.getStoreKey().getID());
    Assert.assertEquals(lifeVersion, deserialized.getLifeVersion());
    forceDeleteAdminRequest.release();
  }

  /**
   * Tests for {@link TtlUpdateRequest} and {@link TtlUpdateResponse}.
   * @throws IOException
   */
  @Test
  public void ttlUpdateRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    int correlationId = TestUtils.RANDOM.nextInt();
    long opTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    short[] versions = new short[]{TtlUpdateRequest.TTL_UPDATE_REQUEST_VERSION_1};
    for (short version : versions) {
      TtlUpdateRequest ttlUpdateRequest =
          new TtlUpdateRequest(correlationId, "client", id1, expiresAtMs, opTimeMs, version);
      DataInputStream requestStream = serAndPrepForRead(ttlUpdateRequest, -1, true);
      TtlUpdateRequest deserializedTtlUpdateRequest = TtlUpdateRequest.readFrom(requestStream, clusterMap);
      Assert.assertEquals("Request type mismatch", RequestOrResponseType.TtlUpdateRequest,
          deserializedTtlUpdateRequest.getRequestType());
      Assert.assertEquals("Correlation ID mismatch", correlationId, deserializedTtlUpdateRequest.getCorrelationId());
      Assert.assertEquals("Client ID mismatch", "client", deserializedTtlUpdateRequest.getClientId());
      Assert.assertEquals("Blob ID mismatch", id1, deserializedTtlUpdateRequest.getBlobId());
      Assert.assertEquals("AccountId mismatch ", id1.getAccountId(), deserializedTtlUpdateRequest.getAccountId());
      Assert.assertEquals("ContainerId mismatch ", id1.getContainerId(), deserializedTtlUpdateRequest.getContainerId());
      Assert.assertEquals("ExpiresAtMs mismatch ", expiresAtMs, deserializedTtlUpdateRequest.getExpiresAtMs());
      Assert.assertEquals("DeletionTime mismatch ", opTimeMs, deserializedTtlUpdateRequest.getOperationTimeInMs());
      ttlUpdateRequest.release();

      TtlUpdateResponse response = new TtlUpdateResponse(correlationId, "client", ServerErrorCode.NoError);
      requestStream = serAndPrepForRead(response, -1, false);
      TtlUpdateResponse deserializedTtlUpdateResponse = TtlUpdateResponse.readFrom(requestStream);
      Assert.assertEquals("Response type mismatch", RequestOrResponseType.TtlUpdateResponse,
          deserializedTtlUpdateResponse.getRequestType());
      Assert.assertEquals("Correlation ID mismatch", correlationId, deserializedTtlUpdateResponse.getCorrelationId());
      Assert.assertEquals("Client ID mismatch", "client", deserializedTtlUpdateResponse.getClientId());
      Assert.assertEquals("Server error code mismatch", ServerErrorCode.NoError,
          deserializedTtlUpdateResponse.getError());
      response.release();
    }
  }

  /**
   * Tests for {@link ReplicateBlobRequest} and {@link ReplicateBlobResponse}.
   * @throws IOException
   */
  @Test
  public void replicateBlobRequestResponseTest() throws IOException {
    final MockClusterMap clusterMap = new MockClusterMap();
    final short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    final short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    final String clientId = "client";
    final BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    final String sourceHostName = "datacenter1_host1";
    final int sourceHostPort = 15058;
    final int correlationId = TestUtils.RANDOM.nextInt();

    final RequestOrResponseType operationType =
        RequestOrResponseType.values()[Utils.getRandomShort(TestUtils.RANDOM) % RequestOrResponseType.values().length];
    final long operationTime = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
    final short lifeVersion = Utils.getRandomShort(TestUtils.RANDOM);
    final long operationParameter = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    short[] versions = new short[]{ReplicateBlobRequest.VERSION_1, ReplicateBlobRequest.VERSION_2};
    for (short version : versions) {
      final ReplicateBlobRequest replicateBlobRequest;
      if (version == ReplicateBlobRequest.VERSION_1) {
        replicateBlobRequest = new ReplicateBlobRequest(correlationId, clientId, id1, sourceHostName, sourceHostPort);
      } else {
        replicateBlobRequest =
            new ReplicateBlobRequest(correlationId, clientId, id1, sourceHostName, sourceHostPort, operationType,
                operationTime, lifeVersion, operationParameter);
      }

      DataInputStream requestStream = serAndPrepForRead(replicateBlobRequest, -1, true);
      final ReplicateBlobRequest deserializedReplicateBlobRequest =
          ReplicateBlobRequest.readFrom(requestStream, clusterMap);
      verifyReplicateBlobRequest(replicateBlobRequest, deserializedReplicateBlobRequest);
      replicateBlobRequest.release();

      final ReplicateBlobResponse response =
          new ReplicateBlobResponse(correlationId, clientId, ServerErrorCode.NoError);
      requestStream = serAndPrepForRead(response, -1, false);
      final ReplicateBlobResponse deserializedReplicateBlobResponse = ReplicateBlobResponse.readFrom(requestStream);
      verifyReplicateBlobResponse(response, deserializedReplicateBlobResponse);
      response.release();
    }
  }

  /**
   * Tests for {@link BatchDeleteRequest} and {@link BatchDeleteResponse}.
   * @throws IOException
   */
  @Test
  public void testBatchDeleteRequestResponse() throws IOException {
    final MockClusterMap clusterMap = new MockClusterMap();
    final short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    final short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    final String clientId = "client";
    final PartitionId partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    final BlobId blobId1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
    final BlobId blobId2 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId, false, BlobId.BlobDataType.DATACHUNK);
    final int correlationId = TestUtils.RANDOM.nextInt();
    final long deletionTimeInMs = 1L;

    BatchDeletePartitionRequestInfo batchDeletePartitionRequestInfo =
        new BatchDeletePartitionRequestInfo(partitionId, Arrays.asList(blobId1, blobId2));
    BatchDeleteRequest batchDeleteRequest =
        new BatchDeleteRequest(correlationId, clientId, Collections.singletonList(batchDeletePartitionRequestInfo),
            deletionTimeInMs);

    DataInputStream requestStream = serAndPrepForRead(batchDeleteRequest, -1, true);
    final BatchDeleteRequest deserializedBatchDeleteRequest = BatchDeleteRequest.readFrom(requestStream, clusterMap);
    //verify the request
    verifyBatchDeleteRequest(batchDeleteRequest, deserializedBatchDeleteRequest);
    batchDeleteRequest.release();

    final BatchDeletePartitionResponseInfo batchDeletePartitionResponseInfo =
        new BatchDeletePartitionResponseInfo(partitionId,
            Arrays.asList(new BlobDeleteStatus(blobId1, ServerErrorCode.NoError),
                new BlobDeleteStatus(blobId2, ServerErrorCode.NoError)));
    final BatchDeleteResponse batchDeleteResponse = new BatchDeleteResponse((short) 1, correlationId, clientId,
        Collections.singletonList(batchDeletePartitionResponseInfo), ServerErrorCode.NoError);
    requestStream = serAndPrepForRead(batchDeleteResponse, -1, false);
    final BatchDeleteResponse deserializedBatchDeleteResponse = BatchDeleteResponse.readFrom(requestStream, clusterMap);
    //verify the response
    verifyBatchDeleteResponse(batchDeleteResponse, deserializedBatchDeleteResponse);
    batchDeleteResponse.release();
  }

  /**
   * Tests for {@link BatchDeleteRequest} and {@link BatchDeleteResponse} with multiple partitions.
   * @throws IOException
   */
  @Test
  public void testBatchDeleteRequestResponseWithMultiplePartitions() throws IOException {
    final MockClusterMap clusterMap = new MockClusterMap();
    final short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    final short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    final String clientId = "client";
    //Partition 0
    final PartitionId partitionId0 = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    final BlobId blobId1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId0, false, BlobId.BlobDataType.DATACHUNK);
    final BlobId blobId2 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId0, false, BlobId.BlobDataType.DATACHUNK);

    //Partition 1
    final PartitionId partitionId1 = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(1);
    final BlobId blobId3 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId1, false, BlobId.BlobDataType.DATACHUNK);
    final BlobId blobId4 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId, partitionId1, false, BlobId.BlobDataType.DATACHUNK);

    final int correlationId = TestUtils.RANDOM.nextInt();
    final RequestOrResponseType operationType =
        RequestOrResponseType.values()[Utils.getRandomShort(TestUtils.RANDOM) % RequestOrResponseType.values().length];
    final long deletionTimeInMs = 1L;

    //Delete Partition Request Info for Partition 0
    final BatchDeletePartitionRequestInfo batchDeletePartitionRequestInfo0 =
        new BatchDeletePartitionRequestInfo(partitionId0, Arrays.asList(blobId1, blobId2));
    //Delete Partition Request Info for Partition 1
    final BatchDeletePartitionRequestInfo batchDeletePartitionRequestInfo1 =
        new BatchDeletePartitionRequestInfo(partitionId1, Arrays.asList(blobId3, blobId4));

    //Batch Delete Request
    final BatchDeleteRequest batchDeleteRequest = new BatchDeleteRequest(correlationId, clientId,
        Arrays.asList(batchDeletePartitionRequestInfo0, batchDeletePartitionRequestInfo1), deletionTimeInMs);

    DataInputStream requestStream = serAndPrepForRead(batchDeleteRequest, -1, true);
    final BatchDeleteRequest deserializedBatchDeleteRequest = BatchDeleteRequest.readFrom(requestStream, clusterMap);
    //verify the request
    verifyBatchDeleteRequest(batchDeleteRequest, deserializedBatchDeleteRequest);
    batchDeleteRequest.release();

    //Delete Partition Response Info for Partition 0
    final BatchDeletePartitionResponseInfo batchDeletePartitionResponseInfo0 =
        new BatchDeletePartitionResponseInfo(partitionId0,
            Arrays.asList(new BlobDeleteStatus(blobId1, ServerErrorCode.NoError),
                new BlobDeleteStatus(blobId2, ServerErrorCode.BlobNotFound)));
    //Delete Partition Response Info for Partition 1
    final BatchDeletePartitionResponseInfo batchDeletePartitionResponseInfo1 =
        new BatchDeletePartitionResponseInfo(partitionId1,
            Arrays.asList(new BlobDeleteStatus(blobId3, ServerErrorCode.BlobDeleted),
                new BlobDeleteStatus(blobId4, ServerErrorCode.BlobNotDeleted)));
    //Batch Delete Response
    final BatchDeleteResponse batchDeleteResponse = new BatchDeleteResponse((short) 1, correlationId, clientId,
        Arrays.asList(batchDeletePartitionResponseInfo0, batchDeletePartitionResponseInfo1), ServerErrorCode.NoError);
    requestStream = serAndPrepForRead(batchDeleteResponse, -1, false);
    final BatchDeleteResponse deserializedBatchDeleteResponse = BatchDeleteResponse.readFrom(requestStream, clusterMap);
    //verify the response
    verifyBatchDeleteResponse(batchDeleteResponse, deserializedBatchDeleteResponse);
    batchDeleteResponse.release();
  }

  /**
   * Verify the two {@link BatchDeleteRequest} are the same
   * @param orgReq the original {@link BatchDeleteRequest}
   * @param deserializedReq the deserialized {@link BatchDeleteRequest}
   */
  private void verifyBatchDeleteRequest(BatchDeleteRequest orgReq, BatchDeleteRequest deserializedReq) {
    Assert.assertEquals(orgReq.type, RequestOrResponseType.BatchDeleteRequest);
    Assert.assertEquals(orgReq.type, deserializedReq.type);
    Assert.assertEquals(orgReq.getCorrelationId(), deserializedReq.getCorrelationId());
    Assert.assertEquals(orgReq.getClientId(), deserializedReq.getClientId());
    Assert.assertEquals(orgReq.getDeletionTimeInMs(), deserializedReq.getDeletionTimeInMs());
    Assert.assertEquals(orgReq.getPartitionRequestInfoList().size(),
        deserializedReq.getPartitionRequestInfoList().size());
    for (int i = 0; i < orgReq.getPartitionRequestInfoList().size(); i++) {
      BatchDeletePartitionRequestInfo orgPartitionRequestInfo = orgReq.getPartitionRequestInfoList().get(i);
      BatchDeletePartitionRequestInfo deserializedPartitionRequestInfo =
          deserializedReq.getPartitionRequestInfoList().get(i);
      Assert.assertEquals(orgPartitionRequestInfo.getPartition(), deserializedPartitionRequestInfo.getPartition());
      Assert.assertEquals(orgPartitionRequestInfo.getBlobIds().size(),
          deserializedPartitionRequestInfo.getBlobIds().size());
      for (int j = 0; j < orgPartitionRequestInfo.getBlobIds().size(); j++) {
        Assert.assertEquals(orgPartitionRequestInfo.getBlobIds().get(j),
            deserializedPartitionRequestInfo.getBlobIds().get(j));
      }
    }
  }

  /**
   * Verify the two {@link BatchDeleteResponse} are the same
   * @param orgRes the original {@link BatchDeleteResponse}
   * @param deserializedRes the deserialized {@link BatchDeleteResponse}
   */
  private void verifyBatchDeleteResponse(BatchDeleteResponse orgRes, BatchDeleteResponse deserializedRes) {
    Assert.assertEquals(orgRes.type, RequestOrResponseType.BatchDeleteResponse);
    Assert.assertEquals(orgRes.type, deserializedRes.type);
    Assert.assertEquals(orgRes.getCorrelationId(), deserializedRes.getCorrelationId());
    Assert.assertEquals(orgRes.getClientId(), deserializedRes.getClientId());
    Assert.assertEquals(orgRes.getError(), deserializedRes.getError());
    Assert.assertEquals(orgRes.getPartitionResponseInfoList().size(),
        deserializedRes.getPartitionResponseInfoList().size());
    for (int i = 0; i < orgRes.getPartitionResponseInfoList().size(); i++) {
      BatchDeletePartitionResponseInfo orgPartitionResponseInfo = orgRes.getPartitionResponseInfoList().get(i);
      BatchDeletePartitionResponseInfo deserializedPartitionResponseInfo =
          deserializedRes.getPartitionResponseInfoList().get(i);
      Assert.assertEquals(orgPartitionResponseInfo.getPartition(), deserializedPartitionResponseInfo.getPartition());
      Assert.assertEquals(orgPartitionResponseInfo.getBlobsDeleteStatus().size(),
          deserializedPartitionResponseInfo.getBlobsDeleteStatus().size());
      for (int j = 0; j < orgPartitionResponseInfo.getBlobsDeleteStatus().size(); j++) {
        Assert.assertEquals(orgPartitionResponseInfo.getBlobsDeleteStatus().get(j).getBlobId(),
            deserializedPartitionResponseInfo.getBlobsDeleteStatus().get(j).getBlobId());
        Assert.assertEquals(orgPartitionResponseInfo.getBlobsDeleteStatus().get(j).getStatus(),
            deserializedPartitionResponseInfo.getBlobsDeleteStatus().get(j).getStatus());
      }
    }
  }

  /**
   * Verify the two {@link ReplicateBlobRequest} are the same
   * @param orgReq the original {@link ReplicateBlobRequest}
   * @param deserializedReq the deserialized {@link ReplicateBlobRequest}
   */
  private void verifyReplicateBlobRequest(ReplicateBlobRequest orgReq, ReplicateBlobRequest deserializedReq) {
    Assert.assertEquals(orgReq.getCorrelationId(), deserializedReq.getCorrelationId());
    Assert.assertEquals(orgReq.getAccountId(), deserializedReq.getAccountId());
    Assert.assertEquals(orgReq.getContainerId(), deserializedReq.getContainerId());
    Assert.assertEquals(orgReq.getClientId(), deserializedReq.getClientId());
    Assert.assertEquals(orgReq.getBlobId(), deserializedReq.getBlobId());
    Assert.assertEquals(orgReq.getSourceHostName(), deserializedReq.getSourceHostName());
    Assert.assertEquals(orgReq.getSourceHostPort(), deserializedReq.getSourceHostPort());

    if (orgReq.versionId == ReplicateBlobRequest.VERSION_1) {
      Assert.assertEquals(RequestOrResponseType.PutRequest, deserializedReq.getOperationType());
      Assert.assertEquals(0, deserializedReq.getOperationTimeInMs());
      Assert.assertEquals(-1, deserializedReq.getLifeVersion());
      Assert.assertEquals(0, deserializedReq.getExpirationTimeInMs());
    } else {
      Assert.assertEquals(orgReq.getOperationType(), deserializedReq.getOperationType());
      Assert.assertEquals(orgReq.getOperationTimeInMs(), deserializedReq.getOperationTimeInMs());
      Assert.assertEquals(orgReq.getLifeVersion(), deserializedReq.getLifeVersion());
      Assert.assertEquals(orgReq.getExpirationTimeInMs(), deserializedReq.getExpirationTimeInMs());
    }
  }

  /**
   * Verify the two {@link ReplicateBlobResponse} are the same
   * @param orgRes the original {@link ReplicateBlobResponse}
   * @param deserializedRes the deserialized {@link ReplicateBlobResponse}
   */
  private void verifyReplicateBlobResponse(ReplicateBlobResponse orgRes, ReplicateBlobResponse deserializedRes) {
    Assert.assertEquals(deserializedRes.getCorrelationId(), orgRes.getCorrelationId());
    Assert.assertEquals(deserializedRes.getClientId(), orgRes.getClientId());
    Assert.assertEquals(deserializedRes.getRequestType(), RequestOrResponseType.ReplicateBlobResponse);
    Assert.assertEquals(deserializedRes.getError(), orgRes.getError());
  }

  /**
   * Does the actual test of ser/de of {@link BlobStoreControlAdminRequest} and checks for equality of fields with
   * reference data.
   * @param storeControlRequestType the type of store control request specified in {@link BlobStoreControlAdminRequest}.
   * @throws IOException
   */
  private void doBlobStoreControlAdminRequestTest(BlobStoreControlAction storeControlRequestType) throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = 1234;
    String clientId = "client";
    // test BlobStore Control request
    short numCaughtUpPerPartition = Utils.getRandomShort(TestUtils.RANDOM);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.BlobStoreControl, id, correlationId, clientId);
    BlobStoreControlAdminRequest blobStoreControlAdminRequest =
        new BlobStoreControlAdminRequest(numCaughtUpPerPartition, storeControlRequestType, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(blobStoreControlAdminRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.BlobStoreControl, id);
    BlobStoreControlAdminRequest deserializedBlobStoreControlRequest =
        BlobStoreControlAdminRequest.readFrom(requestStream, deserializedAdminRequest);
    Assert.assertEquals("Num caught up per partition not as set", numCaughtUpPerPartition,
        deserializedBlobStoreControlRequest.getNumReplicasCaughtUpPerPartition());
    Assert.assertEquals("Control request type is not expected", storeControlRequestType,
        deserializedBlobStoreControlRequest.getStoreControlAction());
    // test toString method
    String correctString = "BlobStoreControlAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", BlobStoreControlAction=" + deserializedBlobStoreControlRequest.getStoreControlAction()
        + ", NumReplicasCaughtUpPerPartition="
        + deserializedBlobStoreControlRequest.getNumReplicasCaughtUpPerPartition() + ", PartitionId="
        + deserializedBlobStoreControlRequest.getPartitionId() + "]";
    Assert.assertEquals("The test of toString method fails", correctString, "" + deserializedBlobStoreControlRequest);
    blobStoreControlAdminRequest.release();
  }

  /**
   * Serializes a {@link RequestOrResponseType} and prepares it for reading.
   * @param requestOrResponse the {@link RequestOrResponseType} to serialize.
   * @param channelSize the amount of data that the output channel should read in one iteration. Setting this to -1
   *                       will set the size of the output channel buffer to 1/3rd the size of {@code requestOrResponse}
   * @param isRequest {@code true} if {@code requestOrResponse} is a request. {@code false} otherwise.
   * @return the serialized form of {@code requestOrResponse} as a {@link DataInputStream}.
   * @throws IOException
   */
  private DataInputStream serAndPrepForRead(RequestOrResponse requestOrResponse, int channelSize, boolean isRequest)
      throws IOException {
    DataInputStream stream;
    if (useByteBufContent && requestOrResponse.content() != null) {
      stream = new NettyByteBufDataInputStream(requestOrResponse.content());
    } else {
      if (channelSize == -1) {
        channelSize = (int) (requestOrResponse.sizeInBytes() / 3);
      }
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      int expectedWriteToCount = (int) ((requestOrResponse.sizeInBytes() + channelSize - 1) / channelSize);
      int actualWriteToCount = 0;
      do {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(channelSize));
        requestOrResponse.writeTo(channel);
        ByteBuffer underlyingBuf = channel.getBuffer();
        underlyingBuf.flip();
        outputStream.write(underlyingBuf.array(), underlyingBuf.arrayOffset(), underlyingBuf.remaining());
        actualWriteToCount++;
      } while (!requestOrResponse.isSendComplete());
      Assert.assertEquals("Should not have written anything", 0,
          requestOrResponse.writeTo(new ByteBufferChannel(ByteBuffer.allocate(1))));
      Assert.assertEquals("writeTo() should have written out as much as the channel could take in every call",
          expectedWriteToCount, actualWriteToCount);
      stream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    }
    // read length
    stream.readLong();
    if (isRequest) {
      // read version
      Assert.assertEquals(RequestOrResponseType.values()[stream.readShort()], requestOrResponse.getRequestType());
    }
    return stream;
  }

  /**
   * De-serializes {@code adminRequest} and verifies the fields.
   * @param requestStream the {@link DataInputStream} that contains the request stream
   * @param clusterMap the {@link ClusterMap} to use for deser
   * @param correlationId the correlation id of the request
   * @param clientId the client of the request
   * @param adminRequestType the {@link AdminRequestOrResponseType}
   * @param id the partition ID in the request (can be {@code null}).
   * @return the deserialized {@link AdminRequest}
   * @throws IOException
   */
  private AdminRequest deserAdminRequestAndVerify(DataInputStream requestStream, ClusterMap clusterMap,
      int correlationId, String clientId, AdminRequestOrResponseType adminRequestType, PartitionId id)
      throws IOException {
    AdminRequest deserializedAdminRequest = AdminRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedAdminRequest.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedAdminRequest.getClientId(), clientId);
    Assert.assertEquals(deserializedAdminRequest.getType(), adminRequestType);
    if (id != null) {
      Assert.assertTrue(deserializedAdminRequest.getPartitionId().isEqual(id.toPathString()));
    } else {
      Assert.assertNull(deserializedAdminRequest.getPartitionId());
    }
    return deserializedAdminRequest;
  }

  /**
   * Does the actual test of ser/de of {@link RequestControlAdminRequest} and checks for equality of fields with
   * reference data.
   * @param requestOrResponseType the {@link RequestOrResponseType} to be used in the {@link RequestControlAdminRequest}
   * @param enable the value for the enable field in {@link RequestControlAdminRequest}.
   * @throws IOException
   */
  private void doRequestControlAdminRequestTest(RequestOrResponseType requestOrResponseType, boolean enable)
      throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = 1234;
    String clientId = "client";
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.RequestControl, id, correlationId, clientId);
    RequestControlAdminRequest controlRequest =
        new RequestControlAdminRequest(requestOrResponseType, enable, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(controlRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.RequestControl, id);
    RequestControlAdminRequest deserializedControlRequest =
        RequestControlAdminRequest.readFrom(requestStream, deserializedAdminRequest);
    Assert.assertEquals(requestOrResponseType, deserializedControlRequest.getRequestTypeToControl());
    Assert.assertEquals(enable, deserializedControlRequest.shouldEnable());
    controlRequest.release();
  }

  /**
   * Does the actual test of ser/de of {@link ReplicationControlAdminRequest} and checks for equality of fields with
   * reference data.
   * @param origins the origins list to use in {@link ReplicationControlAdminRequest}.
   * @param enable the value for the enable field in {@link ReplicationControlAdminRequest}.
   * @throws IOException
   */
  private void doReplicationControlAdminRequestTest(List<String> origins, boolean enable) throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    int correlationId = 1234;
    String clientId = "client";
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.ReplicationControl, id, correlationId, clientId);
    ReplicationControlAdminRequest controlRequest = new ReplicationControlAdminRequest(origins, enable, adminRequest);
    DataInputStream requestStream = serAndPrepForRead(controlRequest, -1, true);
    AdminRequest deserializedAdminRequest =
        deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId,
            AdminRequestOrResponseType.ReplicationControl, id);
    ReplicationControlAdminRequest deserializedControlRequest =
        ReplicationControlAdminRequest.readFrom(requestStream, deserializedAdminRequest);
    Assert.assertEquals(origins, deserializedControlRequest.getOrigins());
    Assert.assertEquals(enable, deserializedControlRequest.shouldEnable());
    controlRequest.release();
  }

  /**
   * Class representing {@link DeleteRequest} in version {@link DeleteRequest#DELETE_REQUEST_VERSION_1}
   */
  private class DeleteRequestV1 extends DeleteRequest {
    /**
     * Constructs {@link DeleteRequest} in {@link #DELETE_REQUEST_VERSION_1}
     * @param correlationId correlationId of the delete request
     * @param clientId clientId of the delete request
     * @param blobId blobId of the delete request
     */
    private DeleteRequestV1(int correlationId, String clientId, BlobId blobId) {
      super(correlationId, clientId, blobId, Utils.Infinite_Time, DeleteRequest.DELETE_REQUEST_VERSION_1, false);
    }
  }

  /**
   * A mock {@link Send} implementation that returns non-null value for {@link #content()} method.
   */
  private static class SendWithContent extends AbstractByteBufHolder<SendWithContent> implements Send {
    protected ByteBuf buf;
    private final int size;

    public SendWithContent(int size, boolean useComposite) {
      this.size = size;
      if (useComposite) {
        int halfSize = size / 2;
        int otherHalfSize = size - halfSize;
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeHeapBuffer(2);
        compositeByteBuf.addComponent(true,
            PooledByteBufAllocator.DEFAULT.heapBuffer(halfSize).writeBytes(new byte[halfSize]));
        compositeByteBuf.addComponent(true,
            PooledByteBufAllocator.DEFAULT.heapBuffer(otherHalfSize).writeBytes(new byte[otherHalfSize]));
        buf = compositeByteBuf;
      } else {
        buf = PooledByteBufAllocator.DEFAULT.heapBuffer(this.size).writeBytes(new byte[this.size]);
      }
    }

    @Override
    public long writeTo(WritableByteChannel channel) throws IOException {
      long written = channel.write(buf.nioBuffer());
      buf.skipBytes((int) written);
      return written;
    }

    @Override
    public boolean isSendComplete() {
      return buf.readableBytes() == 0;
    }

    @Override
    public long sizeInBytes() {
      return this.size;
    }

    @Override
    public ByteBuf content() {
      return buf;
    }

    @Override
    public SendWithContent replace(ByteBuf content) {
      return null;
    }
  }

  /**
   * A mock {@link Send} implementation that returns null value for {@link #content()} method.
   */
  private static class SendWithoutContent extends SendWithContent {

    public SendWithoutContent(int size, boolean useComposite) {
      super(size, useComposite);
    }

    @Override
    public ByteBuf content() {
      return null;
    }

    @Override
    public SendWithoutContent replace(ByteBuf content) {
      return null;
    }

    @Override
    public boolean release() {
      return buf.release();
    }
  }
}
