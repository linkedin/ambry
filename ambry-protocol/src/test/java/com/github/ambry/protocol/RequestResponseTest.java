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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferChannel;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

  public MockFindTokenFactory(StoreKeyFactory factory) {

  }

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
public class RequestResponseTest {
  private static short versionSaved;

  @BeforeClass
  public static void saveVersionToUse() {
    versionSaved = GetResponse.CURRENT_VERSION;
  }

  @After
  public void resetVersionToUse() {
    GetResponse.CURRENT_VERSION = versionSaved;
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
    doPutRequestTest((short) -1, clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, blobType,
        blob, blobSize, blobKey, blobKey);
    doPutRequestTest(PutRequest.PUT_REQUEST_VERSION_V4, clusterMap, correlationId, clientId, blobId, blobProperties,
        userMetadata, blobType, blob, blobSize, null, null);
    doPutRequestTest(PutRequest.PUT_REQUEST_VERSION_V4, clusterMap, correlationId, clientId, blobId, blobProperties,
        userMetadata, blobType, blob, blobSize, new byte[0], null);
    doPutRequestTest(PutRequest.PUT_REQUEST_VERSION_V4, clusterMap, correlationId, clientId, blobId, blobProperties,
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
    int sizeInBytes =
        (int) new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            Unpooled.wrappedBuffer(blob), blobSize, blobType,
            blobKey == null ? null : ByteBuffer.wrap(blobKey)).sizeInBytes();
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
                ((CompositeByteBuf)blobBuf).addComponent(true, blob1);
                ((CompositeByteBuf)blobBuf).addComponent(true, blob2);
                ((CompositeByteBuf)blobBuf).addComponent(true, blob3);
              }
              request = new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
                  blobBuf, blobSize, blobType, blobKey == null ? null : ByteBuffer.wrap(blobKey));
            }
            requestStream = serAndPrepForRead(request, allocationSize, true);
            PutRequest deserializedPutRequest = PutRequest.readFrom(requestStream, clusterMap);
            Assert.assertEquals(blobId, deserializedPutRequest.getBlobId());
            Assert.assertEquals(sizeInBlobProperties, deserializedPutRequest.getBlobProperties().getBlobSize());
            Assert.assertArrayEquals(userMetadata, deserializedPutRequest.getUsermetadata().array());
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
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(TestUtils.RANDOM),
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
            TestUtils.RANDOM.nextBoolean(), null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize, blobKey);
    doPutRequestTest(InvalidVersionPutRequest.Put_Request_Invalid_version, clusterMap, correlationId, clientId, blobId,
        blobProperties, userMetadata, BlobType.DataBlob, blob, blobSize, blobKey, null);

    // Put Request with size in blob properties different from the data size and blob type: Data blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            TestUtils.RANDOM.nextBoolean(), null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize, blobKey);

    // Put Request with size in blob properties different from the data size and blob type: Metadata blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            TestUtils.RANDOM.nextBoolean(), null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.MetadataBlob,
        blob, blobSize, blobKey);

    // Put Request with empty user metadata.
    byte[] emptyUserMetadata = new byte[0];
    blobProperties = new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), TestUtils.RANDOM.nextBoolean(),
        null);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, emptyUserMetadata, BlobType.DataBlob,
        blob, blobSize, blobKey);

    // Response test
    PutResponse response = new PutResponse(1234, clientId, ServerErrorCode.No_Error);
    DataInputStream responseStream = serAndPrepForRead(response, -1, false);
    PutResponse deserializedPutResponse = PutResponse.readFrom(responseStream);
    Assert.assertEquals(deserializedPutResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedPutResponse.getError(), ServerErrorCode.No_Error);
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
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
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
        new GetResponse(1234, "clientId", partitionResponseInfoList, byteStream, ServerErrorCode.No_Error);
    requestStream = serAndPrepForRead(response, -1, false);
    GetResponse deserializedGetResponse = GetResponse.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedGetResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedGetResponse.getError(), ServerErrorCode.No_Error);
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
  }

  @Test
  public void deleteRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
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
      DeleteResponse response = new DeleteResponse(correlationId, "client", ServerErrorCode.No_Error);
      requestStream = serAndPrepForRead(response, -1, false);
      DeleteResponse deserializedDeleteResponse = DeleteResponse.readFrom(requestStream);
      Assert.assertEquals(deserializedDeleteResponse.getCorrelationId(), correlationId);
      Assert.assertEquals(deserializedDeleteResponse.getError(), ServerErrorCode.No_Error);
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
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
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
    UndeleteResponse response = null;
    try {
      response = new UndeleteResponse(correlationId, "client", ServerErrorCode.No_Error);
      Assert.fail("No Error is not a valid error node for this response");
    } catch (IllegalArgumentException e) {
      // do nothing
    }
    response = new UndeleteResponse(correlationId, "client", ServerErrorCode.Blob_Deleted);
    requestStream = serAndPrepForRead(response, -1, false);
    UndeleteResponse deserializedUndeleteResponse = UndeleteResponse.readFrom(requestStream);
    Assert.assertEquals(deserializedUndeleteResponse.getCorrelationId(), correlationId);
    Assert.assertEquals(deserializedUndeleteResponse.getError(), ServerErrorCode.Blob_Deleted);
    Assert.assertEquals(deserializedUndeleteResponse.getLifeVersion(), UndeleteResponse.INVALID_LIFE_VERSION);

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
    Assert.assertEquals(deserializedUndeleteResponse.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals(deserializedUndeleteResponse.getLifeVersion(), lifeVersion);
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
            ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
            clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
            BlobId.BlobDataType.DATACHUNK);
        MessageInfo messageInfo =
            new MessageInfo(id, msgSize, false, false, true, Utils.Infinite_Time, null, accountId, containerId, operationTimeMs,
                (short) 1);
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
        new ReplicaMetadataResponse(1234, "clientId", ServerErrorCode.No_Error, replicaMetadataResponseInfoList,
            responseVersionToUse);
    requestStream = serAndPrepForRead(response, -1, false);
    ReplicaMetadataResponse deserializedReplicaMetadataResponse =
        ReplicaMetadataResponse.readFrom(requestStream, new MockFindTokenHelper(), clusterMap);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getError(), ServerErrorCode.No_Error);
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
    response = new ReplicaMetadataResponse(1234, "clientId", ServerErrorCode.No_Error, Collections.emptyList(),
        responseVersionToUse);
    Assert.assertTrue("Length of toString() should be > 0", response.toString().length() > 0);
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
      // with a null partition id
      adminRequest = new AdminRequest(type, null, correlationId, clientId);
      requestStream = serAndPrepForRead(adminRequest, -1, true);
      deserAdminRequestAndVerify(requestStream, clusterMap, correlationId, clientId, type, null);
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
    }
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
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
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

      TtlUpdateResponse response = new TtlUpdateResponse(correlationId, "client", ServerErrorCode.No_Error);
      requestStream = serAndPrepForRead(response, -1, false);
      TtlUpdateResponse deserializedTtlUpdateResponse = TtlUpdateResponse.readFrom(requestStream);
      Assert.assertEquals("Response type mismatch", RequestOrResponseType.TtlUpdateResponse,
          deserializedTtlUpdateResponse.getRequestType());
      Assert.assertEquals("Correlation ID mismatch", correlationId, deserializedTtlUpdateResponse.getCorrelationId());
      Assert.assertEquals("Client ID mismatch", "client", deserializedTtlUpdateResponse.getClientId());
      Assert.assertEquals("Server error code mismatch", ServerErrorCode.No_Error,
          deserializedTtlUpdateResponse.getError());
    }
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
    DataInputStream stream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
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
      Assert.assertTrue(deserializedAdminRequest.getPartitionId().isEqual(id.toString()));
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
      super(correlationId, clientId, blobId, Utils.Infinite_Time, DeleteRequest.DELETE_REQUEST_VERSION_1);
    }
  }
}
