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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;


class MockFindTokenFactory implements FindTokenFactory {

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
  int index;
  long bytesRead;

  public MockFindToken(int index, long bytesRead) {
    this.index = index;
    this.bytesRead = bytesRead;
  }

  public MockFindToken(DataInputStream stream) throws IOException {
    this.index = stream.readInt();
    this.bytesRead = stream.readLong();
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
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
}

class InvalidVersionPutRequest extends PutRequest {
  static final short Put_Request_Invalid_version = 0;

  public InvalidVersionPutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, ByteBuffer blob, long blobSize, BlobType blobType) {
    super(correlationId, clientId, blobId, properties, usermetadata, blob, blobSize, blobType);
    versionId = Put_Request_Invalid_version;
  }
}

public class RequestResponseTest {
  private final Random random = new Random();

  private void testPutRequest(MockClusterMap clusterMap, int correlationId, String clientId, BlobId blobId,
      BlobProperties blobProperties, byte[] userMetadata, BlobType blobType, byte[] blob, int blobSize)
      throws IOException {
    // This PutRequest is created just to get the size.
    int sizeInBytes =
        (int) new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            ByteBuffer.wrap(blob), blobSize, blobType).sizeInBytes();
    // Initialize channel write limits in such a way that writeTo() may or may not be able to write out all the
    // data at once.
    int channelWriteLimits[] =
        {sizeInBytes, 2 * sizeInBytes, sizeInBytes / 2, sizeInBytes / (random.nextInt(sizeInBytes - 1) + 1)};
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    for (int allocationSize : channelWriteLimits) {
      PutRequest request =
          new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
              ByteBuffer.wrap(blob), blobSize, blobType);
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      int expectedWriteToCount = ((int) request.sizeInBytes() + allocationSize - 1) / allocationSize;
      int actualWriteToCount = 0;
      while (!request.isSendComplete()) {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(allocationSize));
        request.writeTo(channel);
        ByteBuffer underlyingBuf = channel.getBuffer();
        underlyingBuf.flip();
        outputStream.write(underlyingBuf.array(), underlyingBuf.arrayOffset(), underlyingBuf.remaining());
        actualWriteToCount++;
      }
      Assert.assertEquals("writeTo() should have written out as much as the channel could take in every call",
          expectedWriteToCount, actualWriteToCount);
      DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      requestStream.readLong();
      Assert.assertEquals(RequestOrResponseType.values()[requestStream.readShort()], RequestOrResponseType.PutRequest);
      PutRequest.ReceivedPutRequest deserializedPutRequest = PutRequest.readFrom(requestStream, clusterMap);
      Assert.assertEquals(deserializedPutRequest.getBlobId(), blobId);
      Assert.assertEquals(deserializedPutRequest.getBlobProperties().getBlobSize(), sizeInBlobProperties);
      Assert.assertArrayEquals(userMetadata, deserializedPutRequest.getUsermetadata().array());
      Assert.assertEquals(deserializedPutRequest.getBlobSize(), blobSize);
      Assert.assertEquals(deserializedPutRequest.getBlobType(), blobType);
      byte[] blobRead = new byte[blobSize];
      deserializedPutRequest.getBlobStream().read(blobRead);
      Assert.assertArrayEquals(blob, blobRead);
    }
  }

  private void testPutRequestInvalidVersion(MockClusterMap clusterMap, int correlationId, String clientId,
      BlobId blobId, BlobProperties blobProperties, byte[] userMetadata, byte[] blob) throws IOException {
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    PutRequest request =
        new InvalidVersionPutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            ByteBuffer.wrap(blob), sizeInBlobProperties, BlobType.DataBlob);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    while (!request.isSendComplete()) {
      request.writeTo(writableByteChannel);
    }
    DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong();
    Assert.assertEquals(RequestOrResponseType.values()[requestStream.readShort()], RequestOrResponseType.PutRequest);
    try {
      PutRequest.readFrom(requestStream, clusterMap);
      Assert.fail("Deserialization of PutRequest with invalid version should have thrown an exception.");
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void putRequestResponseTest() throws IOException {
    Random rnd = new Random();
    MockClusterMap clusterMap = new MockClusterMap();

    int correlationId = 5;
    String clientId = "client";
    BlobId blobId = new BlobId(BlobId.DEFAULT_FLAG, ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, clusterMap.getWritablePartitionIds().get(0));
    byte[] userMetadata = new byte[50];
    rnd.nextBytes(userMetadata);
    ByteBuffer.wrap(userMetadata);
    int blobSize = 100;
    byte[] blob = new byte[blobSize];
    rnd.nextBytes(blob);

    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize);

    // Put Request with size in blob properties different from the data size and blob type: Data blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.DataBlob, blob,
        blobSize);

    // Put Request with size in blob properties different from the data size and blob type: Metadata blob.
    blobProperties =
        new BlobProperties(blobSize * 10, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, BlobType.MetadataBlob,
        blob, blobSize);

    // Put Request with empty user metadata.
    byte[] emptyUserMetadata = new byte[0];
    blobProperties = new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);
    testPutRequest(clusterMap, correlationId, clientId, blobId, blobProperties, emptyUserMetadata, BlobType.DataBlob,
        blob, blobSize);

    blobProperties = new BlobProperties(blobSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);
    // Ensure a Put Request with an invalid version does not get deserialized correctly.
    testPutRequestInvalidVersion(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, blob);

    // Response test
    PutResponse response = new PutResponse(1234, clientId, ServerErrorCode.No_Error);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    do {
      response.writeTo(writableByteChannel);
    } while (!response.isSendComplete());
    DataInputStream responseStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    responseStream.readLong();
    PutResponse deserializedPutResponse = PutResponse.readFrom(responseStream);
    Assert.assertEquals(deserializedPutResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedPutResponse.getError(), ServerErrorCode.No_Error);
  }

  @Test
  public void getRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    BlobId id1 = new BlobId(BlobId.DEFAULT_FLAG, ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, clusterMap.getWritablePartitionIds().get(0));
    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>();
    blobIdList.add(id1);
    PartitionRequestInfo partitionRequestInfo1 = new PartitionRequestInfo(new MockPartitionId(), blobIdList);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.add(partitionRequestInfo1);
    GetRequest getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    do {
      getRequest.writeTo(writableByteChannel);
    } while (!getRequest.isSendComplete());
    DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong(); // read length
    requestStream.readShort(); // read short
    GetRequest deserializedGetRequest = GetRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedGetRequest.getClientId(), "clientId");
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().size(), 1);
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().get(0).getBlobIds().size(), 1);
    Assert.assertEquals(deserializedGetRequest.getPartitionInfoList().get(0).getBlobIds().get(0), id1);

    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    long operationTimeMs = SystemTime.getInstance().milliseconds() + random.nextInt();
    MessageInfo messageInfo = new MessageInfo(id1, 1000, 1000, accountId, containerId, operationTimeMs);
    ArrayList<MessageInfo> messageInfoList = new ArrayList<MessageInfo>();
    messageInfoList.add(messageInfo);
    PartitionResponseInfo partitionResponseInfo =
        new PartitionResponseInfo(clusterMap.getWritablePartitionIds().get(0), messageInfoList);
    List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
    partitionResponseInfoList.add(partitionResponseInfo);
    byte[] buf = new byte[1000];
    new Random().nextBytes(buf);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(buf);
    GetResponse response =
        new GetResponse(1234, "clientId", partitionResponseInfoList, byteStream, ServerErrorCode.No_Error);
    outputStream.reset();
    do {
      response.writeTo(writableByteChannel);
    } while (!response.isSendComplete());
    requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong(); // read size
    GetResponse deserializedGetResponse = GetResponse.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedGetResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedGetResponse.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().size(), 1);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().size(), 1);
    MessageInfo msgInfo = deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0);
    Assert.assertEquals(msgInfo.getSize(), 1000);
    Assert.assertEquals(msgInfo.getStoreKey(), id1);
    Assert.assertEquals(msgInfo.getExpirationTimeInMs(), 1000);
    if (GetResponse.getCurrentVersion() == GetResponse.Get_Response_Version_V3) {
      Assert.assertEquals("AccountId mismatch ", accountId, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", containerId, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", operationTimeMs, msgInfo.getOperationTimeMs());
    } else {
      Assert.assertEquals("AccountId mismatch ", UNKNOWN_ACCOUNT_ID, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", UNKNOWN_CONTAINER_ID, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", Utils.Infinite_Time, msgInfo.getOperationTimeMs());
    }
  }

  @Test
  public void deleteRequestResponseTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    BlobId id1 = new BlobId(BlobId.DEFAULT_FLAG, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds().get(0));
    short[] versions = new short[]{DeleteRequest.DELETE_REQUEST_VERSION_1, DeleteRequest.DELETE_REQUEST_VERSION_2};
    for (short version : versions) {
      long deletionTimeMs = Utils.getRandomLong(random, Long.MAX_VALUE);
      int correlationId = random.nextInt();
      DeleteRequest deleteRequest;
      if (version == DeleteRequest.DELETE_REQUEST_VERSION_1) {
        deleteRequest = new DeleteRequest(correlationId, "client", id1);
      } else {
        deleteRequest = new DeleteRequestV2(correlationId, "client", id1, deletionTimeMs);
      }
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
      do {
        deleteRequest.writeTo(writableByteChannel);
      } while (!deleteRequest.isSendComplete());
      DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      requestStream.readLong(); // read length
      requestStream.readShort(); // read short
      DeleteRequest deserializedDeleteRequest = DeleteRequest.readFrom(requestStream, clusterMap);
      Assert.assertEquals(deserializedDeleteRequest.getClientId(), "client");
      Assert.assertEquals(deserializedDeleteRequest.getBlobId(), id1);
      if (version == DeleteRequest.DELETE_REQUEST_VERSION_2) {
        Assert.assertEquals("AccountId mismatch ", id1.getAccountId(), deserializedDeleteRequest.getAccountId());
        Assert.assertEquals("ContainerId mismatch ", id1.getContainerId(), deserializedDeleteRequest.getContainerId());
        Assert.assertEquals("DeletionTime mismatch ", deletionTimeMs, deserializedDeleteRequest.getDeletionTimeInMs());
      } else {
        Assert.assertEquals("AccountId mismatch ", Account.UNKNOWN_ACCOUNT_ID,
            deserializedDeleteRequest.getAccountId());
        Assert.assertEquals("ContainerId mismatch ", Container.UNKNOWN_CONTAINER_ID,
            deserializedDeleteRequest.getContainerId());
        Assert.assertEquals("DeletionTime mismatch ", Utils.Infinite_Time,
            deserializedDeleteRequest.getDeletionTimeInMs());
      }
      DeleteResponse response = new DeleteResponse(correlationId, "client", ServerErrorCode.No_Error);
      outputStream.reset();
      do {
        response.writeTo(writableByteChannel);
      } while (!response.isSendComplete());
      requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      requestStream.readLong(); // read size
      DeleteResponse deserializedDeleteResponse = DeleteResponse.readFrom(requestStream);
      Assert.assertEquals(deserializedDeleteResponse.getCorrelationId(), correlationId);
      Assert.assertEquals(deserializedDeleteResponse.getError(), ServerErrorCode.No_Error);
    }
  }

  @Test
  public void replicaMetadataRequestTest() throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    BlobId id1 = new BlobId(BlobId.DEFAULT_FLAG, ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, clusterMap.getWritablePartitionIds().get(0));
    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<ReplicaMetadataRequestInfo>();
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
        new ReplicaMetadataRequestInfo(new MockPartitionId(), new MockFindToken(0, 1000), "localhost", "path");
    replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
    ReplicaMetadataRequest request = new ReplicaMetadataRequest(1, "id", replicaMetadataRequestInfoList, 1000);
    ByteBuffer buffer = ByteBuffer.allocate((int) request.sizeInBytes());
    ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(buffer);
    do {
      request.writeTo(Channels.newChannel(byteBufferOutputStream));
    } while (!request.isSendComplete());
    buffer.flip();
    buffer.getLong();
    buffer.getShort();
    ReplicaMetadataRequest replicaMetadataRequestFromBytes =
        ReplicaMetadataRequest.readFrom(new DataInputStream(new ByteBufferInputStream(buffer)), new MockClusterMap(),
            new MockFindTokenFactory());
    Assert.assertEquals(replicaMetadataRequestFromBytes.getMaxTotalSizeOfEntriesInBytes(), 1000);
    Assert.assertEquals(replicaMetadataRequestFromBytes.getReplicaMetadataRequestInfoList().size(), 1);

    try {
      request = new ReplicaMetadataRequest(1, "id", null, 12);
      buffer = ByteBuffer.allocate((int) request.sizeInBytes());
      byteBufferOutputStream = new ByteBufferOutputStream(buffer);
      do {
        request.writeTo(Channels.newChannel(byteBufferOutputStream));
      } while (!request.isSendComplete());
      Assert.assertEquals(true, false);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(true, true);
    }
    try {
      replicaMetadataRequestInfo = new ReplicaMetadataRequestInfo(new MockPartitionId(), null, "localhost", "path");
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    long operationTimeMs = SystemTime.getInstance().milliseconds() + random.nextInt();
    MessageInfo messageInfo = new MessageInfo(id1, 1000, accountId, containerId, operationTimeMs);
    List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>();
    messageInfoList.add(messageInfo);
    ReplicaMetadataResponseInfo responseInfo =
        new ReplicaMetadataResponseInfo(clusterMap.getWritablePartitionIds().get(0), new MockFindToken(0, 1000),
            messageInfoList, 1000);
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList = new ArrayList<ReplicaMetadataResponseInfo>();
    replicaMetadataResponseInfoList.add(responseInfo);
    ReplicaMetadataResponse response =
        new ReplicaMetadataResponse(1234, "clientId", ServerErrorCode.No_Error, replicaMetadataResponseInfoList);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    outputStream.reset();
    do {
      response.writeTo(writableByteChannel);
    } while (!response.isSendComplete());
    DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong(); // read size
    ReplicaMetadataResponse deserializedReplicaMetadataResponse =
        ReplicaMetadataResponse.readFrom(requestStream, new MockFindTokenFactory(), clusterMap);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedReplicaMetadataResponse.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals("ReplicaMetadataResponse list size mismatch ", 1,
        deserializedReplicaMetadataResponse.getReplicaMetadataResponseInfoList().size());
    Assert.assertEquals("MsgInfo list size in ReplicaMetadataResponse mismatch ", 1,
        deserializedReplicaMetadataResponse.getReplicaMetadataResponseInfoList().get(0).getMessageInfoList().size());
    MessageInfo msgInfo =
        deserializedReplicaMetadataResponse.getReplicaMetadataResponseInfoList().get(0).getMessageInfoList().get(0);
    Assert.assertEquals("MsgInfo size mismatch ", 1000, msgInfo.getSize());
    Assert.assertEquals("MsgInfo key mismatch ", id1, msgInfo.getStoreKey());
    Assert.assertEquals("MsgInfo expiration value mismatch ", Utils.Infinite_Time, msgInfo.getExpirationTimeInMs());
    if (GetResponse.getCurrentVersion() == GetResponse.Get_Response_Version_V3) {
      Assert.assertEquals("AccountId mismatch ", accountId, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", containerId, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", operationTimeMs, msgInfo.getOperationTimeMs());
    } else {
      Assert.assertEquals("AccountId mismatch ", UNKNOWN_ACCOUNT_ID, msgInfo.getAccountId());
      Assert.assertEquals("ConatinerId mismatch ", UNKNOWN_CONTAINER_ID, msgInfo.getContainerId());
      Assert.assertEquals("OperationTime mismatch ", Utils.Infinite_Time, msgInfo.getOperationTimeMs());
    }
  }

  /**
   * Tests the ser/de of {@link AdminRequest} and {@link AdminResponse} and checks for equality of fields with
   * reference data.
   * @throws IOException
   */
  @Test
  public void adminRequestResponseTest() throws IOException {
    for (AdminRequestOrResponseType type : AdminRequestOrResponseType.values()) {
      MockClusterMap clusterMap = new MockClusterMap();
      PartitionId id = clusterMap.getWritablePartitionIds().get(0);
      AdminRequest adminRequest = new AdminRequest(type, id, 1234, "client");
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
      do {
        adminRequest.writeTo(writableByteChannel);
      } while (!adminRequest.isSendComplete());
      DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      // read length
      requestStream.readLong();
      // read version
      requestStream.readShort();
      AdminRequest deserializedAdminRequest = AdminRequest.readFrom(requestStream, clusterMap);
      Assert.assertEquals(deserializedAdminRequest.getCorrelationId(), 1234);
      Assert.assertEquals(deserializedAdminRequest.getClientId(), "client");
      Assert.assertEquals(deserializedAdminRequest.getType(), type);
      Assert.assertTrue(deserializedAdminRequest.getPartitionId().isEqual(id.toString()));
      AdminResponse response = new AdminResponse(1234, "client", ServerErrorCode.No_Error);
      outputStream.reset();
      do {
        response.writeTo(writableByteChannel);
      } while (!response.isSendComplete());
      requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      requestStream.readLong(); // read size
      AdminResponse deserializedAdminResponse = AdminResponse.readFrom(requestStream);
      Assert.assertEquals(deserializedAdminResponse.getCorrelationId(), 1234);
      Assert.assertEquals(deserializedAdminResponse.getError(), ServerErrorCode.No_Error);
    }
  }

  /**
   * Class representing {@link DeleteRequest} in version {@link DeleteRequest#DELETE_REQUEST_VERSION_2}
   */
  private class DeleteRequestV2 extends DeleteRequest {
    /**
     * Constructs {@link DeleteRequest} in {@link #DELETE_REQUEST_VERSION_2}
     * @param correlationId correlationId of the delete request
     * @param clientId clientId of the delete request
     * @param blobId blobId of the delete request
     * @param deletionTimeInMs deletion time of the blob in ms
     */
    private DeleteRequestV2(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs) {
      super(correlationId, clientId, blobId, deletionTimeInMs, DeleteRequest.DELETE_REQUEST_VERSION_2);
    }
  }
}
