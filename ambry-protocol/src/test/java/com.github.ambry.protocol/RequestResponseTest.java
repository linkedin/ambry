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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobPropertiesSerDe;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


class MockFindTokenFactory implements FindTokenFactory {

  @Override
  public FindToken getFindToken(DataInputStream stream)
      throws IOException {
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

  public MockFindToken(DataInputStream stream)
      throws IOException {
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

class MockPutRequestV1 extends PutRequest {
  public MockPutRequestV1(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream blobStream, long blobSize, BlobType blobType, short versionId) {
    super(correlationId, clientId, blobId, properties, usermetadata, blobStream, blobSize, blobType, versionId);
  }

  protected int sizeExcludingBlobSize() {
    // size of (header + blobId + blob properties + metadata size + metadata + blob size + blob type)
    return super.sizeExcludingBlobSize() - Blob_Size_InBytes - BlobType_Size_InBytes;
  }

  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long totalWritten = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(sizeExcludingBlobSize());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      BlobPropertiesSerDe.putBlobPropertiesToBuffer(bufferToSend, properties);
      bufferToSend.putInt(usermetadata.capacity());
      bufferToSend.put(usermetadata);
      bufferToSend.flip();
    }
    while (sentBytes < sizeInBytes()) {
      if (bufferToSend.remaining() > 0) {
        int toWrite = bufferToSend.remaining();
        int written = channel.write(bufferToSend);
        totalWritten += written;
        sentBytes += written;
        if (toWrite != written || sentBytes == sizeInBytes()) {
          break;
        }
      }
      logger.trace("sent Bytes from Put Request {}", sentBytes);
      bufferToSend.clear();
      int streamReadCount = blobStream
          .read(bufferToSend.array(), 0, (int) Math.min(bufferToSend.capacity(), (sizeInBytes() - sentBytes)));
      bufferToSend.limit(streamReadCount);
    }
    return totalWritten;
  }
}

public class RequestResponseTest {
  private void testPutRequest(MockClusterMap clusterMap, int correlationId, String clientId, BlobId blobId,
      BlobProperties blobProperties, byte[] userMetadata, BlobType blobType, byte[] blob, int blobSize)
      throws IOException {
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    PutRequest request = new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
        new ByteBufferInputStream(ByteBuffer.wrap(blob)), blobSize, blobType);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    while (!request.isSendComplete()) {
      request.writeTo(writableByteChannel);
    }
    DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong();
    Assert.assertEquals(RequestOrResponseType.values()[requestStream.readShort()], RequestOrResponseType.PutRequest);
    PutRequest deserializedPutRequest = PutRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedPutRequest.getBlobId(), blobId);
    Assert.assertEquals(deserializedPutRequest.getBlobProperties().getBlobSize(), sizeInBlobProperties);
    Assert.assertArrayEquals(userMetadata, deserializedPutRequest.getUsermetadata().array());
    Assert.assertEquals(deserializedPutRequest.getBlobSize(), blobSize);
    Assert.assertEquals(deserializedPutRequest.getBlobType(), blobType);
    byte[] blobRead = new byte[blobSize];
    deserializedPutRequest.getBlobStream().read(blobRead);
    Assert.assertArrayEquals(blob, blobRead);
  }

  private void testPutRequestV1(MockClusterMap clusterMap, int correlationId, String clientId, BlobId blobId,
      BlobProperties blobProperties, byte[] userMetadata, byte[] blob)
      throws IOException {
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    PutRequest request =
        new MockPutRequestV1(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(blob)), sizeInBlobProperties, BlobType.DataBlob,
            PutRequest.Put_Request_Version_V1);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    while (!request.isSendComplete()) {
      request.writeTo(writableByteChannel);
    }
    DataInputStream requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong();
    Assert.assertEquals(RequestOrResponseType.values()[requestStream.readShort()], RequestOrResponseType.PutRequest);
    PutRequest deserializedPutRequest = PutRequest.readFrom(requestStream, clusterMap);
    Assert.assertEquals(deserializedPutRequest.getBlobId(), blobId);
    Assert.assertEquals(deserializedPutRequest.getBlobProperties().getBlobSize(), sizeInBlobProperties);
    Assert.assertArrayEquals(userMetadata, deserializedPutRequest.getUsermetadata().array());
    Assert.assertEquals(deserializedPutRequest.getBlobSize(), sizeInBlobProperties);
    Assert.assertEquals(deserializedPutRequest.getBlobType(), BlobType.DataBlob);
    byte[] blobRead = new byte[sizeInBlobProperties];
    deserializedPutRequest.getBlobStream().read(blobRead);
    Assert.assertArrayEquals(blob, blobRead);
  }

  private void testPutRequestInvalidVersion(MockClusterMap clusterMap, int correlationId, String clientId,
      BlobId blobId, BlobProperties blobProperties, byte[] userMetadata, byte[] blob)
      throws IOException {
    final short Put_Request_Invalid_version = 0;
    int sizeInBlobProperties = (int) blobProperties.getBlobSize();
    PutRequest request =
        new MockPutRequestV1(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(blob)), sizeInBlobProperties, BlobType.DataBlob,
            Put_Request_Invalid_version);
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
  public void putRequestResponseTest()
      throws IOException {
    Random rnd = new Random();
    MockClusterMap clusterMap = new MockClusterMap();

    int correlationId = 5;
    String clientId = "client";
    BlobId blobId = new BlobId(clusterMap.getWritablePartitionIds().get(0));
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
    // Ensure Put Request V1 still deserializes correctly.
    testPutRequestV1(clusterMap, correlationId, clientId, blobId, blobProperties, userMetadata, blob);
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
  public void getRequestResponseTest()
      throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    BlobId id1 = new BlobId(clusterMap.getWritablePartitionIds().get(0));
    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>();
    blobIdList.add(id1);
    PartitionRequestInfo partitionRequestInfo1 = new PartitionRequestInfo(new MockPartitionId(), blobIdList);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.add(partitionRequestInfo1);
    GetRequest getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
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

    MessageInfo messageInfo = new MessageInfo(id1, 1000, 1000);
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
    Assert.assertEquals(
        deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getSize(), 1000);
    Assert.assertEquals(
        deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getStoreKey(), id1);
    Assert.assertEquals(deserializedGetResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0)
        .getExpirationTimeInMs(), 1000);
  }

  @Test
  public void deleteRequestResponseTest()
      throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    BlobId id1 = new BlobId(clusterMap.getWritablePartitionIds().get(0));
    DeleteRequest deleteRequest = new DeleteRequest(1234, "client", id1);
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
    DeleteResponse response = new DeleteResponse(1234, "client", ServerErrorCode.No_Error);
    outputStream.reset();
    do {
      response.writeTo(writableByteChannel);
    } while (!response.isSendComplete());
    requestStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    requestStream.readLong(); // read size
    DeleteResponse deserializedDeleteResponse = DeleteResponse.readFrom(requestStream);
    Assert.assertEquals(deserializedDeleteResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedDeleteResponse.getError(), ServerErrorCode.No_Error);
  }

  @Test
  public void replicaMetadataRequestTest()
      throws IOException {
    MockClusterMap clusterMap = new MockClusterMap();
    BlobId id1 = new BlobId(clusterMap.getWritablePartitionIds().get(0));
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
    ReplicaMetadataRequest replicaMetadataRequestFromBytes = ReplicaMetadataRequest
        .readFrom(new DataInputStream(new ByteBufferInputStream(buffer)), new MockClusterMap(),
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
    MessageInfo messageInfo = new MessageInfo(id1, 1000);
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
    ReplicaMetadataResponse deserializedDeleteResponse =
        ReplicaMetadataResponse.readFrom(requestStream, new MockFindTokenFactory(), clusterMap);
    Assert.assertEquals(deserializedDeleteResponse.getCorrelationId(), 1234);
    Assert.assertEquals(deserializedDeleteResponse.getError(), ServerErrorCode.No_Error);
  }
}
