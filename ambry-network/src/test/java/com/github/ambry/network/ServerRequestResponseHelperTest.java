/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.network;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.ReplicateBlobRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.Response;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


public class ServerRequestResponseHelperTest {

  MockClusterMap clusterMap;
  private final ServerRequestResponseHelper requestResponseHelper;
  String clientId = "client";
  int correlationId = TestUtils.RANDOM.nextInt();
  BlobId blobId;

  public ServerRequestResponseHelperTest() throws IOException {
    clusterMap = new MockClusterMap();
    requestResponseHelper = new ServerRequestResponseHelper(clusterMap, null);
    blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, (short) 1, (short) 1,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
  }

  @Test
  public void putRequestResponseTest() throws IOException {
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
    PutRequest putRequest =
        new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
            Unpooled.wrappedBuffer(blob), blobSize, BlobType.DataBlob, null);
    DataInputStream requestStream = serAndPrepForRead(putRequest);
    MockRequest mockRequest = new MockRequest(requestStream);
    // 1. Verify request is decoded correctly
    RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
    assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.PutRequest);
    // 2. Verify response is formed correctly
    Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
    assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.PutResponse);
    assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", response.getClientId(), clientId);
    assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
    request.release();
    response.release();
  }

  @Test
  public void getRequestResponseTest() throws IOException {
    PartitionRequestInfo partitionRequestInfo =
        new PartitionRequestInfo(new MockPartitionId(), Collections.singletonList(blobId));
    GetRequest getRequest = new GetRequest(correlationId, clientId, MessageFormatFlags.Blob,
        Collections.singletonList(partitionRequestInfo), GetOption.None);
    DataInputStream requestStream = serAndPrepForRead(getRequest);
    MockRequest mockRequest = new MockRequest(requestStream);
    // 1. Verify request is decoded correctly
    RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
    assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.GetRequest);
    // 2. Verify response is formed correctly
    Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
    assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.GetResponse);
    assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", response.getClientId(), clientId);
    assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
    request.release();
    response.release();
  }

  @Test
  public void ttlUpdateRequestResponseTest() throws IOException {
    TtlUpdateRequest ttlUpdateRequest =
        new TtlUpdateRequest(correlationId, clientId, blobId, System.currentTimeMillis() + 1000,
            System.currentTimeMillis(), (short) 1);
    DataInputStream requestStream = serAndPrepForRead(ttlUpdateRequest);
    MockRequest mockRequest = new MockRequest(requestStream);
    // 1. Verify request is decoded correctly
    RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
    assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.TtlUpdateRequest);
    // 2. Verify response is formed correctly
    Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
    assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.TtlUpdateResponse);
    assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", response.getClientId(), clientId);
    assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
    request.release();
    response.release();
  }

  @Test
  public void deleteRequestResponseTest() throws IOException {
    DeleteRequest deleteRequest = new DeleteRequest(correlationId, clientId, blobId, System.currentTimeMillis());
    DataInputStream requestStream = serAndPrepForRead(deleteRequest);
    MockRequest mockRequest = new MockRequest(requestStream);
    // 1. Verify request is decoded correctly
    RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
    assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.DeleteRequest);
    // 2. Verify response is formed correctly
    Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
    assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.DeleteResponse);
    assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", response.getClientId(), clientId);
    assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
    request.release();
    response.release();
  }

  /**
   * Test for undelete request and response.
   * @throws IOException
   */
  @Test
  public void undeleteRequestResponseTest() throws IOException {
    UndeleteRequest undeleteRequest = new UndeleteRequest(correlationId, clientId, blobId, System.currentTimeMillis());
    DataInputStream requestStream = serAndPrepForRead(undeleteRequest);
    MockRequest mockRequest = new MockRequest(requestStream);
    // 1. Verify request is decoded correctly
    RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
    assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.UndeleteRequest);
    // 2. Verify response is formed correctly
    Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
    assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.UndeleteResponse);
    assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", response.getClientId(), clientId);
    assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
    request.release();
    response.release();
  }

  @Test
  public void adminRequestResponseTest() throws IOException {
    for (AdminRequestOrResponseType type : AdminRequestOrResponseType.values()) {
      PartitionId id = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
      // with a valid partition id
      AdminRequest adminRequest = new AdminRequest(type, id, correlationId, clientId);
      DataInputStream requestStream = serAndPrepForRead(adminRequest);
      MockRequest mockRequest = new MockRequest(requestStream);
      // 1. Verify request is decoded correctly
      RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
      assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.AdminRequest);
      // 2. Verify response is formed correctly
      Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
      assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.AdminResponse);
      assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
      assertEquals("Mismatch in response client id", response.getClientId(), clientId);
      assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
      request.release();
      response.release();
    }
  }

  @Test
  public void replicateBlobRequestResponseTest() throws IOException {
    RequestOrResponseType operationType =
        RequestOrResponseType.values()[Utils.getRandomShort(TestUtils.RANDOM) % RequestOrResponseType.values().length];
    short[] versions = new short[]{ReplicateBlobRequest.VERSION_1, ReplicateBlobRequest.VERSION_2};
    for (short version : versions) {
      final ReplicateBlobRequest replicateBlobRequest;
      if (version == ReplicateBlobRequest.VERSION_1) {
        replicateBlobRequest = new ReplicateBlobRequest(correlationId, clientId, blobId, "datacenter1_host1", 15058);
      } else {
        replicateBlobRequest =
            new ReplicateBlobRequest(correlationId, clientId, blobId, "datacenter1_host1", 15058, operationType,
                System.currentTimeMillis(), (short) 1, 1L);
      }
      DataInputStream requestStream = serAndPrepForRead(replicateBlobRequest);
      MockRequest mockRequest = new MockRequest(requestStream);
      // 1. Verify request is decoded correctly
      RequestOrResponse request = requestResponseHelper.getDecodedRequest(mockRequest);
      assertEquals("Mismatch in request type", request.getRequestType(), RequestOrResponseType.ReplicateBlobRequest);
      // 2. Verify response is formed correctly
      Response response = requestResponseHelper.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
      assertEquals("Mismatch in response type", response.getRequestType(), RequestOrResponseType.ReplicateBlobResponse);
      assertEquals("Mismatch in response correlation id", response.getCorrelationId(), correlationId);
      assertEquals("Mismatch in response client id", response.getClientId(), clientId);
      assertEquals("Mismatch in error code", response.getError(), ServerErrorCode.Retry_After_Backoff);
      request.release();
      response.release();
    }
  }

//  @Test
//  public void doFileCopyChunkDataResponseTest() throws IOException {
//    short requestVersionToUse = 1;
//    String str = "Hello, Netty ByteBuf!";
//    ByteBuf byteBuf = Unpooled.copiedBuffer(str, StandardCharsets.UTF_8);
//    FileCopyGetChunkResponse fileCopyGetChunkResponse = new FileCopyGetChunkResponse(requestVersionToUse, 111,
//        "id1",ServerErrorCode.No_Error,  new MockPartitionId(), "file1", null,
//        byteBuf, 1000, 22, false);
//
//    DataInputStream requestStream = serAndPrepForRead(fileCopyGetChunkResponse);
//
//    FileCopyGetChunkResponse fileCopyGetChunkResponseOnNetwork = FileCopyGetChunkResponse.readFrom(requestStream, new MockClusterMap());
//    String chunkData = Utils.readIntString(fileCopyGetChunkResponseOnNetwork.getChunkDataOnReceiverNode());
//
//    Assert.assertEquals(chunkData, str);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getCorrelationId(), 111);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getFileName(), "file1");
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getChunkSizeInBytes(), 22);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getStartOffset(), 1000);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getError(), ServerErrorCode.No_Error);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getPartitionId().getId(), 0);
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getPartitionId().toPathString(), "0");
//    Assert.assertEquals(fileCopyGetChunkResponseOnNetwork.getVersionId(), requestVersionToUse);
//    fileCopyGetChunkResponse.release();
//  }

  @Test
  public void doFileCopyChunkDataResponseTest2() throws IOException {
    File file = new File("/tmp/0/0_index");
    DataInputStream chunkStreamWithSender = new DataInputStream(Files.newInputStream(file.toPath()));

    FileCopyGetChunkResponse response = new FileCopyGetChunkResponse(
        FileCopyGetChunkResponse.File_Copy_Chunk_Response_Version_V1,
        0, "", ServerErrorCode.No_Error, new MockPartitionId(),
        file.getName(), chunkStreamWithSender, 0, chunkStreamWithSender.available(), false);

    DataInputStream chunkResponseStream = serAndPrepForRead(response);
    FileCopyGetChunkResponse fileCopyGetChunkResponseWithReciever = FileCopyGetChunkResponse.readFrom(chunkResponseStream, new MockClusterMap());

    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getCorrelationId(), 0);
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getFileName(), file.getName());
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getStartOffset(), 0);
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getPartitionId().getId(), 0);
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getPartitionId().toPathString(), "0");
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getVersionId(),
        FileCopyGetChunkResponse.File_Copy_Chunk_Response_Version_V1);
    Assert.assertEquals(fileCopyGetChunkResponseWithReciever.getChunkStream().available(), file.length());

//    DataInputStream chunkStreamWithReceiver = fileCopyGetChunkResponseWithReciever.getChunkStream();
//    byte[] buffer = new byte[chunkStreamWithReceiver.available()];
//    chunkStreamWithReceiver.readFully(buffer);
//
//    System.out.println("Dw: " + new String(buffer));
  }

  private DataInputStream serAndPrepForRead(RequestOrResponse requestOrResponse) throws IOException {
    DataInputStream stream;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    do {
      ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) requestOrResponse.sizeInBytes()));
      requestOrResponse.writeTo(channel);
      ByteBuffer underlyingBuf = channel.getBuffer();
      underlyingBuf.flip();
      outputStream.write(underlyingBuf.array(), underlyingBuf.arrayOffset(), underlyingBuf.remaining());
    } while (!requestOrResponse.isSendComplete());
    stream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    // read length
    stream.readLong();
    return stream;
  }

  /**
   * Implementation of {@link NetworkRequest} to help with tests.
   */
  private static class MockRequest implements NetworkRequest {

    private final InputStream stream;

    public MockRequest(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getInputStream() {
      return stream;
    }

    @Override
    public long getStartTimeInMs() {
      return 0;
    }
  }
}