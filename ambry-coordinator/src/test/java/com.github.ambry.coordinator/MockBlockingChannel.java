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
package com.github.ambry.coordinator;

import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.ByteBufferSend;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Crc32;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 */
class MockBlockingChannel extends BlockingChannel {
  private MockDataNode mockDataNode;
  private AtomicBoolean connected;
  private final Object lock;
  private InputStream responseStream;

  public MockBlockingChannel(MockDataNode mockDataNode, String host, int port, int readBufferSize, int writeBufferSize,
      int readTimeoutMs, int connectTimeoutMs) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);

    this.mockDataNode = mockDataNode;
    this.connected = new AtomicBoolean(false);
    this.lock = new Object();
    this.responseStream = null;
  }

  @Override
  public void connect()
      throws SocketException, IOException {
    connected.set(true);
    responseStream = null;
  }

  @Override
  public void disconnect() {
    connected.set(false);
    responseStream = null;
  }

  @Override
  public boolean isConnected() {
    return connected.get();
  }

  @Override
  public void send(Send send)
      throws ClosedChannelException, IOException {
    if (!connected.get()) {
      throw new ClosedChannelException();
    }

    // A barrier to ensure responseStream is set up.
    synchronized (lock) {
      if (responseStream != null) {
        throw new IOException("responseStream is not null. Should be null.");
      }
    }

    RequestOrResponse request = (RequestOrResponse) send;
    RequestOrResponse response = null;

    switch (request.getRequestType()) {
      case PutRequest: {
        PutRequest putRequest = (PutRequest) request;
        BlobId blobId = putRequest.getBlobId();
        BlobProperties blobProperties = putRequest.getBlobProperties();
        ByteBuffer userMetadata = putRequest.getUsermetadata();
        BlobOutput blobOutput = new BlobOutput(putRequest.getBlobSize(), putRequest.getBlobStream());

        ServerErrorCode error = mockDataNode.put(blobId, new Blob(blobProperties, userMetadata, blobOutput));
        response = new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), error);
        break;
      }

      case GetRequest: {
        GetRequest getRequest = (GetRequest) request;
        if (getRequest.getPartitionInfoList().get(0).getBlobIds().size() != 1) {
          throw new IOException(
              "Number of blob ids in get request should be 1: " + getRequest.getPartitionInfoList().get(0).getBlobIds()
                  .size());
        }

        BlobId blobId = (BlobId) getRequest.getPartitionInfoList().get(0).getBlobIds().get(0);

        int byteBufferSize = 0;
        ByteBuffer byteBuffer = null;
        ServerErrorCode getResponseErrorCode = ServerErrorCode.No_Error;

        switch (getRequest.getMessageFormatFlag()) {
          case BlobProperties:
            MockDataNode.BlobPropertiesAndError bpae = mockDataNode.getBlobProperties(blobId);
            getResponseErrorCode = bpae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              BlobProperties blobProperties = bpae.getBlobProperties();
              byteBufferSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(byteBuffer, blobProperties);
            }
            break;

          case BlobUserMetadata:
            MockDataNode.UserMetadataAndError umae = mockDataNode.getUserMetadata(blobId);
            getResponseErrorCode = umae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              ByteBuffer userMetadata = umae.getUserMetadata();
              byteBufferSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              userMetadata.flip();
              MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(byteBuffer, userMetadata);
            }
            break;

          case Blob:
            MockDataNode.BlobOutputAndError boae = mockDataNode.getData(blobId);
            getResponseErrorCode = boae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              BlobOutput blobOutput = boae.getBlobOutput();
              byteBufferSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobOutput.getSize());
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              MessageFormatRecord.Blob_Format_V2
                  .serializePartialBlobRecord(byteBuffer, blobOutput.getSize(), BlobType.DataBlob);

              byte[] blobDataBytes = new byte[(int) blobOutput.getSize()];
              blobOutput.getStream().read(blobDataBytes, 0, (int) blobOutput.getSize());
              byteBuffer.put(blobDataBytes);

              Crc32 crc = new Crc32();
              crc.update(byteBuffer.array(), 0, byteBuffer.position());
              byteBuffer.putLong(crc.getValue());
            }
            break;

          default:
            throw new IOException("GetRequest flag is not supported: " + getRequest.getMessageFormatFlag());
        }

        if (getResponseErrorCode == ServerErrorCode.No_Error) {
          byteBuffer.flip();
          ByteBufferSend responseSend = new ByteBufferSend(byteBuffer);
          List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(1);
          messageInfoList.add(new MessageInfo(blobId, byteBufferSize));
          List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(getRequest.getPartitionInfoList().get(0).getPartition(), messageInfoList);
          partitionResponseInfoList.add(partitionResponseInfo);
          response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), partitionResponseInfoList,
              responseSend, getResponseErrorCode);
        } else {
          List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(getRequest.getPartitionInfoList().get(0).getPartition(), getResponseErrorCode);
          partitionResponseInfoList.add(partitionResponseInfo);
          ByteBufferSend responseSend = new ByteBufferSend(ByteBuffer.allocate(0));
          response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), partitionResponseInfoList,
              responseSend, ServerErrorCode.No_Error);
        }

        break;
      }

      case DeleteRequest: {
        DeleteRequest deleteRequest = (DeleteRequest) request;
        BlobId blobId = deleteRequest.getBlobId();

        ServerErrorCode error = mockDataNode.delete(blobId);
        response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error);
        break;
      }

      default: {
        throw new IOException("Request type is not supported: " + request.getRequestType());
        // break;
      }
    }

    if (response == null) {
      throw new IOException("response is null. Should not be null.");
    }

    synchronized (lock) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
      response.writeTo(writableByteChannel);
      responseStream = new ByteArrayInputStream(outputStream.toByteArray());
    }
  }

  @Override
  public ChannelOutput receive()
      throws ClosedChannelException, IOException {
    if (!connected.get()) {
      throw new ClosedChannelException();
    }
    // A barrier to ensure responseStream is set up.
    synchronized (lock) {
      if (responseStream == null) {
        throw new IOException("responseStream is null. Should not be null.");
      }
    }

    // consume the size header and return the remaining response.
    ByteBuffer streamSizeBuffer = ByteBuffer.allocate(8);
    while (streamSizeBuffer.position() < streamSizeBuffer.capacity()) {
      streamSizeBuffer.put((byte) responseStream.read());
    }
    streamSizeBuffer.flip();
    return new ChannelOutput(responseStream, streamSizeBuffer.getLong() - 8);
  }
}