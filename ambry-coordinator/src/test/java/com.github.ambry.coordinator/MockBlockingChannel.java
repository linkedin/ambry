package com.github.ambry.coordinator;

import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.Send;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.ChannelOutput;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PartitionResponseInfo;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.ServerErrorCode;
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
      int readTimeoutMs) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs);

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
        BlobOutput blobOutput = new BlobOutput(putRequest.getDataSize(), putRequest.getData());

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
            MockDataNode.BlobOutputAndError bdae = mockDataNode.getData(blobId);
            getResponseErrorCode = bdae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              BlobOutput blobData = bdae.getBlobOutput();
              byteBufferSize = (int) MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobData.getSize());
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(byteBuffer, blobData.getSize());

              byte[] blobDataBytes = new byte[(int) blobData.getSize()];
              blobData.getStream().read(blobDataBytes, 0, (int) blobData.getSize());
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
      streamSizeBuffer.put((byte)responseStream.read());
    }
    streamSizeBuffer.flip();
    return new ChannelOutput(responseStream, streamSizeBuffer.getLong() - 8);
  }
}