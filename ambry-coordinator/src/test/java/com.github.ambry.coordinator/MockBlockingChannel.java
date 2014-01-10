package com.github.ambry.coordinator;

import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.network.Send;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.shared.TTLRequest;
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
public class MockBlockingChannel extends BlockingChannel {
  private MockDataNode mockDataNode;
  private AtomicBoolean connected;
  private Object lock;
  private InputStream responseStream;

  public MockBlockingChannel(MockDataNode mockDataNode, String host, int port, int readBufferSize,
                             int writeBufferSize, int readTimeoutMs) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs);

    this.mockDataNode = mockDataNode;
    this.connected = new AtomicBoolean(false);
    this.lock = new Object();
    this.responseStream = null;
  }

  @Override
  public void connect() throws SocketException, IOException {
    connected.set(true);
    responseStream = null;
    return;
  }

  @Override
  public void disconnect() {
    connected.set(false);
    responseStream = null;
    return;
  }

  @Override
  public boolean isConnected() {
    return connected.get();
  }

  @Override
  public void send(Send send) throws ClosedChannelException, IOException {
    if (!connected.get()) {
      throw new ClosedChannelException();
    }

    // A barrier to ensure responseStream is set up.
    synchronized (lock) {
      if (responseStream != null) {
        throw new IOException("responseStream is not null. Should be null.");
      }
    }

    RequestOrResponse request = (RequestOrResponse)send;
    RequestOrResponse response = null;

    switch (request.getRequestType()) {
      case PutRequest: {
        PutRequest putRequest = (PutRequest)request;
        BlobId blobId = putRequest.getBlobId();
        BlobProperties blobProperties = putRequest.getBlobProperties();
        ByteBuffer userMetadata = putRequest.getUsermetadata();
        BlobOutput blobOutput = new BlobOutput(putRequest.getDataSize(), putRequest.getData());

        ServerErrorCode error = mockDataNode.put(blobId, new Blob(blobProperties, userMetadata, blobOutput));
        response = new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), error);
        break;
      }

      case GetRequest: {
        GetRequest getRequest = (GetRequest)request;
        if (getRequest.getBlobIds().size() != 1) {
          throw new IOException("Number of blob ids in get request should be 1: " + getRequest.getBlobIds().size());
        }

        BlobId blobId = (BlobId)getRequest.getBlobIds().get(0);

        int byteBufferSize = 0;
        ByteBuffer byteBuffer = null;
        ServerErrorCode getResponseErrorCode = ServerErrorCode.No_Error;

        switch (getRequest.getMessageFormatFlag()) {
          case BlobProperties:
            MockDataNode.BlobPropertiesAndError bpae = mockDataNode.getBlobProperties(blobId);
            getResponseErrorCode = bpae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              BlobProperties blobProperties = bpae.getBlobProperties();
              byteBufferSize = MessageFormat.getCurrentVersionBlobPropertyRecordSize(blobProperties);
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              MessageFormat.serializeCurrentVersionBlobPropertyRecord(byteBuffer, blobProperties);
            }
            break;

          case BlobUserMetadata:
            MockDataNode.UserMetadataAndError umae = mockDataNode.getUserMetadata(blobId);
            getResponseErrorCode = umae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              ByteBuffer userMetadata = umae.getUserMetadata();
              byteBufferSize = MessageFormat.getCurrentVersionUserMetadataSize(userMetadata);
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              userMetadata.flip();
              MessageFormat.serializeCurrentVersionUserMetadata(byteBuffer, userMetadata);
            }
            break;

          case Blob:
            MockDataNode.BlobOutputAndError bdae = mockDataNode.getData(blobId);
            getResponseErrorCode = bdae.getError();
            if (getResponseErrorCode == ServerErrorCode.No_Error) {
              BlobOutput blobData = bdae.getBlobOutput();
              byteBufferSize = (int)MessageFormat.getCurrentVersionDataSize(blobData.getSize());
              byteBuffer = ByteBuffer.allocate(byteBufferSize);
              MessageFormat.serializeCurrentVersionPartialData(byteBuffer, blobData.getSize());

              byte[] blobDataBytes = new byte[(int)blobData.getSize()];
              blobData.getStream().read(blobDataBytes, 0, (int)blobData.getSize());
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
          response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), messageInfoList,
                                     responseSend, getResponseErrorCode);
        } else {
          response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), getResponseErrorCode);
        }

        break;
      }

      case DeleteRequest: {
        DeleteRequest deleteRequest = (DeleteRequest)request;
        BlobId blobId = deleteRequest.getBlobId();

        ServerErrorCode error = mockDataNode.delete(blobId);
        response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error);
        break;
      }

      case TTLRequest: {
        TTLRequest ttlRequest = (TTLRequest)request;
        throw new IOException("TTLRequest is not yet mocked: " + request.getRequestType());
        // break;
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
  public InputStream receive() throws ClosedChannelException, IOException {
    if (!connected.get()) {
      throw new ClosedChannelException();
    }
    // A barrier to ensure responseStream is set up.
    synchronized (lock) {
      if (responseStream == null) {
        throw new IOException("responseStream is null. Should not be null.");
      }
    }

    // get the size and return the remaining response. Need to be done by network receive?
    long toRead = 8;
    long read = 0;
    while (read < toRead) {
      responseStream.read();
      read++;
    }
    return responseStream;
  }
}