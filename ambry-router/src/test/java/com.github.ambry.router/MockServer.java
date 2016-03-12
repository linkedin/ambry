package com.github.ambry.router;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.github.ambry.utils.Time;
import java.util.HashMap;


class MockServer {
  private ServerErrorCode putError = ServerErrorCode.No_Error;
  private final HashMap<String, ByteBuffer> blobs = new HashMap<String, ByteBuffer>();
  private final HashMap<String, ServerErrorCode> blobIdToServerErrorCode = new HashMap<String, ServerErrorCode>();
  private final Time time;

  private boolean noResponse;

  MockServer(Time time, boolean serverNoResponse) {
    this.time = time;
    this.noResponse = serverNoResponse;
  }

  synchronized BoundedByteBufferReceive send(Send send)
      throws IOException {
    RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
    switch (type) {
      case PutRequest:
        PutRequest putRequest = (PutRequest) send;
        updateBlobMap(putRequest);
        return new MockBoundedByteBufferReceive(
            new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), ServerErrorCode.No_Error) {
              ByteBuffer getPayload() {
                ByteBuffer buf;
                if (getError() == ServerErrorCode.No_Error) {
                  buf = ByteBuffer.allocate(2 + 2 + 4 + 4 + 2);
                  buf.putShort((short) RequestOrResponseType.PutResponse.ordinal());
                  buf.putShort(versionId);
                  buf.putInt(correlationId);
                  buf.putInt(0);
                  buf.putShort((short) ServerErrorCode.No_Error.ordinal());
                  buf.flip();
                } else {
                  buf = null; //@todo should this be set at all?
                }
                // ignore version for now
                return buf;
              }
            }.getPayload());


      case DeleteRequest:
        if(noResponse) {
          return null;
        }
        final DeleteRequest deleteRequest = (DeleteRequest) send;
        final String blobIdString = deleteRequest.getBlobId().getID();
        final ServerErrorCode error = getErrorFromBlobIdStr(blobIdString);
        return new MockBoundedByteBufferReceive(
            new PutResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error) {
              ByteBuffer getPayload() {
                ByteBuffer buf;
                buf = ByteBuffer.allocate(2 + 2 + 4 + 4 + 2);
                buf.putShort((short) RequestOrResponseType.DeleteResponse.ordinal());
                buf.putShort(versionId);
                buf.putInt(correlationId);
                buf.putInt(0);
                buf.putShort((short) error.ordinal());
                buf.flip();
                return buf;
              }
            }.getPayload());
      default:
        throw new IOException("Unknown request type received");
    }
  }

  private void updateBlobMap(PutRequest putRequest)
      throws IOException {
    String id = putRequest.getBlobId().getID();
    ByteBuffer buf = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(buf);
    putRequest.writeTo(bufChannel);
    buf.flip();
    blobs.put(id, buf);
  }

  HashMap<String, ByteBuffer> getBlobs() {
    return blobs;
  }

  public void setPutError(ServerErrorCode putError) {
    this.putError = putError;
  }

  public void clearPutError() {
    this.putError = ServerErrorCode.No_Error;
  }

  public void setBlobIdToServerErrorCode (String blobIdString, ServerErrorCode code) {
    blobIdToServerErrorCode.put(blobIdString, code);
  }

  public ServerErrorCode getErrorFromBlobIdStr(String blobIdString) {
    return blobIdToServerErrorCode.containsKey(blobIdString)? blobIdToServerErrorCode.get(blobIdString) : ServerErrorCode.Blob_Not_Found;
  }
}

/**
 * A mock implementation of {@link BoundedByteBufferReceive} that constructs a buffer with the passed in correlation
 * id and returns that buffer as part of {@link #getPayload()}.
 */
class MockBoundedByteBufferReceive extends BoundedByteBufferReceive {
  private final ByteBuffer buf;

  /**
   * Construct a MockBoundedByteBufferReceive with the given correlation id.
   * @param buf the ByteBuffer that is the payload of this object.
   */
  public MockBoundedByteBufferReceive(ByteBuffer buf) {
    this.buf = buf;
  }

  /**
   * Return the buffer associated with this object.
   * @return the buffer associated with this object.
   */
  @Override
  public ByteBuffer getPayload() {
    return buf;
  }
}

