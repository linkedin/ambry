package com.github.ambry.router;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A class that mocks the server (data node) and provides methods for sending requests and setting error states.
 */
class MockServer {
  private ServerErrorCode putError = ServerErrorCode.No_Error;
  private final AtomicBoolean blockResponses = new AtomicBoolean(false);
  private final Object block = new Object();
  private final Map<String, ByteBuffer> blobs = new ConcurrentHashMap<String, ByteBuffer>();

  /**
   * Take in a request in the form of {@link Send} and return a response in the form of a
   * {@link MockBoundedByteBufferReceive}.
   * @param send the request.
   * @return the response.
   * @throws IOException if there was an error in interpreting the request.
   */
  public MockBoundedByteBufferReceive send(Send send)
      throws IOException {
    try {
      RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
      switch (type) {
        case PutRequest:
          PutRequest putRequest = (PutRequest) send;
          updateBlobMap(putRequest);
          return new MockBoundedByteBufferReceive(
              new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), putError) {
                ByteBuffer getPayload() {
                  ByteBuffer buf;
                  buf = ByteBuffer.allocate(2 + 2 + 4 + 4 + 2);
                  buf.putShort((short) RequestOrResponseType.PutResponse.ordinal());
                  buf.putShort(versionId);
                  buf.putInt(correlationId);
                  buf.putInt(0);
                  buf.putShort((short) putError.ordinal());
                  buf.flip();
                  // ignore version for now
                  return buf;
                }
              }.getPayload());
        default:
          throw new IOException("Unknown request type received");
      }
    } finally {
      if (blockResponses.get()) {
        try {
          synchronized (block) {
            block.wait();
          }
        } catch (InterruptedException e) {
          throw new IllegalStateException("wait() should not have been interrupted");
        }
      }
    }
  }

  /**
   * Serialize contents of the PutRequest and update the blob map with the serialized content.
   * @param putRequest the PutRequest
   * @throws IOException if there was an error reading the contents of the given PutRequest.
   */
  private void updateBlobMap(PutRequest putRequest)
      throws IOException {
    String id = putRequest.getBlobId().getID();
    ByteBuffer buf = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(buf);
    putRequest.writeTo(bufChannel);
    buf.flip();
    blobs.put(id, buf);
  }

  /**
   * @return the blobs that were put on this server.
   */
  Map<String, ByteBuffer> getBlobs() {
    return blobs;
  }

  /**
   * Set the error that affects subsequent PutRequests sent to this node.
   * @param putError the error that affects subsequent PutRequests.
   */
  public void setPutError(ServerErrorCode putError) {
    this.putError = putError;
  }

  /**
   * Clear the error for subsequent PutRequests.
   */
  public void resetPutError() {
    this.putError = ServerErrorCode.No_Error;
  }

  /**
   * Set state to make every send block until notified.
   */
  public void setBlockUntilNotifiedState() {
    blockResponses.set(true);
  }

  /**
   * Clear the set-until-notified state.
   */
  public void resetBlockUntilNotifiedState() {
    blockResponses.set(false);
    synchronized (block) {
      block.notifyAll();
    }
  }

  /**
   * Unblock a send that is currently blocked.
   */
  public void unBlockOne() {
    synchronized (block) {
      block.notify();
    }
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

