/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A class that mocks the server (data node) and provides methods for sending requests and setting error states.
 */
class MockServer {
  private ServerErrorCode hardError = null;
  private LinkedList<ServerErrorCode> putErrors = new LinkedList<ServerErrorCode>();
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
    RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
    switch (type) {
      case PutRequest:
        PutRequest putRequest = (PutRequest) send;
        final ServerErrorCode putError =
            hardError != null ? hardError : putErrors.size() > 0 ? putErrors.poll() : ServerErrorCode.No_Error;
        if (putError == ServerErrorCode.No_Error) {
          updateBlobMap(putRequest);
        }
        return new MockBoundedByteBufferReceive(
            new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), putError) {
              ByteBuffer getPayload()
                  throws IOException {
                ByteArrayOutputStream bStream = new ByteArrayOutputStream();
                DataOutputStream dStream = new DataOutputStream(bStream);
                dStream.writeShort((short) RequestOrResponseType.PutResponse.ordinal());
                dStream.writeShort(versionId);
                dStream.writeInt(correlationId);
                dStream.writeInt(0); // avoiding adding clientId
                dStream.writeShort((short) putError.ordinal());
                return ByteBuffer.wrap(bStream.toByteArray());
              }
            }.getPayload());
      default:
        throw new IOException("Unknown request type received");
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
   * Set the error for each request from this point onwards that affects subsequent PutRequests sent to this node
   * (until/unless the next set or reset error method is invoked).
   * Each request from the list is used exactly once and in order. So, if the list contains {No_Error, Unknown_Error,
   * Disk_Error}, then the first, second and third requests would receive No_Error,
   * Unknown_Error and Disk_Error respectively. Once the errors are exhausted, the default No_Error is assumed for
   * all further requests until the next call to this method.
   * @param putErrors the list of errors that affects subsequent PutRequests.
   */
  public void setPutErrors(List<ServerErrorCode> putErrors) {
    this.putErrors.clear();
    this.putErrors.addAll(putErrors);
  }

  /**
   * Set the error to be set in the responses for all requests from this point onwards (until/unless another set or
   * reset method for errors is invoked).
   * @param putError the error to set from this point onwards.
   */
  public void setPutErrorForAllRequests(ServerErrorCode putError) {
    this.hardError = putError;
  }

  /**
   * Clear the error for subsequent PutRequests. That is all responses from this point onwards will be successful
   * ({@link ServerErrorCode#No_Error}) until/unless another set error method is invoked.
   */
  public void resetPutErrors() {
    this.putErrors.clear();
    this.hardError = null;
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

