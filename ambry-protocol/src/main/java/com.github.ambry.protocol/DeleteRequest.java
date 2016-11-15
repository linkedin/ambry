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
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Delete request to delete blob
 */
public class DeleteRequest extends RequestOrResponse {
  private BlobId blobId;
  private int sizeSent;
  private static final short Delete_Request_Version_V1 = 1;

  public DeleteRequest(int correlationId, String clientId, BlobId blobId) {
    super(RequestOrResponseType.DeleteRequest, Delete_Request_Version_V1, correlationId, clientId);
    this.blobId = blobId;
    sizeSent = 0;
  }

  public static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.DeleteRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    BlobId blobId = new BlobId(stream, map);
    // ignore version for now
    return new DeleteRequest(correlationId, clientId, blobId);
  }

  public BlobId getBlobId() {
    return blobId;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
      sizeSent += written;
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DeleteRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append("]");
    return sb.toString();
  }
}
