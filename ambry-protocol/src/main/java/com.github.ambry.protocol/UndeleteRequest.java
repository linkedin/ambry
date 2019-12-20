/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 * Undelete request to undelete blob
 */
public class UndeleteRequest extends RequestOrResponse {
  private final BlobId blobId;
  private final long undeletionTimeInMs;
  private final static short CURRENT_VERSION = 1;
  protected static final int UNDELETION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

  private int sizeSent;


  /**
   * Constructs {@link UndeleteRequest}
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param blobId blobId of the delete request
   * @param undeletionTimeInMs deletion time of the blob in ms
   */
  protected UndeleteRequest(int correlationId, String clientId, BlobId blobId, long undeletionTimeInMs) {
    super(RequestOrResponseType.DeleteRequest, CURRENT_VERSION, correlationId, clientId);
    this.blobId = blobId;
    this.undeletionTimeInMs = undeletionTimeInMs;
    sizeSent = 0;
  }

  public static UndeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    BlobId id = new BlobId(stream, map);
    long deletionTimeInMs = stream.readLong();
    return new UndeleteRequest(correlationId, clientId, id, deletionTimeInMs);
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      bufferToSend.putLong(undeletionTimeInMs);
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

  public BlobId getBlobId() {
    return blobId;
  }

  public short getAccountId() {
    return blobId.getAccountId();
  }

  public short getContainerId() {
    return blobId.getContainerId();
  }

  public long getUndeletionTimeInMs() {
    return undeletionTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    sizeInBytes += UNDELETION_TIME_FIELD_SIZE_IN_BYTES;
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UndeleteRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("PartitionId=").append(blobId.getPartition());
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(blobId.getAccountId());
    sb.append(", ").append("ContainerId=").append(blobId.getContainerId());
    sb.append(", ").append("UndeletionTimeInMs=").append(undeletionTimeInMs);
    sb.append("]");
    return sb.toString();
  }

}
