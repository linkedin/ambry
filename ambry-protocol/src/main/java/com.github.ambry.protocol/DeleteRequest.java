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
import com.github.ambry.messageformat.DeleteRecord;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Delete request to delete blob
 */
public class DeleteRequest extends RequestOrResponse {
  private final BlobId blobId;
  private final short accountId;
  private final short containerId;
  private final int deletionTimeInSecs;
  private int sizeSent;
  private final short version;
  static final short Delete_Request_Version_V1 = 1;
  static final short Delete_Request_Version_V2 = 2;
  private static short currentVersion = Delete_Request_Version_V1;

  private static final int AccountId_ContainerId_Field_Size_InBytes = 2;
  private static final int DeletionTime_Field_Size_InBytes = 4;

  // @TODO: remove this constructor once DeleteRequest V2 is enabled
  public DeleteRequest(int correlationId, String clientId, BlobId blobId) {
    this(correlationId, clientId, blobId, DeleteRecord.ACCOUNTID_DEFAULT_VALUE, DeleteRecord.CONTAINERID_DEFAULT_VALUE,
        (int) Utils.Infinite_Time, currentVersion);
  }

  public DeleteRequest(int correlationId, String clientId, BlobId blobId, short accountId, short containerId,
      int deletionTimeInSecs) {
    this(correlationId, clientId, blobId, accountId, containerId, deletionTimeInSecs, Delete_Request_Version_V2);
  }

  private DeleteRequest(int correlationId, String clientId, BlobId blobId, short accountId, short containerId,
      int deletionTimeInSecs, short version) {
    super(RequestOrResponseType.DeleteRequest, version, correlationId, clientId);
    this.version = version;
    this.blobId = blobId;
    this.accountId = accountId;
    this.containerId = containerId;
    this.deletionTimeInSecs = deletionTimeInSecs;
    sizeSent = 0;
  }

  public static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    switch (version) {
      case Delete_Request_Version_V1:
        return DeleteRequest_V1.readFrom(stream, map);
      case Delete_Request_Version_V2:
        return DeleteRequest_V2.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Delete Request version " + version);
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      if (version == Delete_Request_Version_V2) {
        bufferToSend.putShort(accountId);
        bufferToSend.putShort(containerId);
        bufferToSend.putInt(deletionTimeInSecs);
      }
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

  public int getCorrelationId() {
    return correlationId;
  }

  public String getClientId() {
    return clientId;
  }

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  public int getDeletionTimeInSecs() {
    return deletionTimeInSecs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    if (version == Delete_Request_Version_V2) {
      // accountId
      sizeInBytes += AccountId_ContainerId_Field_Size_InBytes;
      // containerId
      sizeInBytes += AccountId_ContainerId_Field_Size_InBytes;
      // deletion time
      sizeInBytes += DeletionTime_Field_Size_InBytes;
    }
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DeleteRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(accountId);
    sb.append(", ").append("ContainerId=").append(containerId);
    sb.append(", ").append("DeletionTimeInSecs=").append(deletionTimeInSecs);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Class to read protocol version 1 DeleteRequest from the stream.
   */
  private static class DeleteRequest_V1 {
    static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      return new DeleteRequest(correlationId, clientId, id);
    }
  }

  /**
   * Class to read protocol version 2 DeleteRequest from the stream.
   */
  private static class DeleteRequest_V2 {
    static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      short accountId = stream.readShort();
      short containerId = stream.readShort();
      int deletionTimeInSecs = stream.readInt();
      return new DeleteRequest(correlationId, clientId, id, accountId, containerId, deletionTimeInSecs);
    }
  }
}
