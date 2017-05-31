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
  private BlobId blobId;
  private short accountId;
  private short containerId;
  private int deletionTimeInSecs;
  private int sizeSent;
  static final short Delete_Request_Version_V1 = 1;
  static final short Delete_Request_Version_V2 = 2;
  private static short currentVersion = Delete_Request_Version_V1;

  private static final int AccountId_ContainerId_Field_Size_InBytes = 2;
  private static final int DeletionTime_Field_Size_InBytes = 4;

  // @TODO: remove this constructor once DeleteRequest V2 is enabled
  public DeleteRequest(int correlationId, String clientId, BlobId blobId) {
    super(RequestOrResponseType.DeleteRequest, currentVersion, correlationId, clientId);
    this.blobId = blobId;
    sizeSent = 0;
  }

  public DeleteRequest(int correlationId, String clientId, BlobId blobId, short accountId, short containerId,
      int deletionTimeInSecs) {
    super(RequestOrResponseType.DeleteRequest, Delete_Request_Version_V2, correlationId, clientId);
    this.currentVersion = Delete_Request_Version_V2;
    this.blobId = blobId;
    this.accountId = accountId;
    this.containerId = containerId;
    this.deletionTimeInSecs = deletionTimeInSecs;
    sizeSent = 0;
  }

  public static ReceivedDeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
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
      if (currentVersion == Delete_Request_Version_V2) {
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

  @Override
  public long sizeInBytes() {
    // header + blobId
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    if (currentVersion == Delete_Request_Version_V2) {
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
    static ReceivedDeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      return new ReceivedDeleteRequest(correlationId, clientId, id);
    }
  }

  /**
   * Class to read protocol version 2 DeleteRequest from the stream.
   */
  private static class DeleteRequest_V2 {
    static ReceivedDeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      short accountId = stream.readShort();
      short containerId = stream.readShort();
      int deletionTimeInSecs = stream.readInt();
      return new ReceivedDeleteRequest(correlationId, clientId, id, accountId, containerId, deletionTimeInSecs);
    }
  }

  /**
   * Class that represents a DeleteRequest that was received and cannot be sent out.
   */
  public static class ReceivedDeleteRequest {
    private int correlationId;
    private String clientId;
    private BlobId blobId;
    private short accountId = DeleteRecord.ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
    private short containerId = DeleteRecord.ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
    private int deletionTimeInSecs = (int) Utils.Infinite_Time;

    ReceivedDeleteRequest(int correlationId, String clientId, BlobId blobId) {
      this.correlationId = correlationId;
      this.clientId = clientId;
      this.blobId = blobId;
    }

    ReceivedDeleteRequest(int correlationId, String clientId, BlobId blobId, short accountId, short containerId,
        int deletionTimeInSecs) {
      this.correlationId = correlationId;
      this.clientId = clientId;
      this.blobId = blobId;
      this.accountId = accountId;
      this.containerId = containerId;
      this.deletionTimeInSecs = deletionTimeInSecs;
    }

    public int getCorrelationId() {
      return correlationId;
    }

    public String getClientId() {
      return clientId;
    }

    public BlobId getBlobId() {
      return blobId;
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
  }
}
