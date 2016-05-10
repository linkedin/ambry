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
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobPropertiesSerDe;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * A Put Request used to put a blob
 */
public class PutRequest extends RequestOrResponse {
  protected final ByteBuffer usermetadata;
  protected final InputStream blobStream;
  protected final long blobSize;
  protected final BlobId blobId;
  protected long sentBytes = 0;
  protected final BlobProperties properties;
  protected final BlobType blobType;

  private static final int UserMetadata_Size_InBytes = 4;
  protected static final int Blob_Size_InBytes = 8;
  protected static final int BlobType_Size_InBytes = 2;
  protected static final short Put_Request_Version_V1 = 1;
  // Version 2 added to support chunking for large objects, where the size of a chunk can be different from the size
  // in the BlobProperties (which will be the size of the whole object).
  protected static final short Put_Request_Version_V2 = 2;

  protected PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream blobStream, long blobSize, BlobType blobType, short versionId) {
    super(RequestOrResponseType.PutRequest, versionId, correlationId, clientId);
    this.blobId = blobId;
    this.properties = properties;
    this.usermetadata = usermetadata;
    this.blobStream = blobStream;
    this.blobSize = blobSize;
    this.blobType = blobType;
  }

  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream blobStream, long blobSize, BlobType blobType) {
    this(correlationId, clientId, blobId, properties, usermetadata, blobStream, blobSize, blobType,
        Put_Request_Version_V2);
  }

  @Deprecated
  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream data) {
    this(correlationId, clientId, blobId, properties, usermetadata, data, properties.getBlobSize(), BlobType.DataBlob);
  }

  public static PutRequest readFrom(DataInputStream stream, ClusterMap map)
      throws IOException {
    short versionId = stream.readShort();
    switch (versionId) {
      case Put_Request_Version_V1:
        return PutRequest_V1.readFrom(stream, map);
      case Put_Request_Version_V2:
        return PutRequest_V2.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Request response version" + versionId);
    }
  }

  public BlobId getBlobId() {
    return blobId;
  }

  public BlobProperties getBlobProperties() {
    return properties;
  }

  public ByteBuffer getUsermetadata() {
    return usermetadata;
  }

  public InputStream getBlobStream() {
    return blobStream;
  }

  public long getBlobSize() {
    return blobSize;
  }

  public BlobType getBlobType() {
    return blobType;
  }

  @Override
  public long sizeInBytes() {
    return sizeExcludingBlobSize() + blobSize;
  }

  protected int sizeExcludingBlobSize() {
    // size of (header + blobId + blob properties + metadata size + metadata + blob size + blob type)
    return (int) super.sizeInBytes() + blobId.sizeInBytes() + BlobPropertiesSerDe.getBlobPropertiesSize(properties) +
        UserMetadata_Size_InBytes + usermetadata.capacity() + Blob_Size_InBytes + BlobType_Size_InBytes;
  }

  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long totalWritten = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(sizeExcludingBlobSize());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      BlobPropertiesSerDe.putBlobPropertiesToBuffer(bufferToSend, properties);
      bufferToSend.putInt(usermetadata.capacity());
      bufferToSend.put(usermetadata);
      bufferToSend.putShort((short) blobType.ordinal());
      bufferToSend.putLong(blobSize);
      bufferToSend.flip();
    }
    while (sentBytes < sizeInBytes()) {
      if (bufferToSend.remaining() > 0) {
        int toWrite = bufferToSend.remaining();
        int written = channel.write(bufferToSend);
        totalWritten += written;
        sentBytes += written;
        if (toWrite != written || sentBytes == sizeInBytes()) {
          break;
        }
      }
      logger.trace("sent Bytes from Put Request {}", sentBytes);
      bufferToSend.clear();
      int streamReadCount = blobStream
          .read(bufferToSend.array(), 0, (int) Math.min(bufferToSend.capacity(), (sizeInBytes() - sentBytes)));
      bufferToSend.limit(streamReadCount);
    }
    return totalWritten;
  }

  @Override
  public boolean isSendComplete() {
    return sizeInBytes() == sentBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PutRequest[");
    sb.append("BlobID=").append(blobId.getID());
    if (properties != null) {
      sb.append(", ").append(getBlobProperties());
    } else {
      sb.append(", ").append("Properties=Null");
    }
    if (usermetadata != null) {
      sb.append(", ").append("UserMetaDataSize=").append(getUsermetadata().capacity());
    } else {
      sb.append(", ").append("UserMetaDataSize=0");
    }
    sb.append(", ").append("blobType=").append(getBlobType());
    sb.append(", ").append("blobSize=").append(getBlobSize());
    sb.append("]");
    return sb.toString();
  }

  // Class to read protocol version 1 Put Request from the stream.
  private static class PutRequest_V1 {
    static PutRequest readFrom(DataInputStream stream, ClusterMap map)
        throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      return new PutRequest(correlationId, clientId, id, properties, metadata, stream, properties.getBlobSize(),
          BlobType.DataBlob, Put_Request_Version_V1);
    }
  }

  // Class to read protocol version 2 Put Request from the stream.
  private static class PutRequest_V2 {
    static PutRequest readFrom(DataInputStream stream, ClusterMap map)
        throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      BlobType blobType = BlobType.values()[stream.readShort()];
      long blobSize = stream.readLong();
      return new PutRequest(correlationId, clientId, id, properties, metadata, stream, blobSize, blobType,
          Put_Request_Version_V2);
    }
  }
}
