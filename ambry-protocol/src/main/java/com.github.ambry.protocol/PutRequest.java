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
import com.github.ambry.utils.ByteBufferInputStream;
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
  protected ByteBuffer materializedBlobBuffer;

  private static final int UserMetadata_Size_InBytes = 4;
  protected static final int Blob_Size_InBytes = 8;
  protected static final int BlobType_Size_InBytes = 2;
  protected static final short Put_Request_Version_V2 = 2;

  /**
   * Public constructor that takes in a materialized blob stream to create a PutRequest
   * @param correlationId the correlation id associated with the request.
   * @param clientId the clientId associated with the request.
   * @param blobId the {@link BlobId} of the blob that is being put as part of this request.
   * @param properties the {@link BlobProperties} associated with the request.
   * @param usermetadata the user metadata associated with the request.
   * @param materializedBlobStream the materialized stream containing the blob data.
   * @param blobSize the size of the blob data.
   * @param blobType the type of the blob data.
   */
  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, ByteBufferInputStream materializedBlobStream, long blobSize, BlobType blobType) {
    this(correlationId, clientId, blobId, properties, usermetadata, (InputStream) materializedBlobStream, blobSize,
        blobType);
    this.materializedBlobBuffer = materializedBlobStream.getByteBuffer();
  }

  /**
   * Private constructor that differs from the public constructor in that the associated stream containing blob data
   * may or may not be already materialized in memory. This constructor is used when reconstructing a received
   * PutRequest.
   * @param correlationId the correlation id associated with the request.
   * @param clientId the clientId associated with the request.
   * @param blobId the {@link BlobId} of the blob that is being put as part of this request.
   * @param properties the {@link BlobProperties} associated with the request.
   * @param usermetadata the user metadata associated with the request.
   * @param blobStream the stream containing the blob data (may or may not already be materialized).
   * @param blobSize the size of the blob data.
   * @param blobType the type of the blob data.
   */
  private PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream blobStream, long blobSize, BlobType blobType) {
    super(RequestOrResponseType.PutRequest, Put_Request_Version_V2, correlationId, clientId);
    this.blobId = blobId;
    this.properties = properties;
    this.usermetadata = usermetadata;
    this.blobStream = blobStream;
    this.blobSize = blobSize;
    this.blobType = blobType;
    this.materializedBlobBuffer = null;
  }

  public static PutRequest readFrom(DataInputStream stream, ClusterMap map)
      throws IOException {
    short versionId = stream.readShort();
    switch (versionId) {
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
    if (materializedBlobBuffer == null) {
      // This PutRequest was constructed from a stream and cannot be written out.
      throw new IllegalStateException("Attempt to write out a PutRequest that cannot be written out.");
    }
    long written = 0;
    if (sentBytes < sizeInBytes()) {
      if (bufferToSend == null) {
        // this is the first time this method was called, prepare the buffer to send the header and other metadata
        // (everything except the blob content).
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

      // If the header and metadata are not yet written out completely, try and write out as much of it now.
      if (bufferToSend.hasRemaining()) {
        written = channel.write(bufferToSend);
      }

      // If the header and metadata were written out completely (in this call or a previous call),
      // try and write out as much of the blob now.
      if (!bufferToSend.hasRemaining()) {
        written += channel.write(materializedBlobBuffer);
      }
      sentBytes += written;
    }
    return written;
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
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
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
      return new PutRequest(correlationId, clientId, id, properties, metadata, stream, blobSize, blobType);
    }
  }
}
