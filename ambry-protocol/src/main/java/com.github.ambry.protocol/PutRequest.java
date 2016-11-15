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
  protected final long blobSize;
  protected final BlobId blobId;
  protected long sentBytes = 0;
  protected final BlobProperties properties;
  protected final BlobType blobType;
  protected final ByteBuffer blob;

  private static final int UserMetadata_Size_InBytes = 4;
  protected static final int Blob_Size_InBytes = 8;
  protected static final int BlobType_Size_InBytes = 2;
  protected static final short Put_Request_Version_V2 = 2;

  /**
   * Construct a PutRequest
   * @param correlationId the correlation id associated with the request.
   * @param clientId the clientId associated with the request.
   * @param blobId the {@link BlobId} of the blob that is being put as part of this request.
   * @param properties the {@link BlobProperties} associated with the request.
   * @param usermetadata the user metadata associated with the request.
   * @param materializedBlob the materialized buffer containing the blob data.
   * @param blobSize the size of the blob data.
   * @param blobType the type of the blob data.
   */
  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, ByteBuffer materializedBlob, long blobSize, BlobType blobType) {
    super(RequestOrResponseType.PutRequest, Put_Request_Version_V2, correlationId, clientId);
    this.blobId = blobId;
    this.properties = properties;
    this.usermetadata = usermetadata;
    this.blobSize = blobSize;
    this.blobType = blobType;
    this.blob = materializedBlob;
  }

  public static ReceivedPutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short versionId = stream.readShort();
    switch (versionId) {
      case Put_Request_Version_V2:
        return PutRequest_V2.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Request response version" + versionId);
    }
  }

  @Override
  public long sizeInBytes() {
    return sizeExcludingBlobSize() + blobSize;
  }

  protected int sizeExcludingBlobSize() {
    // size of (header + blobId + blob properties + metadata size + metadata + blob size + blob type)
    return (int) super.sizeInBytes() + blobId.sizeInBytes() + BlobPropertiesSerDe.getBlobPropertiesSize(properties)
        + UserMetadata_Size_InBytes + usermetadata.capacity() + Blob_Size_InBytes + BlobType_Size_InBytes;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
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
        written += channel.write(blob);
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
      sb.append(", ").append(properties);
    } else {
      sb.append(", ").append("Properties=Null");
    }
    if (usermetadata != null) {
      sb.append(", ").append("UserMetaDataSize=").append(usermetadata.capacity());
    } else {
      sb.append(", ").append("UserMetaDataSize=0");
    }
    sb.append(", ").append("blobType=").append(blobType);
    sb.append(", ").append("blobSize=").append(blobSize);
    sb.append("]");
    return sb.toString();
  }

  // Class to read protocol version 2 Put Request from the stream.
  private static class PutRequest_V2 {
    static ReceivedPutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      BlobType blobType = BlobType.values()[stream.readShort()];
      long blobSize = stream.readLong();
      return new ReceivedPutRequest(correlationId, clientId, id, properties, metadata, blobSize, blobType, stream);
    }
  }

  /**
   * Class that represents a PutRequest that was received and cannot be sent out.
   */
  public static class ReceivedPutRequest {
    private final int correlationId;
    private final String clientId;
    private final BlobId blobId;
    private final BlobProperties blobProperties;
    private final ByteBuffer userMetadata;
    private final long blobSize;
    private final BlobType blobType;
    private final InputStream blobStream;

    /**
     * Construct a ReceivedPutRequest with the given parameters.
     * @param correlationId the correlation id in the request.
     * @param clientId the clientId in the request.
     * @param blobId the {@link BlobId} of the blob being put.
     * @param blobProperties the {@link BlobProperties} associated with the blob being put.
     * @param userMetadata the userMetadata associated with the blob being put.
     * @param blobSize the size of the blob data.
     * @param blobType the type of the blob being put.
     * @param blobStream the {@link InputStream} containing the data associated with the blob.
     */
    ReceivedPutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties blobProperties,
        ByteBuffer userMetadata, long blobSize, BlobType blobType, InputStream blobStream) {
      this.correlationId = correlationId;
      this.clientId = clientId;
      this.blobId = blobId;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.blobSize = blobSize;
      this.blobType = blobType;
      this.blobStream = blobStream;
    }

    /**
     * @return the correlation id.
     */
    public int getCorrelationId() {
      return correlationId;
    }

    /**
     * @return the client id.
     */
    public String getClientId() {
      return clientId;
    }

    /**
     * @return the {@link BlobId} associated with the blob in this request.
     */
    public BlobId getBlobId() {
      return blobId;
    }

    /**
     * @return the {@link BlobProperties} associated with the blob in this request.
     */
    public BlobProperties getBlobProperties() {
      return blobProperties;
    }

    /**
     * @return the userMetadata associated with the blob in this request.
     */
    public ByteBuffer getUsermetadata() {
      return userMetadata;
    }

    /**
     * @return the size of the blob content.
     */
    public long getBlobSize() {
      return blobSize;
    }

    /**
     * @return the {@link BlobType} of the blob in this request.
     */
    public BlobType getBlobType() {
      return blobType;
    }

    /**
     * @return the {@link InputStream} from which to stream in the data associated with the blob.
     */
    public InputStream getBlobStream() {
      return blobStream;
    }
  }
}
