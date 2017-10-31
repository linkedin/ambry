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
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
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
  protected final ByteBuffer blobEncryptionKey;
  protected final ByteBuffer blob;
  // crc will cover all the fields associated with the blob, namely:
  // blob type
  // BlobId
  // BlobProperties
  // UserMetadata
  // BlobData
  private final Crc32 crc;
  private final ByteBuffer crcBuf;
  private boolean okayToWriteCrc = false;
  private int sizeExcludingBlobAndCrc = -1;

  private static final int USERMETADATA_SIZE_IN_BYTES = Integer.BYTES;
  private static final int BLOB_SIZE_IN_BYTES = Long.BYTES;
  private static final int BLOBTYPE_SIZE_IN_BYTES = Short.BYTES;
  private static final int BLOBKEYLENGTH_SIZE_IN_BYTES = Short.BYTES;
  private static final int CRC_SIZE_IN_BYTES = Long.BYTES;
  private static final short PUT_REQUEST_VERSION_V3 = 3;
  static final short PUT_REQUEST_VERSION_V4 = 4;

  private static final short currentVersion = PUT_REQUEST_VERSION_V4;

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
   * @param blobEncryptionKey the encryption key for the blob.
   */
  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, ByteBuffer materializedBlob, long blobSize, BlobType blobType,
      ByteBuffer blobEncryptionKey) {
    super(RequestOrResponseType.PutRequest, currentVersion, correlationId, clientId);
    this.blobId = blobId;
    this.properties = properties;
    this.usermetadata = usermetadata;
    this.blobSize = blobSize;
    this.blobType = blobType;
    this.blobEncryptionKey = blobEncryptionKey;
    this.blob = materializedBlob;
    this.crc = new Crc32();
    this.crcBuf = ByteBuffer.allocate(CRC_SIZE_IN_BYTES);
  }

  public static ReceivedPutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short versionId = stream.readShort();
    switch (versionId) {
      case PUT_REQUEST_VERSION_V3:
        return PutRequest_V3.readFrom(stream, map);
      case PUT_REQUEST_VERSION_V4:
        return PutRequest_V4.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Request response version" + versionId);
    }
  }

  @Override
  public long sizeInBytes() {
    return sizeExcludingBlobAndCrcSize() + blobSize + CRC_SIZE_IN_BYTES;
  }

  private int sizeExcludingBlobAndCrcSize() {
    // size of (header + blobId + blob properties + metadata size + metadata + blob size + blob type)
    if (sizeExcludingBlobAndCrc == -1) {
      sizeExcludingBlobAndCrc =
          (int) super.sizeInBytes() + blobId.sizeInBytes() + BlobPropertiesSerDe.getBlobPropertiesSerDeSize(properties)
              + USERMETADATA_SIZE_IN_BYTES + usermetadata.capacity() + BLOB_SIZE_IN_BYTES + BLOBTYPE_SIZE_IN_BYTES;
      sizeExcludingBlobAndCrc += BLOBKEYLENGTH_SIZE_IN_BYTES;
      if (blobEncryptionKey != null) {
        sizeExcludingBlobAndCrc += blobEncryptionKey.remaining();
      }
    }
    return sizeExcludingBlobAndCrc;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (sentBytes < sizeInBytes()) {
      if (bufferToSend == null) {
        // this is the first time this method was called, prepare the buffer to send the header and other metadata
        // (everything except the blob content).
        bufferToSend = ByteBuffer.allocate(sizeExcludingBlobAndCrcSize());
        writeHeader();
        int crcStart = bufferToSend.position();
        bufferToSend.put(blobId.toBytes());
        BlobPropertiesSerDe.serializeBlobProperties(bufferToSend, properties);
        bufferToSend.putInt(usermetadata.capacity());
        bufferToSend.put(usermetadata);
        bufferToSend.putShort((short) blobType.ordinal());
        short keyLength = blobEncryptionKey == null ? 0 : (short) blobEncryptionKey.remaining();
        bufferToSend.putShort(keyLength);
        if (keyLength > 0) {
          bufferToSend.put(blobEncryptionKey);
        }
        bufferToSend.putLong(blobSize);
        crc.update(bufferToSend.array(), bufferToSend.arrayOffset() + crcStart, bufferToSend.position() - crcStart);
        crc.update(blob.array(), blob.arrayOffset(), blob.remaining());
        crcBuf.putLong(crc.getValue());
        crcBuf.flip();
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
        okayToWriteCrc = !blob.hasRemaining();
      }

      if (okayToWriteCrc && crcBuf.hasRemaining()) {
        written += channel.write(crcBuf);
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

  /**
   * Class to read protocol version 3 PutRequest from the stream.
   */
  private static class PutRequest_V3 {
    static ReceivedPutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      CrcInputStream crcInputStream = new CrcInputStream(stream);
      stream = new DataInputStream(crcInputStream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      BlobType blobType = BlobType.values()[stream.readShort()];
      long blobSize = stream.readLong();
      ByteBufferInputStream blobStream = new ByteBufferInputStream(stream, (int) blobSize);
      long computedCrc = crcInputStream.getValue();
      long receivedCrc = stream.readLong();
      if (computedCrc != receivedCrc) {
        throw new IOException("CRC mismatch, data in PutRequest is unreliable");
      }
      return new ReceivedPutRequest(correlationId, clientId, id, properties, metadata, blobSize, blobType, null,
          blobStream, receivedCrc);
    }
  }

  /**
   * Class to read protocol version 4 PutRequest from the stream.
   */
  private static class PutRequest_V4 {
    static ReceivedPutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      CrcInputStream crcInputStream = new CrcInputStream(stream);
      stream = new DataInputStream(crcInputStream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      BlobType blobType = BlobType.values()[stream.readShort()];
      ByteBuffer blobEncryptionKey = Utils.readShortBuffer(stream);
      long blobSize = stream.readLong();
      ByteBufferInputStream blobStream = new ByteBufferInputStream(stream, (int) blobSize);
      long computedCrc = crcInputStream.getValue();
      long receivedCrc = stream.readLong();
      if (computedCrc != receivedCrc) {
        throw new IOException("CRC mismatch, data in PutRequest is unreliable");
      }
      return new ReceivedPutRequest(correlationId, clientId, id, properties, metadata, blobSize, blobType,
          blobEncryptionKey.remaining() == 0 ? null : blobEncryptionKey, blobStream, receivedCrc);
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
    private final ByteBuffer blobEncryptionKey;
    private final InputStream blobStream;
    private final Long receivedCrc;

    /**
     * Construct a ReceivedPutRequest with the given parameters.
     * @param correlationId the correlation id in the request.
     * @param clientId the clientId in the request.
     * @param blobId the {@link BlobId} of the blob being put.
     * @param blobProperties the {@link BlobProperties} associated with the blob being put.
     * @param userMetadata the userMetadata associated with the blob being put.
     * @param blobSize the size of the blob data.
     * @param blobType the type of the blob being put.
     * @param blobEncryptionKey the encryption key of the blob.
     * @param blobStream the {@link InputStream} containing the data associated with the blob.
     * @param crc the crc associated with this request.
     */
    ReceivedPutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties blobProperties,
        ByteBuffer userMetadata, long blobSize, BlobType blobType, ByteBuffer blobEncryptionKey, InputStream blobStream,
        Long crc) throws IOException {
      this.correlationId = correlationId;
      this.clientId = clientId;
      this.blobId = blobId;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.blobSize = blobSize;
      this.blobType = blobType;
      this.blobEncryptionKey = blobEncryptionKey;
      this.blobStream = blobStream;
      this.receivedCrc = crc;
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
     * @return the encryption key of the blob in this request.
     */
    public ByteBuffer getBlobEncryptionKey() {
      return blobEncryptionKey;
    }

    /**
     * @return the {@link InputStream} from which to stream in the data associated with the blob.
     */
    public InputStream getBlobStream() {
      return blobStream;
    }

    /**
     * @return the crc associated with the request.
     */
    public Long getCrc() {
      return receivedCrc;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("ReceivedPutRequest[");
      sb.append("BlobID=").append(blobId.getID());
      sb.append(", ").append("PartitionId=").append(blobId.getPartition());
      sb.append(", ").append("ClientId=").append(clientId);
      sb.append(", ").append("CorrelationId=").append(correlationId);
      if (blobProperties != null) {
        sb.append(", ").append(blobProperties);
      } else {
        sb.append(", ").append("Properties=Null");
      }
      if (userMetadata != null) {
        sb.append(", ").append("UserMetaDataSize=").append(userMetadata.capacity());
      } else {
        sb.append(", ").append("UserMetaDataSize=0");
      }
      sb.append(", ").append("blobType=").append(blobType);
      sb.append(", ").append("blobSize=").append(blobSize);
      sb.append(", ").append("crc=").append(receivedCrc);
      sb.append(", ").append("BlobKeyAvailable=").append(blobEncryptionKey != null);
      sb.append("]");
      return sb.toString();
    }
  }
}
