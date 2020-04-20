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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;


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

  // Used to carry blob content in ambry-frontend when creating this PutRequest.
  protected ByteBuf blob;
  protected ByteBuffer[] nioBuffers;
  // crc will cover all the fields associated with the blob, namely:
  // blob type
  // BlobId
  // BlobProperties
  // UserMetadata
  // BlobData

  // Used to calculate crc value in ambry-frontend.
  private final Crc32 crc;
  private ByteBuf crcByteBuf;
  private boolean okayToWriteCrc = false;
  private int sizeExcludingBlobAndCrc = -1;
  private int bufferIndex = 0;

  // Used to carry blob content in ambry-server when receiving this PutRequest.
  protected final InputStream blobStream;
  // Crc value received from network.
  protected final Long crcValue;

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
      ByteBuffer usermetadata, ByteBuf materializedBlob, long blobSize, BlobType blobType,
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
    this.crcByteBuf = PooledByteBufAllocator.DEFAULT.ioBuffer(CRC_SIZE_IN_BYTES);
    this.blobStream = null;
    this.crcValue = null;
  }

  /**
   * Construct a PutRequest while deserializing it from stream or bytes.
   * @param correlationId the correlation id associated with the request.
   * @param clientId the clientId associated with the request.
   * @param blobId the {@link BlobId} of the blob that is being put as part of this request.
   * @param blobProperties the {@link BlobProperties} associated with the request.
   * @param userMetadata the user metadata associated with the request.
   * @param blobSize the size of the blob data.
   * @param blobType the type of the blob data.
   * @param blobEncryptionKey the encryption key for the blob.
   * @param blobStream the {@link InputStream} containing the data associated with the blob.
   * @param crc the crc associated with this request.
   */
  private PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties blobProperties,
      ByteBuffer userMetadata, long blobSize, BlobType blobType, ByteBuffer blobEncryptionKey, InputStream blobStream,
      Long crc) {
    super(RequestOrResponseType.PutRequest, currentVersion, correlationId, clientId);
    this.blobId = blobId;
    this.properties = blobProperties;
    this.usermetadata = userMetadata;
    this.blobSize = blobSize;
    this.blobType = blobType;
    this.blobEncryptionKey = blobEncryptionKey;
    this.blob = null;
    this.crc = null;
    this.crcByteBuf = null;
    this.blobStream = blobStream;
    this.crcValue = crc;
  }

  /**
   * Deserialize {@link PutRequest} from a given {@link DataInputStream}.
   * @param stream The stream that contains the serialized bytes.
   * @param map The {@link ClusterMap} to help build {@link BlobId}.
   * @return A deserialized {@link PutRequest}.
   * @throws IOException Any I/O Errors.
   */
  public static PutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
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

  /**
   * Construct the bufferToSend to serialize request metadata and other blob related information. The newly constructed
   * bufferToSend will not include the blob content as it's carried by the {@code blob} field in this class.
   */
  @Override
  protected void prepareBuffer() {
    // bufferToSend now is the header ByteBuf, it will store serialized header, without blob content and crc
    bufferToSend = PooledByteBufAllocator.DEFAULT.ioBuffer(sizeExcludingBlobAndCrcSize());
    writeHeader();
    int crcStart = bufferToSend.writerIndex();
    bufferToSend.writeBytes(blobId.toBytes());
    BlobPropertiesSerDe.serializeBlobProperties(bufferToSend, properties);
    bufferToSend.writeInt(usermetadata.capacity());
    bufferToSend.writeBytes(usermetadata);
    bufferToSend.writeShort((short) blobType.ordinal());
    short keyLength = blobEncryptionKey == null ? 0 : (short) blobEncryptionKey.remaining();
    bufferToSend.writeShort(keyLength);
    if (keyLength > 0) {
      bufferToSend.writeBytes(blobEncryptionKey);
    }
    bufferToSend.writeLong(blobSize);

    // Now compute crc for the put request.
    crc.update(bufferToSend.nioBuffer(crcStart, bufferToSend.writerIndex() - crcStart));
    for (ByteBuffer bb : blob.nioBuffers()) {
      crc.update(bb);
      // change it back to 0 since we are going to write it to the channel later.
      bb.position(0);
    }
    crcByteBuf.writeLong(crc.getValue());

    // Now construct the real bufferToSend, which should be a composite ByteBuf.
    CompositeByteBuf compositeByteBuf = bufferToSend.alloc().compositeHeapBuffer(2 + blob.nioBufferCount());
    compositeByteBuf.addComponent(true, bufferToSend);
    if (blob instanceof CompositeByteBuf) {
      Iterator<ByteBuf> iter = ((CompositeByteBuf) blob).iterator();
      while (iter.hasNext()) {
        compositeByteBuf.addComponent(true, iter.next());
      }
    } else {
      compositeByteBuf.addComponent(true, blob);
    }
    compositeByteBuf.addComponent(true, crcByteBuf);
    blob = null;
    crcByteBuf = null;
    bufferToSend = compositeByteBuf;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      prepareBuffer();
    }
    if (nioBuffers == null) {
      nioBuffers = bufferToSend.nioBuffers();
    }
    long written = 0;
    if (sentBytes < sizeInBytes()) {
      int totalWrittenBytes = 0, currentWritten = -1;
      while (bufferIndex < nioBuffers.length && currentWritten != 0) {
        currentWritten = -1;
        ByteBuffer byteBuffer = nioBuffers[bufferIndex];
        if (!byteBuffer.hasRemaining()) {
          // some bytebuffers are zero length, ignore those bytebuffers.
          bufferIndex++;
        } else {
          currentWritten = channel.write(byteBuffer);
          totalWrittenBytes += currentWritten;
        }
      }
      bufferToSend.skipBytes(totalWrittenBytes);
      written += totalWrittenBytes;
    }
    sentBytes += written;
    return written;
  }

  /**
   * Override release even if the {@link #content()}'s result is not null. When the {@link #prepareBuffer()} is
   * not invoked, there will be {@link #blob} and {@link #crcByteBuf} created but not returned by {@link #content()},
   * and we have to release them.
   * @return
   */
  @Override
  public boolean release() {
    if (blob != null) {
      blob.release();
      blob = null;
    }
    if (crcByteBuf != null) {
      crcByteBuf.release();
      crcByteBuf = null;
    }
    if (bufferToSend != null) {
      bufferToSend.release();
    }
    return false;
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
   * @return the {@link BlobId} associated with the blob in this request.
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the {@link BlobProperties} associated with the blob in this request.
   */
  public BlobProperties getBlobProperties() {
    return properties;
  }

  /**
   * @return the userMetadata associated with the blob in this request.
   */
  public ByteBuffer getUsermetadata() {
    return usermetadata;
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
   * This method should only be used in the ambry-server after receiving a PutRequest.
   * @return the {@link InputStream} from which to stream in the data associated with the blob.
   */
  public InputStream getBlobStream() {
    return blobStream;
  }

  /**
   * This method should only be used in the ambry-server after receiving a PutRequest.
   * @return the crc associated with the request.
   */
  public Long getCrc() {
    return crcValue;
  }

  /**
   * Class to read protocol version 3 PutRequest from the stream.
   */
  private static class PutRequest_V3 {
    static PutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      CrcInputStream crcInputStream = new CrcInputStream(stream);
      stream = new DataInputStream(crcInputStream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      int mdSize = stream.readInt();
      ByteBuffer metadata = Utils.readByteBufferFromCrcInputStream(crcInputStream, mdSize);
      BlobType blobType = BlobType.values()[stream.readShort()];
      long blobSize = stream.readLong();
      ByteBufferInputStream blobStream =
          Utils.getByteBufferInputStreamFromCrcInputStream(crcInputStream, (int) blobSize);
      long computedCrc = crcInputStream.getValue();
      long receivedCrc = stream.readLong();
      if (computedCrc != receivedCrc) {
        throw new IOException("CRC mismatch, data in PutRequest is unreliable");
      }
      return new PutRequest(correlationId, clientId, id, properties, metadata, blobSize, blobType, null, blobStream,
          receivedCrc);
    }
  }

  /**
   * Class to read protocol version 4 PutRequest from the stream.
   */
  private static class PutRequest_V4 {
    static PutRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      CrcInputStream crcInputStream = new CrcInputStream(stream);
      stream = new DataInputStream(crcInputStream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      int mdSize = stream.readInt();
      ByteBuffer metadata = Utils.readByteBufferFromCrcInputStream(crcInputStream, mdSize);
      BlobType blobType = BlobType.values()[stream.readShort()];
      ByteBuffer blobEncryptionKey = Utils.readShortBuffer(stream);
      long blobSize = stream.readLong();
      ByteBufferInputStream blobStream =
          Utils.getByteBufferInputStreamFromCrcInputStream(crcInputStream, (int) blobSize);
      long computedCrc = crcInputStream.getValue();
      long receivedCrc = stream.readLong();
      if (computedCrc != receivedCrc) {
        throw new IOException("CRC mismatch, data in PutRequest is unreliable");
      }
      return new PutRequest(correlationId, clientId, id, properties, metadata, blobSize, blobType,
          blobEncryptionKey.remaining() == 0 ? null : blobEncryptionKey, blobStream, receivedCrc);
    }
  }
}
