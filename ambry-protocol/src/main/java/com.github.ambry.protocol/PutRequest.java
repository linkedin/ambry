package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobPropertiesSerDe;
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
  private ByteBuffer usermetadata;
  private InputStream data;
  private long dataSize;
  private BlobId blobId;
  private long sentBytes = 0;
  private BlobProperties properties;

  private static final int UserMetadata_Size_InBytes = 4;
  private static final int BlobData_Size_InBytes = 8;
  private static final short Put_Request_Version_V1 = 1;
  private static final short Put_Request_Version_V2 = 2;

  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream data, long dataSize, short versionId) {
    super(RequestOrResponseType.PutRequest, versionId, correlationId, clientId);
    this.blobId = blobId;
    this.properties = properties;
    this.usermetadata = usermetadata;
    this.data = data;
    this.dataSize = dataSize;
  }

  public PutRequest(int correlationId, String clientId, BlobId blobId, BlobProperties properties,
      ByteBuffer usermetadata, InputStream data, long dataSize) {
    this(correlationId, clientId, blobId, properties, usermetadata, data, dataSize, Put_Request_Version_V2);
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

  public InputStream getData() {
    return data;
  }

  public long getDataSize() {
    return dataSize;
  }

  @Override
  public long sizeInBytes() {
    // sizeExcludingData + blob size
    return sizeExcludingData() + dataSize;
  }

  private int sizeExcludingData() {
    // size of (header + blobId + blob properties + metadata size + metadata + blob size)
    return (int) super.sizeInBytes() + blobId.sizeInBytes() + BlobPropertiesSerDe.getBlobPropertiesSize(properties) +
        UserMetadata_Size_InBytes + usermetadata.capacity() + BlobData_Size_InBytes;
  }

  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long totalWritten = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(sizeExcludingData());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      BlobPropertiesSerDe.putBlobPropertiesToBuffer(bufferToSend, properties);
      bufferToSend.putInt(usermetadata.capacity());
      bufferToSend.put(usermetadata);
      bufferToSend.putLong(dataSize);
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
      int dataRead =
          data.read(bufferToSend.array(), 0, (int) Math.min(bufferToSend.capacity(), (sizeInBytes() - sentBytes)));
      bufferToSend.limit(dataRead);
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
    sb.append(", ").append("dataSize=").append(getDataSize());
    sb.append("]");
    return sb.toString();
  }

  // Class to read protocol version 1 Put Request from the stream.
  public static class PutRequest_V1 {
    public static PutRequest readFrom(DataInputStream stream, ClusterMap map)
        throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      InputStream data = stream;
      return new PutRequest(correlationId, clientId, id, properties, metadata, data, properties.getBlobSize(),
          Put_Request_Version_V1);
    }
  }

  // Class to read protocol version 2 Put Request from the stream.
  public static class PutRequest_V2 {
    public static PutRequest readFrom(DataInputStream stream, ClusterMap map)
        throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(stream);
      ByteBuffer metadata = Utils.readIntBuffer(stream);
      long dataSize = stream.readLong();
      InputStream data = stream;
      return new PutRequest(correlationId, clientId, id, properties, metadata, data, dataSize,
          Put_Request_Version_V2);
    }
  }
}
