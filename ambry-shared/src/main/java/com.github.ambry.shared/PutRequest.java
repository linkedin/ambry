package com.github.ambry.shared;


import com.github.ambry.utils.Utils;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobPropertySerDe;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A Put Request used to put a blob
 */
public class PutRequest extends RequestOrResponse {

  private long partition;
  private ByteBuffer usermetadata;
  private InputStream data;
  private String clientId;
  private String blobId;
  private long sentBytes = 0;
  private BlobProperties properties;


  private static final int Partition_Size_InBytes = 8;
  private static final int ClientId_Size_InBytes = 2;
  private static final int BlobId_Size_InBytes = 2;
  private static final int UserMetadata_Size_InBytes = 4;


  public PutRequest(short versionId, long partition, int correlationId,
                    String clientId, String blobId, ByteBuffer usermetadata,
                    InputStream data, BlobProperties properties) {
    super(RequestResponseType.PutRequest, versionId, correlationId);

    this.blobId = blobId;
    this.clientId = clientId;
    this.partition = partition;
    this.usermetadata = usermetadata;
    this.data = data;
    this.properties = properties;
  }

  public static PutRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.PutRequest) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    long logicalVolumeId = stream.readLong();
    String clientId = Utils.readShortString(stream);
    String blobId = Utils.readShortString(stream);
    BlobProperties properties = BlobPropertySerDe.getBlobPropertyFromStream(stream);
    ByteBuffer metadata = Utils.readIntBuffer(stream);
    InputStream data = stream;
    return new PutRequest(versionId, logicalVolumeId, correlationId, clientId, blobId, metadata, data, properties);
  }

  public long getPartition() {
    return partition;
  }

  public String getClientId() {
    return clientId;
  }

  public String getBlobId() {
    return blobId;
  }

  public ByteBuffer getUsermetadata() {
    return usermetadata;
  }

  public InputStream getData() {
    return data;
  }

  public long getDataSize() {
    return properties.getBlobSize();
  }

  public BlobProperties getBlobProperties() {
    return properties;
  }

  @Override
  public long sizeInBytes() {
    // sizeExcludingData + data size
    return  sizeExcludingData() + properties.getBlobSize();
  }

  private int sizeExcludingData() {
    // header + logicalVolumeId + clientId size + clientId +
    // blobId size + blobId + metadata size + metadata + data size
    return  (int)super.sizeInBytes() + Partition_Size_InBytes + ClientId_Size_InBytes + clientId.length() +
            BlobId_Size_InBytes + blobId.length() + UserMetadata_Size_InBytes + usermetadata.capacity() +
            BlobPropertySerDe.getBlobPropertySize(properties);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(sizeExcludingData());
      writeHeader();
      bufferToSend.putLong(partition);
      bufferToSend.putShort((short)clientId.length());
      bufferToSend.put(clientId.getBytes());
      bufferToSend.putShort((short)blobId.length());
      bufferToSend.put(blobId.getBytes());
      BlobPropertySerDe.putBlobPropertyToBuffer(bufferToSend, properties);
      bufferToSend.putInt(usermetadata.capacity());
      bufferToSend.put(usermetadata);
      bufferToSend.flip();
    }
    while (sentBytes < sizeInBytes()) {
      if (bufferToSend.remaining() > 0) {
        int toWrite = bufferToSend.remaining();
        int written = channel.write(bufferToSend);
        sentBytes += written;
        if (toWrite != written || sentBytes == sizeInBytes()) {
          break;
        }
      }
      logger.trace("sent Bytes from Put Request {}", sentBytes);
      bufferToSend.clear();
      int dataRead = data.read(bufferToSend.array(), 0, (int)Math.min(bufferToSend.capacity(), properties.getBlobSize()));
      bufferToSend.limit(dataRead);
    }
  }

  @Override
  public boolean isSendComplete() {
    return sizeInBytes() == sentBytes;
  }
}
