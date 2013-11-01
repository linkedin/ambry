package com.github.ambry.shared;


import com.github.ambry.utils.Utils;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A Put Request used to put a blob
 */
public class PutRequest extends RequestOrResponse {

  private long logicalVolumeId;
  private ByteBuffer metadata;
  private InputStream data;
  private String clientId;
  private String blobId;
  private Long dataSize;
  private long sentBytes = 0;


  private static final int Logical_Volume_Id_Size_InBytes = 8;
  private static final int ClientId_Size_InBytes = 2;
  private static final int BlobId_Size_InBytes = 2;
  private static final int Metadata_Size_InBytes = 4;
  private static final int Data_Size_InBytes = 8;


  public PutRequest(short versionId, long logicalVolumeId, int correlationId, String clientId,
                    String blobId, ByteBuffer metadata, InputStream data, long dataSize) {
    super(RequestResponseType.PutRequest, versionId, correlationId);

    this.blobId = blobId;
    this.clientId = clientId;
    this.logicalVolumeId = logicalVolumeId;
    this.metadata = metadata;
    this.data = data;
    this.dataSize = dataSize;
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
    ByteBuffer metadata = Utils.readIntBuffer(stream);
    long dataSize = stream.readLong();
    InputStream data = stream;
    return new PutRequest(versionId, logicalVolumeId, correlationId, clientId, blobId, metadata, data, dataSize);
  }

  public long getLogicalVolumeId() {
    return logicalVolumeId;
  }

  public String getClientId() {
    return clientId;
  }

  public String getBlobId() {
    return blobId;
  }

  public ByteBuffer getMetadata() {
    return metadata;
  }

  public InputStream getData() {
    return data;
  }

  @Override
  public long sizeInBytes() {
    // sizeExcludingData + data size
    return  sizeExcludingData() + dataSize;
  }

  private int sizeExcludingData() {
    // header + logicalVolumeId + clientId size + clientId +
    // blobId size + blobId + metadata size + metadata + data size
    return  (int)super.sizeInBytes() + Logical_Volume_Id_Size_InBytes + ClientId_Size_InBytes + clientId.length() +
            BlobId_Size_InBytes + blobId.length() + Metadata_Size_InBytes + metadata.capacity() + Data_Size_InBytes;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(sizeExcludingData());
      writeHeader();
      bufferToSend.putLong(logicalVolumeId);
      bufferToSend.putShort((short)clientId.length());
      bufferToSend.put(clientId.getBytes());
      bufferToSend.putShort((short)blobId.length());
      bufferToSend.put(blobId.getBytes());
      bufferToSend.putInt(metadata.capacity());
      bufferToSend.put(metadata);
      bufferToSend.putLong(dataSize);
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
      int dataRead = data.read(bufferToSend.array(), 0, (int)Math.min(bufferToSend.capacity(), dataSize));
      bufferToSend.limit(dataRead);
    }
  }

  @Override
  public boolean isSendComplete() {
    return sizeInBytes() == sentBytes;
  }
}
