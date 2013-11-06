package com.github.ambry.shared;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Request to change TTL for a blob
 */

public class TTLRequest extends RequestOrResponse {
  private String blobId;
  private long partitionId;
  private long newTTL;
  private int sizeSent;

  private static final int Partition_Size_In_Bytes = 8;
  private static final int Blob_Id_Size_InBytes = 2;
  private static final int TTL_Size_In_Bytes = 8;

  public TTLRequest(long partition, int correlationId, String clientId, String blobId, long newTTL) {
    super(RequestResponseType.DeleteRequest, (short)1, correlationId, clientId);
    this.blobId = blobId;
    this.partitionId = partition;
    this.newTTL = newTTL;
    sizeSent = 0;
  }

  public String getBlobId() {
    return blobId;
  }

  public long getPartitionId() {
    return partitionId;
  }

  public long getNewTTL() {
    return newTTL;
  }

  public static TTLRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.TTLRequest;
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    long partition = stream.readLong();
    String blobId = Utils.readShortString(stream);
    long newTTL = stream.readLong();

    // ignore version for now
    return new TTLRequest(partition, correlationId, clientId, blobId, newTTL);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putLong(partitionId);
      bufferToSend.putShort((short)blobId.length());
      bufferToSend.put(blobId.getBytes());
      bufferToSend.putLong(newTTL);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      int written = channel.write(bufferToSend);
      sizeSent += written;
    }
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  @Override
  public long sizeInBytes() {
    // header + partition + blobId
    return super.sizeInBytes() + Partition_Size_In_Bytes +
            Blob_Id_Size_InBytes + blobId.length() + TTL_Size_In_Bytes;
  }
}

