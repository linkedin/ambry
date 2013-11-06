package com.github.ambry.shared;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Delete request to delete blob
 */
public class DeleteRequest extends RequestOrResponse {
  private String blobId;
  private long partitionId;
  private int sizeSent;

  private static final int Partition_Size_In_Bytes = 8;
  private static final int Blob_Id_Size_InBytes = 2;

  public DeleteRequest(long partition, int correlationId, String clientId, String blobId) {
    super(RequestResponseType.DeleteRequest, (short)1, correlationId, clientId);
    this.blobId = blobId;
    this.partitionId = partition;
    sizeSent = 0;
  }

  public static DeleteRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.DeleteRequest;
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    long partition = stream.readLong();
    String blobId = Utils.readShortString(stream);

    // ignore version for now
    return new DeleteRequest(partition, correlationId, clientId, blobId);
  }

  public long getPartition() {
    return partitionId;
  }

  public String getBlobId() {
    return blobId;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putLong(partitionId);
      bufferToSend.putShort((short)blobId.length());
      bufferToSend.put(blobId.getBytes());
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
            Blob_Id_Size_InBytes + blobId.length();
  }
}
