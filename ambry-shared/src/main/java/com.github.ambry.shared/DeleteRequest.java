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
  private BlobId blobId;
  private long partitionId;
  private int sizeSent;

  public DeleteRequest(long partition, int correlationId, String clientId, BlobId blobId) {
    super(RequestResponseType.DeleteRequest, Request_Response_Version, correlationId, clientId);
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
    ByteBuffer blobIdBuffer = Utils.readShortBuffer(stream);
    BlobId id = new BlobId(blobIdBuffer);
    // ignore version for now
    return new DeleteRequest(partition, correlationId, clientId, id);
  }

  public long getPartitionId() {
    return partitionId;
  }

  public BlobId getBlobId() {
    return blobId;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putLong(partitionId);
      bufferToSend.putShort(blobId.sizeInBytes());
      bufferToSend.put(blobId.toBytes());
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
    return super.sizeInBytes() + PartitionId_Size_In_Bytes +
            Blob_Id_Size_In_Bytes + blobId.sizeInBytes();
  }
}
