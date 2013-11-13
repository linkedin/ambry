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
  private BlobId blobId;
  private long partitionId;
  private long newTTL;
  private int sizeSent;

  private static final int TTL_Size_In_Bytes = 8;

  public TTLRequest(long partition, int correlationId, String clientId, BlobId blobId, long newTTL) {
    super(RequestResponseType.DeleteRequest, Request_Response_Version, correlationId, clientId);
    this.blobId = blobId;
    this.partitionId = partition;
    this.newTTL = newTTL;
    sizeSent = 0;
  }

  public BlobId getBlobId() {
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
    ByteBuffer blobIdBuffer = Utils.readShortBuffer(stream);
    BlobId id = new BlobId(blobIdBuffer);
    long newTTL = stream.readLong();

    // ignore version for now
    return new TTLRequest(partition, correlationId, clientId, id, newTTL);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putLong(partitionId);
      bufferToSend.putShort(blobId.sizeInBytes());
      bufferToSend.put(blobId.toBytes());
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
    return super.sizeInBytes() + PartitionId_Size_In_Bytes +
            Blob_Id_Size_In_Bytes + blobId.sizeInBytes() + TTL_Size_In_Bytes;
  }
}

