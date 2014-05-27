package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

// TODO: Rename to CancelTTLRequest

/**
 * Request to change TTL for a blob
 */
public class TTLRequest extends RequestOrResponse {
  private BlobId blobId;
  private long newTTL;
  private int sizeSent;

  private static final int TTL_Size_In_Bytes = 8;

  // TODO: Remove newTTL argument as part of rename to CancelTTLRequest
  public TTLRequest(int correlationId, String clientId, BlobId blobId, long newTTL) {
    super(RequestResponseType.DeleteRequest, Request_Response_Version, correlationId, clientId);
    this.blobId = blobId;
    this.newTTL = newTTL;
    sizeSent = 0;
  }

  public BlobId getBlobId() {
    return blobId;
  }

  public long getNewTTL() {
    return newTTL;
  }

  public static TTLRequest readFrom(DataInputStream stream, ClusterMap map)
      throws IOException {
    RequestResponseType type = RequestResponseType.TTLRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    BlobId id = new BlobId(stream, map);
    long newTTL = stream.readLong();

    // ignore version for now
    return new TTLRequest(correlationId, clientId, id, newTTL);
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
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
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes() + TTL_Size_In_Bytes;
  }
}

