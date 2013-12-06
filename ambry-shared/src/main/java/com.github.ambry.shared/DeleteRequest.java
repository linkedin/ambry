package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
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
  private int sizeSent;

  public DeleteRequest(int correlationId, String clientId, BlobId blobId) {
    super(RequestResponseType.DeleteRequest, Request_Response_Version, correlationId, clientId);
    this.blobId = blobId;
    sizeSent = 0;
  }

  public static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    RequestResponseType type = RequestResponseType.DeleteRequest;
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    BlobId id = new BlobId(stream, map);
    // ignore version for now
    return new DeleteRequest(correlationId, clientId, id);
  }

  public BlobId getBlobId() {
    return blobId;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
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
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes();
  }
}
