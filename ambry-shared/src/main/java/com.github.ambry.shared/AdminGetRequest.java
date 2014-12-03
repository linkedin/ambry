package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * An admin get request used to get a blob from ambry server. It provides the ability to
 * get active blobs, expired blobs and deleted blobs as long as the blob exist physically
 * on disk
 */
public class AdminGetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private BlobId blobId;
  private long sizeSent;

  private static final int MessageFormat_Size_InBytes = 2;

  public AdminGetRequest(int correlationId, String clientId, MessageFormatFlags flags, BlobId blobId) {
    super(RequestOrResponseType.AdminGet, Request_Response_Version, correlationId, clientId);

    this.flags = flags;
    if (blobId == null) {
      throw new IllegalArgumentException("No blobId info specified in AdminGetRequest");
    }
    this.blobId = blobId;
    this.sizeSent = 0;
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public BlobId getBlobId() {
    return blobId;
  }

  public static AdminGetRequest readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.AdminGet;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    BlobId blobId = new BlobId(stream, clusterMap);
    // ignore version for now
    return new AdminGetRequest(correlationId, clientId, messageType, blobId);
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
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
    // header + message format size + Blob Id
    return super.sizeInBytes() + MessageFormat_Size_InBytes + blobId.sizeInBytes();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("AdminGetRequest[");
    sb.append(", ").append("MessageFormatFlags=").append(flags);
    sb.append(", ").append("BlobId=").append(blobId);
    sb.append("]");
    return sb.toString();
  }
}
