package com.github.ambry.protocol;

import com.github.ambry.commons.ServerErrorCode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Response for deserialization.
 */
public abstract class Response extends RequestOrResponse {
  private ServerErrorCode error;
  private static final int Error_Size_InBytes = 2;

  public Response(RequestOrResponseType type, short requestResponseVersion, int correlationId, String clientId,
      ServerErrorCode error) {
    super(type, requestResponseVersion, correlationId, clientId);
    this.error = error;
  }

  public ServerErrorCode getError() {
    return error;
  }

  @Override
  protected void writeHeader() {
    super.writeHeader();
    bufferToSend.putShort((short) error.ordinal());
  }

  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return (bufferToSend == null || bufferToSend.remaining() == 0);
  }

  @Override
  public long sizeInBytes() {
    // header + error
    return super.sizeInBytes() + Error_Size_InBytes;
  }
}
