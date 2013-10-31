package com.github.ambry.shared;

import com.github.ambry.network.Send;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Response to GetRequest to fetch data
 */
public class GetResponse extends RequestOrResponse {

  private Send toSend = null;
  private InputStream stream = null;

  public GetResponse(short versionId, int correlationId, Send send) {
    super(RequestResponseType.GetResponse, versionId, correlationId);
    this.toSend = send;
  }

  public InputStream getInputStream() {
    return stream;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) super.sizeInBytes());
      writeHeader();
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
    if (bufferToSend.remaining() == 0 && !toSend.isComplete()) {
      toSend.writeTo(channel);
    }
  }

  @Override
  public boolean isComplete() {
    return !(bufferToSend.remaining() > 0 || toSend.isComplete());
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + toSend.sizeInBytes();
  }
}
