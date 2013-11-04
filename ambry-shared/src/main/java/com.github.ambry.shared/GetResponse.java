package com.github.ambry.shared;

import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.Send;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

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

  public GetResponse(short versionId, int correlationId, InputStream stream) {
    super(RequestResponseType.GetResponse, versionId, correlationId);
    this.stream = stream;
  }

  public InputStream getInputStream() {
    return stream;
  }

  public static GetResponse readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();

    return new GetResponse(versionId, correlationId, stream);
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
    if (bufferToSend.remaining() == 0 && !toSend.isSendComplete()) {
      toSend.writeTo(channel);
    }
  }

  @Override
  public boolean isSendComplete() {
    return !(bufferToSend.remaining() > 0 || toSend.isSendComplete());
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + toSend.sizeInBytes();
  }
}
