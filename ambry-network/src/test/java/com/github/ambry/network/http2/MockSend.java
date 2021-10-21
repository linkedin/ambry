package com.github.ambry.network.http2;

import com.github.ambry.network.Send;
import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * A mock {@link Send} implementation that returns non-null value for {@link #content()} method.
 */
public class MockSend extends AbstractByteBufHolder<MockSend> implements Send {
  protected ByteBuf buf;

  MockSend(ByteBuf content) {
    buf = content;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = channel.write(buf.nioBuffer());
    buf.skipBytes((int) written);
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return buf.readableBytes() == 0;
  }

  @Override
  public long sizeInBytes() {
    return 16;
  }

  @Override
  public ByteBuf content() {
    return buf;
  }

  @Override
  public MockSend replace(ByteBuf content) {
    return null;
  }
}
