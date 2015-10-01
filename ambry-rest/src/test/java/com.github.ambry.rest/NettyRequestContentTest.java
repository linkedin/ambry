package com.github.ambry.rest;

import com.github.ambry.utils.ByteBufferChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link NettyRequestContent}.
 */
public class NettyRequestContentTest {

  /**
   * This tests conversion of {@link io.netty.handler.codec.http.HttpContent} to {@link NettyRequestContent} given
   * good input.
   * @throws IOException
   */
  @Test
  public void conversionWithGoodInputTest()
      throws IOException {
    NettyRequestContent nettyContent;
    String contentStr = "SomeContent";
    ByteBuf content = Unpooled.copiedBuffer(contentStr.getBytes());
    boolean isLast = false;

    nettyContent = new NettyRequestContent(new DefaultHttpContent(content));
    validateContent(nettyContent, contentStr, isLast);

    isLast = true;
    nettyContent = new NettyRequestContent(new DefaultLastHttpContent(content));
    validateContent(nettyContent, contentStr, isLast);

    nettyContent = new NettyRequestContent(new DefaultLastHttpContent());
    validateContent(nettyContent, "", isLast);
  }

  /**
   * This tests conversion of {@link io.netty.handler.codec.http.HttpContent} to {@link NettyRequestContent} given bad
   * bad input (i.e. checks for the correct exception and {@link RestServiceErrorCode} if any).
   */
  @Test
  public void conversionWithBadInputTest() {
    // null input.
    try {
      new NettyRequestContent(null);
      fail("Provided null input to NettyRequestContent, yet it did not fail");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  // helpers
  // conversionWithGoodInputTest() helpers
  private void validateContent(NettyRequestContent nettyContent, String contentStr, boolean isLast)
      throws IOException {
    long contentSize = nettyContent.getSize();
    assertEquals("Content size mismatch", contentStr.length(), contentSize);
    ByteBuffer contentBuffer = ByteBuffer.allocate((int) nettyContent.getSize());
    nettyContent.read(new ByteBufferChannel(contentBuffer));
    assertEquals("Content mismatch", contentStr, new String(contentBuffer.array()));
    assertEquals("Last status mismatch", isLast, nettyContent.isLast());
  }
}
