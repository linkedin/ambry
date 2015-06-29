package com.github.ambry.restservice;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link NettyRequestContent}.
 */
public class NettyRequestContentTest {

  /**
   * This tests conversion of {@link io.netty.handler.codec.http.HttpContent} into {@link NettyRequestContent} given
   * good input.
   */
  @Test
  public void conversionWithGoodInputTest() {
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
  private void validateContent(NettyRequestContent nettyContent, String contentStr, boolean isLast) {
    int contentSize = nettyContent.getContentSize();
    assertEquals("Content size mismatch", contentStr.length(), contentSize);
    byte[] contentBytes = new byte[contentSize];
    nettyContent.getBytes(0, contentBytes, 0, contentSize);
    assertEquals("Content mismatch", contentStr, new String(contentBytes));
    assertEquals("Last status mismatch", isLast, nettyContent.isLast());
  }
}
