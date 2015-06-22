package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


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

  // helpers
  // conversionWithGoodInputTest() helpers
  private void validateContent(NettyRequestContent nettyContent, String contentStr, boolean isLast) {
    assertEquals("Content mismatch", contentStr, new String(nettyContent.getBytes()));
    assertEquals("Last status mismatch", isLast, nettyContent.isLast());
  }
}
