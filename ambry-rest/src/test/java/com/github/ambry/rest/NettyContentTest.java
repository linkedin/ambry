package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Tests functionality of NettyContent
 */
public class NettyContentTest {

  /**
   * This tests conversion of HttpContent instances given good input.
   */
  @Test
  public void conversionWithGoodInputTest() {
    NettyContent nettyContent;
    String contentStr = "SomeContent";
    ByteBuf content = Unpooled.copiedBuffer(contentStr.getBytes());
    boolean isLast = false;

    nettyContent = new NettyContent(new DefaultHttpContent(content));
    validateContent(nettyContent, contentStr, isLast);

    isLast = true;
    nettyContent = new NettyContent(new DefaultLastHttpContent(content));
    validateContent(nettyContent, contentStr, isLast);

    nettyContent = new NettyContent(new DefaultLastHttpContent());
    validateContent(nettyContent, "", isLast);
  }

  // helpers
  // conversionWithGoodInputTest() helpers
  private void validateContent(NettyContent nettyContent, String contentStr, boolean isLast) {
    assertEquals("Content mismatch", contentStr, new String(nettyContent.getBytes()));
    assertEquals("Last status mismatch", isLast, nettyContent.isLast());
  }
}
