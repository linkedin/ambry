package com.github.ambry.rest;

import com.github.ambry.restservice.RestContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;


/**
 * Netty specific implementation of RestContent
 * <p/>
 * Just a wrapper over HttpContent
 */
public class NettyContent implements RestContent {
  private final HttpContent content;
  private final boolean isLast;

  public boolean isLast() {
    return isLast;
  }

  public NettyContent(HttpContent content) {
    ReferenceCountUtil.retain(content);
    this.content = content;
    if (content instanceof LastHttpContent) {
      isLast = true;
    } else {
      isLast = false;
    }
  }

  public byte[] getBytes() {
    return content.content().array();
  }

  public void release() {
    // make sure we release the space we are using so that it can go back to the pool
    ReferenceCountUtil.release(content);
  }
}
