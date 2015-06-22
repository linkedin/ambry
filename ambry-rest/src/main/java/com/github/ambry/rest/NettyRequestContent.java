package com.github.ambry.rest;

import com.github.ambry.restservice.RestRequestContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;


/**
 * Netty specific implementation of {@link RestRequestContent}.
 * <p/>
 * Just a wrapper over {@link HttpContent}
 */
class NettyRequestContent implements RestRequestContent {
  private final HttpContent content;
  private final boolean isLast;

  public NettyRequestContent(HttpContent content) {
    this.content = content;
    if (content instanceof LastHttpContent) {
      // LastHttpContent in the end marker in netty http world
      isLast = true;
    } else {
      isLast = false;
    }
  }

  @Override
  public boolean isLast() {
    return isLast;
  }

  @Override
  public byte[] getBytes() {
    return content.content().array();
  }

  @Override
  public void retain() {
    ReferenceCountUtil.retain(content);
  }

  @Override
  public void release() {
    ReferenceCountUtil.release(content);
  }

  @Override
  public String toString() {
    return content.toString();
  }
}
