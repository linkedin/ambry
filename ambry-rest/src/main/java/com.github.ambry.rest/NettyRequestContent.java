package com.github.ambry.rest;

import com.github.ambry.rest.RestRequestContent;
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
    if (content == null) {
      throw new IllegalArgumentException("Received null HttpContent");
    }
    this.content = content;
    if (content instanceof LastHttpContent) {
      // LastHttpContent in the end marker in netty http world.
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
  public int getContentSize() {
    return content.content().capacity();
  }

  @Override
  public void getBytes(int srcIndex, byte[] dst, int dstIndex, int length) {
    content.content().getBytes(srcIndex, dst, dstIndex, length);
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
