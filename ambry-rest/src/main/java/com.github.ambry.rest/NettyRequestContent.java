package com.github.ambry.rest;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestRequestContent}.
 * <p/>
 * Just a wrapper over {@link HttpContent}
 */
class NettyRequestContent implements RestRequestContent {
  private final HttpContent content;
  private final ByteBuffer contentBuffer;
  private final boolean isLast;

  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock referenceCountLock = new ReentrantLock();
  // We are maintaining a count here for two reasons: -
  // 1. To release when we close.
  // 2. To avoid misplaced errors i.e content was actually released too many times here but the reference count went
  //      negative somewhere else so the error was thrown at that point.
  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Wraps the {@code content} in an implementation of {@link RestRequestContent} so that other layers can understand
   * the content.
   * @param content the {@link HttpContent} that needs to be wrapped.
   * @throws IllegalArgumentException if {@code content} is null.
   */
  public NettyRequestContent(HttpContent content) {
    if (content == null) {
      throw new IllegalArgumentException("Received null HttpContent");
    } else if (content.content().nioBufferCount() > 0) {
      // not a copy.
      contentBuffer = content.content().nioBuffer();
      this.content = content;
    } else {
      // this usually will not happen, but if it does, we cannot avoid a copy.
      // or TODO: we can introduce a read(GatheringByteChannel) method in ReadableStreamChannel.
      logger.warn("Http content had to be copied because ByteBuf did not have a backing ByteBuffer");
      contentBuffer = ByteBuffer.allocate(content.content().capacity());
      content.content().readBytes(contentBuffer);
      // no need to retain content since we have a copy.
      this.content = null;
    }

    // LastHttpContent in the end marker in netty http world.
    isLast = content instanceof LastHttpContent;
  }

  @Override
  public String toString() {
    return contentBuffer.toString();
  }

  @Override
  public long getSize() {
    return contentBuffer.capacity();
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = -1;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else {
      try {
        bufferReadLock.lock();
        if (contentBuffer.hasRemaining()) {
          bytesWritten = channel.write(contentBuffer);
        }
      } finally {
        bufferReadLock.unlock();
      }
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      try {
        bufferReadLock.lock();
        while (referenceCount.get() > 0) {
          release();
        }
      } finally {
        bufferReadLock.unlock();
      }
    }
  }

  @Override
  public boolean isLast() {
    return isLast;
  }

  @Override
  public void retain() {
    if (content != null) {
      try {
        referenceCountLock.lock();
        if (isOpen()) {
          ReferenceCountUtil.retain(content);
          referenceCount.incrementAndGet();
        }
      } finally {
        referenceCountLock.unlock();
      }
    }
  }

  @Override
  public void release() {
    if (content != null) {
      try {
        referenceCountLock.lock();
        if (referenceCount.get() > 0) {
          ReferenceCountUtil.release(content);
          referenceCount.decrementAndGet();
        }
      } finally {
        referenceCountLock.unlock();
      }
    }
  }
}
