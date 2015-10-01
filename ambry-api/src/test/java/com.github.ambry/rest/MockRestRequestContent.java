package com.github.ambry.rest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestRequestContent} that be used in tests.
 * <p/>
 * The underlying data is in the form of a {@link JSONObject} that has the following fields: -
 * "content" - (object that should be serializable to String) - the actual content.
 * "isLast" - boolean - true if this the last content (end marker), false otherwise.
 */
public class MockRestRequestContent implements RestRequestContent {
  public static String CONTENT_KEY = "content";
  public static String IS_LAST_KEY = "isLast";

  private final boolean isLast;
  private final ByteBuffer contentBuffer;

  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock referenceCountLock = new ReentrantLock();
  private final AtomicInteger referenceCount = new AtomicInteger(0);

  public MockRestRequestContent(JSONObject data)
      throws JSONException {
    contentBuffer = ByteBuffer.wrap(data.get(CONTENT_KEY).toString().getBytes());
    isLast = data.getBoolean(IS_LAST_KEY);
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
    try {
      referenceCountLock.lock();
      if (isOpen()) {
        referenceCount.incrementAndGet();
      } else {
        // this is specifically in place so that we know when a piece of code retains after close. Real implementations
        // might (or might not) quietly handle this but in tests we want to know when we retain after close.
        throw new IllegalStateException("Trying to retain content after closing channel");
      }
    } finally {
      referenceCountLock.unlock();
    }
  }

  @Override
  public void release() {
    try {
      referenceCountLock.lock();
      if (referenceCount.get() <= 0) {
        // this is specifically in place so that we know when a piece of code releases too much. Real implementations
        // might (or might not) quietly handle this but in tests we want to know when we release too much.
        throw new IllegalStateException("Content has been released more times than it has been retained");
      } else {
        referenceCount.decrementAndGet();
      }
    } finally {
      referenceCountLock.unlock();
    }
  }
}
