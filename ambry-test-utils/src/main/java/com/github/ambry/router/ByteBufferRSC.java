/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
public class ByteBufferRSC implements ReadableStreamChannel {
  /**
   * List of "events" (function calls) that can occur inside ByteBufferRSC.
   */
  public enum Event {
    GetSize, ReadInto, SetDigestAlgorithm, GetDigest, IsOpen, Close
  }

  /**
   * Callback that can be used to listen to events that happen inside ByteBufferRSC.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside the function that notifies listeners.
   */
  public interface EventListener {

    /**
     * Called when an event (function call) finishes successfully in ByteBufferRSC. Does *not* trigger if the event
     * (function) fails.
     * @param byteBufferRSC the {@link ByteBufferRSC} where the event occurred.
     * @param event the {@link Event} that occurred.
     */
    public void onEventComplete(ByteBufferRSC byteBufferRSC, Event event);
  }

  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean channelEmptied = new AtomicBoolean(false);
  private final List<EventListener> listeners = new ArrayList<EventListener>();

  protected final ByteBuffer buffer;
  protected final int size;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buffer}.
   * @param buffer the {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferRSC(ByteBuffer buffer) {
    this.buffer = buffer;
    size = buffer.remaining();
  }

  @Override
  public long getSize() {
    onEventComplete(Event.GetSize);
    return size;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    Future<Long> future;
    if (!channelOpen.get()) {
      ClosedChannelException closedChannelException = new ClosedChannelException();
      FutureResult<Long> futureResult = new FutureResult<Long>();
      futureResult.done(0L, closedChannelException);
      future = futureResult;
      if (callback != null) {
        callback.onCompletion(0L, closedChannelException);
      }
    } else if (!channelEmptied.compareAndSet(false, true)) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    } else {
      future = asyncWritableChannel.write(buffer, callback);
    }
    onEventComplete(Event.ReadInto);
    return future;
  }

  @Override
  public boolean isOpen() {
    onEventComplete(Event.IsOpen);
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    channelOpen.set(false);
    onEventComplete(Event.Close);
  }

  /**
   * Register to be notified about events that occur in this ByteBufferRSC.
   * @param listener the listener that needs to be notified of events.
   */
  public ByteBufferRSC addListener(EventListener listener) {
    if (listener != null) {
      synchronized (listeners) {
        listeners.add(listener);
      }
    }
    return this;
  }

  /**
   * Notify listeners of events.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside this function.
   * @param event the {@link Event} that just occurred.
   */
  private void onEventComplete(Event event) {
    synchronized (listeners) {
      for (EventListener listener : listeners) {
        try {
          listener.onEventComplete(this, event);
        } catch (Exception ee) {
          // too bad.
        }
      }
    }
  }
}

