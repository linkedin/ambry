/**
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
package com.github.ambry.tools.perf.rest;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * A no-op implementation of {@link AsyncWritableChannel}.
 */
class NoOpAWC implements AsyncWritableChannel {
  private final static ExecutorService DISCARD_SERVICE = Executors.newSingleThreadExecutor();

  private long bytesConsumedTillNow = 0;
  private boolean isOpen = true;

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() {
    isOpen = false;
  }

  @Override
  public Future<Long> write(final ByteBuffer src, final Callback<Long> callback) {
    final FutureResult<Long> futureResult = new FutureResult<Long>();
    DISCARD_SERVICE.submit(new Runnable() {
      @Override
      public void run() {
        long bytesWritten = 0;
        Exception exception = null;
        if (!isOpen()) {
          exception = new ClosedChannelException();
        } else {
          bytesWritten = src.remaining();
          src.position(src.limit());
          bytesConsumedTillNow += bytesWritten;
        }
        futureResult.done(bytesWritten, exception);
        if (callback != null) {
          callback.onCompletion(bytesWritten, exception);
        }
      }
    });
    return futureResult;
  }

  long getBytesConsumedTillNow() {
    return bytesConsumedTillNow;
  }
}
