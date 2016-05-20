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
package com.github.ambry.router;

import java.nio.channels.Channel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Future;


/**
 * A channel that represents a stream of bytes that can be read into different types of destinations asynchronously.
 * <p/>
 * In most implementations, the channel likely can be used for only one read operation after which it cannot be reused.
 * If more than one thread invokes read operations at the same time, only one of them may succeed and the other read
 * operations may be rejected.
 */
public interface ReadableStreamChannel extends Channel {

  /**
   * Return the size of stream that is available on this channel. If -1, then size is unknown.
   * @return the size of the stream available on this channel. -1 if size is unknown.
   */
  public long getSize();

  /**
   * Reads all the data inside this channel into the given {@code asyncWritableChannel} asynchronously. The
   * {@code callback} will be invoked once the read is complete. The {@code callback} and the future returned will
   * contain the bytes read (that should be equal to the size of the channel if there were no exceptions) on success
   * or failure. If the read failed, they will also contain the exception that caused the failure.
   * <p/>
   * It is guaranteed that a read will be acknowledged as either a success or failure.
   * @param asyncWritableChannel the {@link AsyncWritableChannel} to read the data into.
   * @param callback the {@link Callback} that will be invoked either when all the data in the channel has been emptied
   *                 into the {@code asyncWritableChannel} or if there is an exception in doing so. This can be null.
   * @return the {@link Future} that will eventually contain the result of the operation.
   */
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback);

  /**
   * Set the digest algorithm to use on the data that is being streamed from the channel. Once the channel is emptied,
   * the digest can be obtained via {@link #getDigest()}.
   * <p/>
   * This function is ideally called before {@link #readInto(AsyncWritableChannel, Callback)}. After a call to
   * {@link #readInto(AsyncWritableChannel, Callback)}, some content may have been discarded and getting a digest may
   * longer be possible. The safety of doing otherwise depends on the implementation.
   * @param digestAlgorithm the digest algorithm to use.
   * @throws NoSuchAlgorithmException if the {@code digestAlgorithm} does not exist or is not supported.
   * @throws IllegalStateException if {@link #readInto(AsyncWritableChannel, Callback)} has already been called.
   */
  public void setDigestAlgorithm(String digestAlgorithm)
      throws NoSuchAlgorithmException;

  /**
   * Gets the digest as specified by the digest algorithm set through {@link #setDigestAlgorithm(String)}. If none was
   * set, returns {@code null}.
   * <p/>
   * This function is ideally called after the channel is emptied completely. Otherwise, the complete digest may not
   * have been calculated yet. The safety of doing otherwise depends on the implementation.
   * <p/>
   * "Emptying a channel" refers to awaiting on the future or getting the callback after a
   * {@link #readInto(AsyncWritableChannel, Callback)} call.
   * @return the digest as specified by the digest algorithm set through {@link #setDigestAlgorithm(String)}. If none
   * was set, {@code null}.
   * @throws IllegalStateException if called before the channel has been emptied.
   */
  public byte[] getDigest();
}
