package com.github.ambry.router;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.concurrent.Future;


/**
 * A channel that is used to perform writes asynchronously. The channel guarantees that all the data submitted to it
 * will either be acknowledged as successfully written or the reason for failure will be notified.
 * <p/>
 * The ordering in which data is submitted to the channel (even across multiple threads) is the order in which data
 * will be written to any underlying I/O channels or data structures.
 */
public interface ScheduledWriteChannel extends Channel {

  /**
   * The data in {@code src} will be eventually written to the channel and the {@code callback} will be invoked once the
   * write succeeds/fails. The {@code callback} and the future returned will contain the bytes written
   * (that should be equal to {@code src.remaining()} at the time of invocation if there were no exceptions) on success
   * or failure. If the write failed, they will also contain the exception that caused the failure.
   * <p/>
   * Every single write is guaranteed to be acknowledged as either succeeded or failed even if the channel is closed.
   * <p/>
   * {@code src} can be reused ony after the {@code callback} is invoked (or after {@code future.get()} returns).
   * <p/>
   * @param src the data that needs to be written to the channel.
   * @param callback the {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation.
   */
  public Future<Long> write(ByteBuffer src, Callback<Long> callback);
}
