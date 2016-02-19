package com.github.ambry.router;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.WritableByteChannel;
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
   * This function is deprecated and will be removed in the future. The following documentation might not be up to date.
   * <p/>
   * Reads a sequence of bytes into the {@link WritableByteChannel} provided.
   * <p/>
   * This operation might read all the bytes in the channel or might not read any bytes at all. Its behavior depends
   * upon the nature and state of this ReadableStreamChannel and the {@link WritableByteChannel}. It is guaranteed,
   * however, that if this channel is in blocking mode, then this method will block until at least one byte is read into
   * the {@link WritableByteChannel}.
   * <p/>
   * This method may be invoked at any time.  However, if another thread has already initiated another read operation
   * upon this channel, an invocation of this method will block until the first operation is complete.
   * <p/>
   * When there is no more data left to read, a call to this function will return -1 (end of stream).
   * @param channel the {@link WritableByteChannel} to read into.
   * @return the actual number of bytes read (can be 0). If -1 is returned, there is no more data to read (end of
   *          stream).
   * @throws IOException if write to the {@link WritableByteChannel} failed or if any other I/O error occurred.
   */
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException;
}
