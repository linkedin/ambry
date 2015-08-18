package com.github.ambry.network;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * A channel that represents a stream of bytes that can be read into different types of destinations. The read pointer
 * inside the channel is incremented regardless of the destination being written to and therefore the data read on any
 * read operation is exclusive.
 * <p/>
 * Only one read operation upon a ReadableStreamChannel may be in progress at any given time.  If one thread initiates a
 * read operation upon a channel then any other thread that attempts to initiate another read operation will block until
 * the first operation is complete.  Whether or not other kinds of I/O operations may proceed concurrently with a read
 * operation depends upon the type of the channel.
 */
public interface ReadableStreamChannel {

  /**
   * Reads a sequence of bytes into the {@link WritableByteChannel} provided.
   * <p/>
   * This operation might read all the bytes in the channel or might not read any bytes at all. It's behaviour depends
   * upon the nature and state of this ReadableStreamChannel and the {@link WritableByteChannel}. It is guaranteed,
   * however, that if a channel is in blocking mode, then this method will block until at least one byte is read into
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
  public int read(WritableByteChannel channel)
      throws IOException;
}
