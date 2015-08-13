package com.github.ambry.network;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;


/**
 * {@inheritDoc}
 * <p/>
 * In addition to the {@link #read(java.nio.ByteBuffer)} provided by {@link ReadableByteChannel}, this interface also
 * provides a {@link #writeTo(WritableByteChannel)} method that can directly write the data produced by this channel to
 * a {@link WritableByteChannel}.
 */
public interface ReadableChannel extends ReadableByteChannel {

  /**
   * Writes data to the {@link WritableByteChannel} provided. Writes all the data currently available for read on this
   * ReadableChannel.
   * <p/>
   * A writeTo operation might not write any bytes at all.  Whether or not it does so depends upon the nature and state
   * of this ReadableChannel.  A socket channel in non-blocking mode, for example, cannot read any more bytes than are
   * immediately available from the socket's input buffer; similarly, a file channel cannot read any more bytes than
   * remain in the file.  It is guaranteed, however, that if a channel is in blocking mode, then this method will block
   * until at least one byte is written to the {@link WritableByteChannel}.
   * <p/>
   * This method may be invoked at any time.  If another thread has  already initiated a
   * {@link #read(java.nio.ByteBuffer)} or writeTo operation upon this channel, however, then an invocation of this
   * method will block until the first operation is complete.
   * <p/>
   * Both {@link #read(java.nio.ByteBuffer)} and writeTo are considered read operations and therefore the data that they
   * read is exclusive i.e. the read pointer advances forward on a call to either of these functions.
   * <p/>
   * When there is no more data left to write, a call to this function will return -1 (end of stream).
   * @param channel the {@link WritableByteChannel} to write to
   * @return the actual number of bytes written (can be 0). If -1 is returned, there is no more data to write (end of
   *          stream).
   * @throws java.nio.channels.NonReadableChannelException if the channel has not been opened for reading
   * @throws java.nio.channels.ClosedChannelException if channel has already been closed
   * @throws IOException if write to the {@link WritableByteChannel} failed or if any other I/O error occurred.
   */
  public int writeTo(WritableByteChannel channel)
      throws IOException;
}
