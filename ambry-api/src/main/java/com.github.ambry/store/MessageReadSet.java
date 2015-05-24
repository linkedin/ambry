package com.github.ambry.store;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * This represents a list of messages from a given store.
 */
public interface MessageReadSet {

  /**
   * Write the message referred by the index in this set to the given channel from the given relative offset
   * up to maxSize. Less than the complete amount may be written, but no more than maxSize can be. The number
   * of bytes written is returned
   * @param index the index of the message in the set that needs to be written
   * @param channel the channel into which the data needs to be written to
   * @param relativeOffset The relative offset into the message from which the write needs to start
   * @param maxSize The max size that needs to be written from the given message
   * @return The total bytes that was written into the channel
   * @throws IOException
   */
  long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize)
      throws IOException;

  /**
   * Returns the total number of messages in this set
   * @return The total number of messages in this set
   */
  int count();

  /**
   * Returns the size of the ith message
   * @param index The index into the message set
   * @return The size in bytes of the message represented by the index
   */
  long sizeInBytes(int index);

  /**
   * Returns the key of the ith message
   * @param index The index into the message set
   * @return The key of the message represented by the index
   */
  StoreKey getKeyAt(int index);

  /** Given a relative offset in the message, return the max that is left to read bounded by maxSize
   *
   * @param relativeOffset the relative offset in the message.
   * @param maxSize the maximum size to return.
   * @return return the max bytes that can be read starting from the relative offset, or the maxSize whichever is less.
   */
  long getReadableSize(int index, long relativeOffset, long maxSize);
}
