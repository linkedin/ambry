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
package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a segment of a log. The segment is represented by its relative position in the log and the generation
 * number of the segment. Each segment knows the segment that "follows" it logically (if such a segment exists) and can
 * transparently redirect operations if required. A segment can be in one of the states represented by {@link State}.
 */
class LogSegment implements Read, Write {
  /**
   * Used to describe the state of the LogSegment.
   */
  enum State {
    /**
     * The LogSegment is completely free and no writes have been accommodated in the segment.
     */
    FREE,
    /**
     * The LogSegment is currently being written to. It still has capacity remaining for more writes.
     */
    ACTIVE,
    /**
     * The LogSegment is full and is accepting no more writes.
     */
    SEALED
  }

  private final FileChannel fileChannel;
  private final File file;
  private final long capacityInBytes;
  private final String name;
  private final Pair<File, FileChannel> segmentView;
  private final StoreMetrics metrics;
  private final AtomicLong endOffset;
  private final AtomicLong refCount = new AtomicLong(0);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * The state of the segment.
   * <p/>
   * A LogSegment once instantiated can only go from FREE -> ACTIVE -> SEALED in the natural course of operations, never
   * backward. However it can be externally manipulated to move in any direction.
   */
  State state;
  /**
   * Reference to the segment that logically "follows" this segment.
   */
  LogSegment next = null;

  /**
   * Creates a LogSegment abstraction.
   * @param name the desired name of the segment.
   * @param file the backing file for this segment.
   * @param capacityInBytes the intended capacity of the segment
   * @param metrics the {@link StoreMetrics} instance to use.
   * @throws IOException if the file cannot be read or created
   */
  LogSegment(String name, File file, long capacityInBytes, StoreMetrics metrics)
      throws IOException {
    if (!file.exists() || !file.isFile()) {
      throw new IllegalArgumentException(file.getAbsolutePath() + " does not exist or is not a file");
    }
    this.file = file;
    this.name = name;
    this.capacityInBytes = capacityInBytes;
    this.metrics = metrics;
    fileChannel = Utils.openChannel(file, true);
    segmentView = new Pair<>(file, fileChannel);
    // state is always initialized to FREE and the end offset is always initialized to 0.
    // externals will set the correct values for these two variables
    state = State.FREE;
    endOffset = new AtomicLong(0);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Guarantees that the {@code buffer} is either written in its entirety in this segment or is transparently forwarded
   * to the next segment (if one exists).
   * <p/>
   * Can cause state change from {@link State#FREE} -> {@link State#ACTIVE} if this is the first write to the segment.
   * Can cause state change from {@link State#ACTIVE} -> {@link State#SEALED} if this write cannot be accommodated in
   * the segment.
   * @param buffer The buffer from which data needs to be written from
   * @return the number of bytes written.
   * @throws IllegalArgumentException if there is not enough space for {@code buffer} and forwarding is not possible
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws UnsupportedOperationException if this LogSegment is intended to be swap space
   */
  @Override
  public int appendFrom(ByteBuffer buffer)
      throws IOException {
    int bytesWritten = 0;
    if (state.equals(State.SEALED)) {
      ensureNext(metrics.overflowWriteError);
      bytesWritten = next.appendFrom(buffer);
    } else if (endOffset.get() + buffer.remaining() > capacityInBytes) {
      // maintain the invariant that there are no "partial" writes. A log segment should write the data that it receives
      // completely and the data that was provided in a single call cannot be written across multiple segments.
      ensureNext(metrics.overflowWriteError);
      logger.info("{} : Rolling over writes to {} on write of data of size {} from a buffer. "
              + "Current end offset is {} and capacity is {}", file.getAbsolutePath(), next.getName(),
          buffer.remaining(), getEndOffset(), capacityInBytes);
      state = State.SEALED;
      bytesWritten = next.appendFrom(buffer);
    } else {
      state = State.ACTIVE;
      while (buffer.hasRemaining()) {
        bytesWritten += fileChannel.write(buffer, endOffset.get());
      }
      endOffset.addAndGet(bytesWritten);
    }
    return bytesWritten;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Guarantees that the {@code buffer} is either written in its entirety in this segment or is transparently forwarded
   * to the next segment (if one exists).
   * <p/>
   * Can cause state change from {@link State#FREE} -> {@link State#ACTIVE} if this is the first write to the segment.
   * Can cause state change from {@link State#ACTIVE} -> {@link State#SEALED} if this write cannot be accommodated in
   * the segment.
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws IllegalArgumentException if there is not enough space for {@code size} data and forwarding is not possible
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws UnsupportedOperationException if this LogSegment is intended to be swap space
   */
  @Override
  public void appendFrom(ReadableByteChannel channel, long size)
      throws IOException {
    if (state.equals(State.SEALED)) {
      ensureNext(metrics.overflowWriteError);
      next.appendFrom(channel, size);
    } else if (endOffset.get() + size > capacityInBytes) {
      // maintain the invariant that there are no "partial" writes. A log segment should write the data that it receives
      // completely and the data that was provided in a single call cannot be written across multiple segments.
      ensureNext(metrics.overflowWriteError);
      logger.info("{} : Rolling over writes to {} on write of data of size {} from a channel. "
              + "Current end offset is {} and capacity is {}", file.getAbsolutePath(), next.getName(), size,
          getEndOffset(), capacityInBytes);
      state = State.SEALED;
      next.appendFrom(channel, size);
    } else {
      state = State.ACTIVE;
      long bytesWritten = 0;
      while (bytesWritten < size) {
        bytesWritten += fileChannel.transferFrom(channel, endOffset.get() + bytesWritten, size - bytesWritten);
      }
      endOffset.addAndGet(bytesWritten);
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * If {@code offset} + {@code size} exceeds the capacity of the current segment, bytes are written starting from
   * {@code offset} until capacity is reached. The rest of the bytes upto {@code size} are forwarded to the next segment
   * (if one exists).
   * <p/>
   * The assumption with this function is that the caller is sure of the arguments and understands the implications of
   * writing at particular offsets and of writing to specific log segment.
   * <p/>
   * Using this function does not cause any state changes i.e. this function is expected to be used mostly for
   * overwriting and not for appending the log.
   * @param channel The channel from which data needs to be written from.
   * @param offset The offset at which to write in the underlying write interface.
   * @param size The amount of data in bytes to be written from the channel.
   * @throws IllegalArgumentException if {@code offset} < 0 or if there is not enough space for {@code offset } +
   * {@code size} data and forwarding is not possible
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws UnsupportedOperationException if this LogSegment is intended to be swap space or if it is supposed to free
   *
   */
  @Override
  public void writeFrom(ReadableByteChannel channel, long offset, long size)
      throws IOException {
    if (offset < 0 || offset > capacityInBytes) {
      throw new IllegalArgumentException(
          "Provided offset [" + offset + "] is out of bounds for the segment [" + file.getAbsolutePath()
              + "] with capacity [" + capacityInBytes + "]");
    }
    if (offset + size > capacityInBytes) {
      ensureNext(metrics.overflowWriteError);
      logger.info("{} : Write of data of size {} from offset {} from a channel will be rolled over "
              + "to the next segment {} because capacity is {}", file.getAbsolutePath(), next.getName(), size, offset,
          next.getName(), capacityInBytes);
    }
    long sizeToBeWritten = Math.min(size, capacityInBytes - offset);
    long sizeLeft = size - sizeToBeWritten;
    long bytesWritten = 0;
    while (bytesWritten < sizeToBeWritten) {
      bytesWritten += fileChannel.transferFrom(channel, offset + bytesWritten, sizeToBeWritten - bytesWritten);
    }
    if (offset + sizeToBeWritten > endOffset.get()) {
      endOffset.set(offset + sizeToBeWritten);
    }
    if (sizeLeft > 0) {
      next.writeFrom(channel, 0, sizeLeft);
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * If {@code position} + {@code buffer.remaining()} exceeds the capacity of the current segment, bytes are read
   * starting from {@code position} until capacity is reached. The rest of the bytes upto {@code buffer.remaining()} are
   * read from the next segment (if one exists).
   * <p/>
   * Using this function does not cause any state changes
   * @param buffer The buffer into which the read needs to write to
   * @param position The position to start the read from
   * @throws IllegalArgumentException if {@code position} < 0 or if {@code buffer} size is greater than the data
   * available for read.
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws UnsupportedOperationException if this LogSegment is intended to be swap space or if it is supposed to free
   */
  @Override
  public void readInto(ByteBuffer buffer, long position)
      throws IOException {
    if (position < 0 || position > getEndOffset()) {
      throw new IllegalArgumentException(
          "Provided position [" + position + "] is out of bounds for the segment [" + file.getAbsolutePath()
              + "] with end offset [" + getEndOffset() + "]");
    }
    if (position + buffer.remaining() > getEndOffset()) {
      ensureNext(metrics.overflowReadError);
      logger.info("{} : Read from position {} into a buffer of size {} when end offset is {} will continue "
              + "into the next segment {}", file.getAbsolutePath(), position, buffer.remaining(), getEndOffset(),
          next.getName());
    }
    long sizeToBeRead = Math.min(buffer.remaining(), getEndOffset() - position);
    long sizeLeft = buffer.remaining() - sizeToBeRead;
    long bytesRead = 0;
    int savedLimit = buffer.limit();
    buffer.limit(buffer.position() + (int) sizeToBeRead);
    try {
      while (bytesRead < sizeToBeRead) {
        bytesRead += fileChannel.read(buffer, position + bytesRead);
      }
    } finally {
      buffer.limit(savedLimit);
    }
    if (sizeLeft > 0) {
      next.readInto(buffer, 0);
    }
  }

  /**
   * Gets the {@link File} and {@link FileChannel} backing this log segment. Also increments a ref count.
   * <p/>
   * The expectation is that a matching {@link #closeView()} will be called eventually to decrement the ref count.
   * @param offset the offset that will used to start the read operation from.
   * @return the {@link File} and {@link FileChannel} backing this log segment.
   * @throws IllegalArgumentException if {@code offset} < 0 or is > {@link #getEndOffset()}.
   */
  Pair<File, FileChannel> getView(long offset) {
    if (offset < 0 || offset > getEndOffset()) {
      throw new IllegalArgumentException(
          "Provided offset [" + offset + "] is out of bounds for the segment [" + file.getAbsolutePath()
              + "] with end offset [" + getEndOffset() + "]");
    }
    refCount.incrementAndGet();
    return segmentView;
  }

  /**
   * Closes view that was obtained (decrements ref count).
   */
  void closeView() {
    refCount.decrementAndGet();
  }

  /**
   * @return size of the backing file on disk.
   * @throws IOException if the size could not be obtained due to I/O error.
   */
  long sizeInBytes()
      throws IOException {
    return fileChannel.size();
  }

  /**
   * @return the name of this segment.
   */
  String getName() {
    return name;
  }

  /**
   * Sets the end offset of this segment. This can be lesser than the actual size of the file and represents the offset
   * until which data that is readable is stored.
   * @param endOffset the end offset of this log.
   * @throws IllegalArgumentException if {@code endOffset} < 0 or {@code endOffset} > the size of the file.
   * @throws IOException if there is any I/O error.
   */
  void setEndOffset(long endOffset)
      throws IOException {
    long fileSize = sizeInBytes();
    if (endOffset < 0 || endOffset > fileSize) {
      throw new IllegalArgumentException(file.getAbsolutePath() + ": EndOffset [" + endOffset +
          "] outside the file size [" + fileSize + "]");
    }
    fileChannel.position(endOffset);
    this.endOffset.set(endOffset);
  }

  /**
   * @return the end offset of this log segment.
   */
  long getEndOffset() {
    return endOffset.get();
  }

  /**
   * @return the reference count of this log segment.
   */
  long refCount() {
    return refCount.get();
  }

  /**
   * Flushes the backing file to disk.
   * @throws IOException if there is an I/O error while flushing.
   */
  void flush()
      throws IOException {
    fileChannel.force(true);
  }

  /**
   * Closes this log segment
   */
  void close()
      throws IOException {
    fileChannel.close();
  }

  /**
   * Checks to make sure that {@link #next} is not {@code null}
   * @param errorCounter the error counter to increment if {@link #next} is {@code null}.
   * @throws IllegalStateException if {@link #next} is {@code null}
   */
  private void ensureNext(Counter errorCounter) {
    if (next == null) {
      errorCounter.inc();
      throw new IllegalStateException(file.getAbsolutePath() + ": Cannot perform operation beyond total capacity");
    }
  }
}
