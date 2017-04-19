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

import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents a segment of a log. The segment is represented by its relative position in the log and the generation
 * number of the segment. Each segment knows the segment that "follows" it logically (if such a segment exists) and can
 * transparently redirect operations if required.
 */
class LogSegment implements Read, Write {
  private static final short VERSION = 0;
  private static final int VERSION_HEADER_SIZE = 2;
  private static final int CAPACITY_HEADER_SIZE = 8;
  private static final int CRC_SIZE = 8;

  static final int HEADER_SIZE = VERSION_HEADER_SIZE + CAPACITY_HEADER_SIZE + CRC_SIZE;

  private final FileChannel fileChannel;
  private final File file;
  private final long capacityInBytes;
  private final String name;
  private final Pair<File, FileChannel> segmentView;
  private final StoreMetrics metrics;
  private final long startOffset;
  private final AtomicLong endOffset;
  private final AtomicLong refCount = new AtomicLong(0);
  private final AtomicBoolean open = new AtomicBoolean(true);

  /**
   * Creates a LogSegment abstraction with the given capacity.
   * @param name the desired name of the segment. The name signifies the handle/ID of the LogSegment and may be
   *             different from the filename of the {@code file}.
   * @param file the backing {@link File} for this segment.
   * @param capacityInBytes the intended capacity of the segment
   * @param metrics the {@link StoreMetrics} instance to use.
   * @param writeHeader if {@code true}, headers are written that provide metadata about the segment.
   * @throws IOException if the file cannot be read or created
   */
  LogSegment(String name, File file, long capacityInBytes, StoreMetrics metrics, boolean writeHeader)
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
    // externals will set the correct value of end offset.
    endOffset = new AtomicLong(0);
    if (writeHeader) {
      // this will update end offset
      writeHeader(capacityInBytes);
    }
    startOffset = endOffset.get();
  }

  /**
   * Creates a LogSegment abstraction with the given file. Obtains capacity from the headers in the file.
   * @param name the desired name of the segment. The name signifies the handle/ID of the LogSegment and may be
   *             different from the filename of the {@code file}.
   * @param file the backing {@link File} for this segment.
   * @param metrics he {@link StoreMetrics} instance to use.
   * @throws IOException
   */
  LogSegment(String name, File file, StoreMetrics metrics) throws IOException {
    if (!file.exists() || !file.isFile()) {
      throw new IllegalArgumentException(file.getAbsolutePath() + " does not exist or is not a file");
    }
    // TODO: just because the file exists, it does not mean the headers have been written into it. LogSegment should
    // TODO: be able to handle this situation.
    CrcInputStream crcStream = new CrcInputStream(new FileInputStream(file));
    try (DataInputStream stream = new DataInputStream(crcStream)) {
      switch (stream.readShort()) {
        case 0:
          capacityInBytes = stream.readLong();
          long computedCrc = crcStream.getValue();
          long crcFromFile = stream.readLong();
          if (crcFromFile != computedCrc) {
            throw new IllegalStateException("CRC from the segment file does not match computed CRC of header");
          }
          startOffset = HEADER_SIZE;
          break;
        default:
          throw new IllegalArgumentException("Unknown version in segment [" + file.getAbsolutePath() + "]");
      }
    }
    this.file = file;
    this.name = name;
    this.metrics = metrics;
    fileChannel = Utils.openChannel(file, true);
    segmentView = new Pair<>(file, fileChannel);
    // externals will set the correct value of end offset.
    endOffset = new AtomicLong(startOffset);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Attempts to write the {@code buffer} in its entirety in this segment. To guarantee that the write is persisted,
   * {@link #flush()} has to be called.
   * <p/>
   * The write is not started if it cannot be completed.
   * @param buffer The buffer from which data needs to be written from
   * @return the number of bytes written.
   * @throws IllegalArgumentException if there is not enough space for {@code buffer}
   * @throws IOException if data could not be written to the file because of I/O errors
   */
  @Override
  public int appendFrom(ByteBuffer buffer) throws IOException {
    int bytesWritten = 0;
    if (endOffset.get() + buffer.remaining() > capacityInBytes) {
      metrics.overflowWriteError.inc();
      throw new IllegalArgumentException(
          "Buffer cannot be written to segment [" + file.getAbsolutePath() + "] because " + "it exceeds the capacity ["
              + capacityInBytes + "]");
    } else {
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
   * Attempts to write the {@code channel} in its entirety in this segment. To guarantee that the write is persisted,
   * {@link #flush()} has to be called.
   * <p/>
   * The write is not started if it cannot be completed.
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws IllegalArgumentException if there is not enough space for data of size {@code size}.
   * @throws IOException if data could not be written to the file because of I/O errors
   */
  @Override
  public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
    if (endOffset.get() + size > capacityInBytes) {
      metrics.overflowWriteError.inc();
      throw new IllegalArgumentException(
          "Channel cannot be written to segment [" + file.getAbsolutePath() + "] because" + " it exceeds the capacity ["
              + capacityInBytes + "]");
    } else {
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
   * The read is not started if it cannot be completed.
   * @param buffer The buffer into which the data needs to be written
   * @param position The position to start the read from
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws IndexOutOfBoundsException if {@code position} < header size or >= {@link #sizeInBytes()} or if
   * {@code buffer} size is greater than the data available for read.
   */
  @Override
  public void readInto(ByteBuffer buffer, long position) throws IOException {
    long sizeInBytes = sizeInBytes();
    if (position < startOffset || position >= sizeInBytes) {
      throw new IndexOutOfBoundsException(
          "Provided position [" + position + "] is out of bounds for the segment [" + file.getAbsolutePath()
              + "] with size [" + sizeInBytes + "]");
    }
    if (position + buffer.remaining() > sizeInBytes) {
      metrics.overflowReadError.inc();
      throw new IndexOutOfBoundsException(
          "Cannot read from segment [" + file.getAbsolutePath() + "] from position [" + position + "] for size ["
              + buffer.remaining() + "] because it exceeds the size [" + sizeInBytes + "]");
    }
    long bytesRead = 0;
    int size = buffer.remaining();
    while (bytesRead < size) {
      bytesRead += fileChannel.read(buffer, position + bytesRead);
    }
  }

  /**
   * Writes {@code size} number of bytes from the channel {@code channel} into the segment at {@code offset}.
   * <p/>
   * The write is not started if it cannot be completed.
   * @param channel The channel from which data needs to be written from.
   * @param offset The offset in the segment at which to start writing.
   * @param size The amount of data in bytes to be written from the channel.
   * @throws IOException if data could not be written to the file because of I/O errors
   * @throws IndexOutOfBoundsException if {@code offset} < header size or if there is not enough space for
   * {@code offset } + {@code size} data.
   *
   */
  void writeFrom(ReadableByteChannel channel, long offset, long size) throws IOException {
    if (offset < startOffset || offset >= capacityInBytes) {
      throw new IndexOutOfBoundsException(
          "Provided offset [" + offset + "] is out of bounds for the segment [" + file.getAbsolutePath()
              + "] with capacity [" + capacityInBytes + "]");
    }
    if (offset + size > capacityInBytes) {
      metrics.overflowWriteError.inc();
      throw new IndexOutOfBoundsException(
          "Cannot write to segment [" + file.getAbsolutePath() + "] from offset [" + offset + "] for size [" + size
              + "] because it exceeds the capacity [" + capacityInBytes + "]");
    }
    long bytesWritten = 0;
    while (bytesWritten < size) {
      bytesWritten += fileChannel.transferFrom(channel, offset + bytesWritten, size - bytesWritten);
    }
    if (offset + size > endOffset.get()) {
      endOffset.set(offset + size);
    }
  }

  /**
   * Gets the {@link File} and {@link FileChannel} backing this log segment. Also increments a ref count.
   * <p/>
   * The expectation is that a matching {@link #closeView()} will be called eventually to decrement the ref count.
   * @return the {@link File} and {@link FileChannel} backing this log segment.
   */
  Pair<File, FileChannel> getView() {
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
  long sizeInBytes() throws IOException {
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
   * until which data that is readable is stored (exclusive) and the offset (inclusive) from which the next append will
   * begin.
   * @param endOffset the end offset of this log.
   * @throws IllegalArgumentException if {@code endOffset} < header size or {@code endOffset} > the size of the file.
   * @throws IOException if there is any I/O error.
   */
  void setEndOffset(long endOffset) throws IOException {
    long fileSize = sizeInBytes();
    if (endOffset < startOffset || endOffset > fileSize) {
      throw new IllegalArgumentException(
          file.getAbsolutePath() + ": EndOffset [" + endOffset + "] outside the file size [" + fileSize + "]");
    }
    fileChannel.position(endOffset);
    this.endOffset.set(endOffset);
  }

  /**
   * @return the offset in this log segment from which there is valid data.
   */
  long getStartOffset() {
    return startOffset;
  }

  /**
   * @return the offset in this log segment until which there is valid data.
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
   * @return the total capacity, in bytes, of this log segment.
   */
  long getCapacityInBytes() {
    return capacityInBytes;
  }

  /**
   * Flushes the backing file to disk.
   * @throws IOException if there is an I/O error while flushing.
   */
  void flush() throws IOException {
    fileChannel.force(true);
  }

  /**
   * Closes this log segment
   */
  void close() throws IOException {
    if (open.compareAndSet(true, false)) {
      flush();
      fileChannel.close();
    }
  }

  /**
   * Writes a header describing the segment.
   * @param capacityInBytes the intended capacity of the segment.
   * @throws IOException if there is any I/O error writing to the file.
   */
  private void writeHeader(long capacityInBytes) throws IOException {
    Crc32 crc = new Crc32();
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
    buffer.putShort(VERSION);
    buffer.putLong(capacityInBytes);
    crc.update(buffer.array(), 0, HEADER_SIZE - CRC_SIZE);
    buffer.putLong(crc.getValue());
    buffer.flip();
    appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), buffer.remaining());
  }

  @Override
  public String toString() {
    return "(File: [" + file + " ], Capacity: [" + capacityInBytes + "], Start offset: [" + startOffset
        + "], End offset: [" + endOffset + "])";
  }
}
