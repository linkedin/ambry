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

import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * The underlying file is backed by a log abstraction. This allows writes as append only operations.
 * For pre-allocated files, this tracks the end of valid file to ensure appends happen correctly.
 * It provides ability to read from arbitrary offset into the file. It can also provide a static view
 * of the log for a given set of offset,size pairs.
 */
public class Log implements Read, Write {

  private AtomicLong currentWriteOffset;
  private final FileChannel fileChannel;
  private final File file;
  private final long capacityInBytes;
  private static final String Log_File_Name = "log_current";
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final StoreMetrics metrics;

  public Log(String dataDir, long capacityInBytes, StoreMetrics metrics)
      throws IOException {
    file = new File(dataDir, Log_File_Name);
    if (!file.exists()) {
      // if the file does not exist, preallocate it
      Utils.preAllocateFileIfNeeded(file, capacityInBytes);
    }
    this.capacityInBytes = capacityInBytes;
    fileChannel = Utils.openChannel(file, true);
    logger.trace("Log : {} file size on start {} ", dataDir, fileChannel.size());
    // A log's write offset will always be set to the start of the log.
    // External components is responsible for setting it the right value
    currentWriteOffset = new AtomicLong(0);
    this.metrics = metrics;
  }

  StoreMessageReadSet getView(List<BlobReadOptions> readOptions)
      throws IOException {
    return new StoreMessageReadSet(file, fileChannel, readOptions, currentWriteOffset.get());
  }

  public long sizeInBytes()
      throws IOException {
    return fileChannel.size();
  }

  public void setLogEndOffset(long endOffset)
      throws IOException {
    long fileSize = fileChannel.size();
    if (endOffset < 0 || endOffset > fileSize) {
      throw new IllegalArgumentException("Log : " + file.getAbsolutePath() + " endOffset " + endOffset +
          " outside the file size " + fileSize);
    }
    fileChannel.position(endOffset);
    logger.trace("Log : {} setting log end offset {}", file.getAbsolutePath(), endOffset);
    this.currentWriteOffset.set(endOffset);
  }

  public long getLogEndOffset() {
    return currentWriteOffset.get();
  }

  @Override
  public int appendFrom(ByteBuffer buffer)
      throws IOException {
    if (currentWriteOffset.get() + buffer.remaining() > capacityInBytes) {
      metrics.overflowWriteError.inc(1);
      throw new IllegalArgumentException(
          "Log : " + file.getAbsolutePath() + " error trying to append to log from buffer since new data size " +
              buffer.remaining() + " exceeds total log size " + capacityInBytes);
    }
    int bytesWritten = fileChannel.write(buffer, currentWriteOffset.get());
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Log: {} bytes appended to the log from bytebuffer byteswritten : {}", file.getAbsolutePath(),
        bytesWritten);
    return bytesWritten;
  }

  @Override
  public void appendFrom(ReadableByteChannel channel, long size)
      throws IOException {
    logger.trace("Log : {} currentWriteOffset {} capacityInBytes {} sizeToAppend {}", file.getAbsolutePath(),
        currentWriteOffset, capacityInBytes, size);
    if (currentWriteOffset.get() + size > capacityInBytes) {
      metrics.overflowWriteError.inc(1);
      throw new IllegalArgumentException("Log : " + file.getAbsolutePath() + " error trying to append to log " +
          "from channel since new data size " + size + "exceeds total log size " + capacityInBytes);
    }
    long bytesWritten = 0;
    while (bytesWritten < size) {
      bytesWritten += fileChannel.transferFrom(channel, currentWriteOffset.get() + bytesWritten, size - bytesWritten);
    }
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Log : {} bytes appended to the log from read channel bytesWritten: {}", file.getAbsolutePath(),
        bytesWritten);
  }

  @Override
  public void writeFrom(ReadableByteChannel channel, long offset, long size)
      throws IOException {
    logger.trace("Log : {} currentWriteOffset {} capacityInBytes {} sizeToAppend {} offset to append at {}",
        file.getAbsolutePath(), currentWriteOffset, capacityInBytes, size, offset);
    if (offset < 0 || offset + size > currentWriteOffset.get()) {
      metrics.overflowWriteError.inc(1);
      throw new IllegalArgumentException("Log : " + file.getAbsolutePath() + " error trying to write to log " +
          "from channel since new data size " + size + "exceeds log end offset " + currentWriteOffset.get());
    }
    long bytesWritten = 0;
    while (bytesWritten < size) {
      bytesWritten += fileChannel.transferFrom(channel, offset + bytesWritten, size - bytesWritten);
    }
    logger.trace("Log : {} bytes written to the log from read channel at {}, bytesWritten: {}", file.getAbsolutePath(),
        offset, bytesWritten);
  }

  /**
   * Close this log
   */
  void close()
      throws IOException {
    fileChannel.close();
  }

  public void flush()
      throws IOException {
    fileChannel.force(true);
  }

  @Override
  public void readInto(ByteBuffer buffer, long position)
      throws IOException {
    if (sizeInBytes() < position || (position + buffer.remaining() > sizeInBytes())) {
      metrics.overflowReadError.inc(1);
      logger.error("Log: {} Error trying to read outside the log range. log end position {} input buffer size {}",
          file.getAbsolutePath(), sizeInBytes(), buffer.remaining());
      throw new IllegalArgumentException("Log : " + file.getAbsolutePath() + " error trying to read outside " +
          "the log range. log end position " + sizeInBytes() + " input buffer size " + buffer.remaining());
    }
    fileChannel.read(buffer, position);
  }
}

