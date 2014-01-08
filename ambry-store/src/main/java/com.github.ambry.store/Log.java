package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
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
public class Log implements Write, Read {

  private AtomicLong currentWriteOffset;
  private final FileChannel fileChannel;
  private final FileInputStream readOnlyStream;
  private final File file;
  private final StoreMetrics metrics;
  private final long capacityGB;
  private static final String Log_File_Name = "log_current";
  private Logger logger = LoggerFactory.getLogger(getClass());

  public Log(String dataDir, StoreMetrics metrics, long capacityGB) throws IOException {
    file = new File(dataDir, Log_File_Name);
    if (!file.exists()) {
      // if the file does not exist, preallocate it
      Utils.preAllocateFileIfNeeded(file, capacityGB);
    }
    this.capacityGB = capacityGB;
    fileChannel = Utils.openChannel(file, true);
    readOnlyStream = new FileInputStream(file);
    // A log's write offset will always be set to the start of the log.
    // External components is responsible for setting it the right value
    currentWriteOffset = new AtomicLong(0);
    this.metrics = metrics;
  }

  MessageReadSet getView(List<BlobReadOptions> readOptions) throws IOException {
    return new BlobMessageReadSet(file, fileChannel, readOptions, currentWriteOffset.get());
  }

  public long sizeInBytes() throws IOException {
    return fileChannel.size();
  }

  public void setLogEndOffset(long endOffset) throws IOException {
    if (endOffset < 0 || endOffset > capacityGB) {
      logger.error("endOffset {} outside the file size {}", endOffset, capacityGB);
      throw new IllegalArgumentException("endOffset " + endOffset + " outside the file size " + capacityGB);
    }
    fileChannel.position(endOffset);
    logger.trace("Setting log end offset {}", endOffset);
    this.currentWriteOffset.set(endOffset);
  }

  public long getLogEndOffset() {
    return currentWriteOffset.get();
  }

  @Override
  public int appendFrom(ByteBuffer buffer) throws IOException {
    if (currentWriteOffset.get() + buffer.remaining() > capacityGB) {
      metrics.overflowWriteError.inc(1);
      logger.error("Error trying to append to log from buffer since new data size {} exceeds total log size {}",
                   buffer.remaining(), capacityGB);
      throw new IllegalArgumentException("Error trying to append to log from buffer since new data size " +
                                         buffer.remaining() + " exceeds total log size " + capacityGB);
    }
    int bytesWritten = fileChannel.write(buffer, currentWriteOffset.get());
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Bytes appended to the log from bytebuffer for logfile {} byteswritten: {}",
                 file.getPath(), bytesWritten);
    return bytesWritten;
  }

  @Override
  public long appendFrom(ReadableByteChannel channel, long size) throws IOException {
    if (currentWriteOffset.get() + size > capacityGB) {
      metrics.overflowWriteError.inc(1);
      logger.error("Error trying to append to log from channel since new data size {} exceeds total log size {}",
                   size, capacityGB);
      throw new IllegalArgumentException("Error trying to append to log from channel since new data size " +
                                         size + "exceeds total log size " + capacityGB);
    }
    long bytesWritten = fileChannel.transferFrom(channel, currentWriteOffset.get(), size);
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Bytes appended to the log from read channel for logfile {} byteswritten: {}",
                 file.getPath(), bytesWritten);
    return bytesWritten;
  }

  /**
   * Close this log
   */
  void close() throws IOException {
    fileChannel.close();
    readOnlyStream.close();
  }

  public void flush() throws IOException {
    fileChannel.force(true);
  }

  @Override
  public void readInto(ByteBuffer buffer , long position) throws IOException {
    if (sizeInBytes() < position || (position + buffer.remaining() > sizeInBytes())) {
      logger.error("Error trying to read outside the log range. log end position {} input buffer size {}",
                   sizeInBytes(), buffer.remaining());
      throw new IllegalArgumentException("Error trying to read outside the log range. log end position " +
                                         sizeInBytes() + " input buffer size " + buffer.remaining());
    }
    fileChannel.read(buffer, position);
  }
}

