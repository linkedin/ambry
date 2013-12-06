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
public class Log implements Write, Read {

  private AtomicLong currentWriteOffset;
  private final FileChannel fileChannel;
  private final File file;
  private final StoreMetrics metrics;
  private static final String logFileName = "log_current";
  private Logger logger = LoggerFactory.getLogger(getClass());

  public Log(String dataDir, StoreMetrics metrics, long capacityGB) throws IOException {
    file = new File(dataDir, logFileName);
    if (!Utils.checkFileExistWithGivenSize(file, capacityGB)) {
      if (file.exists())
        throw new IllegalArgumentException("file exist but size " + capacityGB +
                                           " does not match input capacity " + capacityGB);
      // if the file does not exist, preallocate it
      Utils.preAllocateFile(file, capacityGB);
      // check again to ensure it is created correctly
      if (!Utils.checkFileExistWithGivenSize(file, capacityGB)) {
        throw new IllegalArgumentException("Existing file size on disk " + file.length() +
                                           "does not match capacityGB " + capacityGB);
      }
    }
    fileChannel = Utils.openChannel(file, true);
    currentWriteOffset = new AtomicLong(0);
    this.metrics = metrics;
  }

  MessageReadSet getView(List<BlobReadOptions> readOptions) throws IOException {
    return new BlobMessageReadSet(file, fileChannel, readOptions, currentWriteOffset.get());
  }

  public long sizeInBytes() {
    return currentWriteOffset.get();
  }

  public void setLogEndOffset(long endOffset) throws IOException {
    if (endOffset < 0 || endOffset > fileChannel.size()) {
      logger.error("endOffset {} outside the file size {}", endOffset, fileChannel.size());
      throw new IllegalArgumentException("endOffset " + endOffset + " outside the file size " + fileChannel.size());
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
    if (currentWriteOffset.get() + buffer.remaining() > fileChannel.size()) {
      metrics.overflowWriteError.inc(1);
      logger.error("Error trying to append to log from buffer since new data size {} exceeds log size {}",
                   buffer.remaining(), currentWriteOffset.get());
      throw new IllegalArgumentException("Error trying to append to log from buffer since new data size " +
                                         buffer.remaining() + "exceeds log size " + currentWriteOffset.get());
    }
    int bytesWritten = fileChannel.write(buffer, currentWriteOffset.get());
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Bytes appended to the log from bytebuffer for logfile {} byteswritten: {}",
                 file.getPath(), bytesWritten);
    return bytesWritten;
  }

  @Override
  public long appendFrom(ReadableByteChannel channel, long size) throws IOException {
    if (currentWriteOffset.get() + size > fileChannel.size()) {
      metrics.overflowWriteError.inc(1);
      logger.error("Error trying to append to log from channel since new data size {} exceeds log size {}",
                   size, currentWriteOffset.get());
      throw new IllegalArgumentException("Error trying to append to log from channel since new data size " +
                                         size + "exceeds log size " + currentWriteOffset.get());
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
  }

  public void flush() throws IOException {
    fileChannel.force(true);
  }

  @Override
  public void readInto(ByteBuffer buffer , long position) throws IOException {
    if (currentWriteOffset.get() < position || (position + buffer.remaining() > currentWriteOffset.get())) {
      logger.error("Error trying to read outside the log range. log end position {} input buffer size {}",
                   currentWriteOffset.get(), buffer.remaining());
      throw new IllegalArgumentException("Error trying to read outside the log range. log end position " +
                                         currentWriteOffset.get() + " input buffer size " + buffer.remaining());
    }
    fileChannel.read(buffer, position);
  }
}

