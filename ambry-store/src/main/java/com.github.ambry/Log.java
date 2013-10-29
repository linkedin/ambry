package com.github.ambry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
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
  private static final String logFileName = "log_current";
  private Logger logger = LoggerFactory.getLogger(getClass());

  public Log(String dataDir) throws IOException {
    file = new File(dataDir, logFileName);
    fileChannel = Utils.openChannel(file, true);
    currentWriteOffset = new AtomicLong(0);
  }

  MessageReadSet getView(BlobReadOptions[] readOptions) throws IOException {
    return new BlobMessageReadSet(file, fileChannel, readOptions, currentWriteOffset.get());
  }

  public long sizeInBytes() {
    return currentWriteOffset.get();
  }

  public void setLogEndOffset(long endOffset) throws IOException {
    if (endOffset >= fileChannel.size()) {
      throw new IllegalArgumentException("fileEndPosition outside the file size");
    }
    fileChannel.position(endOffset);
    logger.trace("Setting log end offset {}", endOffset);
    this.currentWriteOffset.set(endOffset);
  }

  @Override
  public int appendFrom(ByteBuffer buffer) throws IOException {
    int bytesWritten = fileChannel.write(buffer, currentWriteOffset.get());
    currentWriteOffset.addAndGet(bytesWritten);
    logger.trace("Bytes appended to the log from bytebuffer for logfile {} byteswritten: {}",
            file.getPath(), bytesWritten);
    return bytesWritten;
  }

  @Override
  public long appendFrom(ReadableByteChannel channel, long size) throws IOException {
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
  public void readInto(ByteBuffer buffer , int position) throws IOException {
    fileChannel.read(buffer, position);
  }
}

