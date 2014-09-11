package com.github.ambry.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;


/**
 * A read option class that maintains the offset and size
 */
class BlobReadOptions implements Comparable<BlobReadOptions> {
  private final Long offset;
  private final Long size;
  private final Long ttl;
  private final StoreKey storeKey;
  private Logger logger = LoggerFactory.getLogger(getClass());

  BlobReadOptions(long offset, long size, long ttl, StoreKey storeKey) {
    this.offset = offset;
    this.size = size;
    this.ttl = ttl;
    this.storeKey = storeKey;
    logger.trace("BlobReadOption offset {} size {} ttl {} storeKey {}", offset, size, ttl, storeKey);
  }

  public long getOffset() {
    return this.offset;
  }

  public long getSize() {
    return this.size;
  }

  public long getTTL() {
    return ttl;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  @Override
  public int compareTo(BlobReadOptions o) {
    return offset.compareTo(o.getOffset());
  }
}

/**
 * An implementation of MessageReadSet that maintains a list of
 * offsets from the underlying file channel
 */
class StoreMessageReadSet implements MessageReadSet {

  private final List<BlobReadOptions> readOptions;
  private final FileChannel fileChannel;
  private final File file;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public StoreMessageReadSet(File file, FileChannel fileChannel, List<BlobReadOptions> readOptions,
      long fileEndPosition)
      throws IOException {

    Collections.sort(readOptions);
    for (BlobReadOptions readOption : readOptions) {
      if (readOption.getOffset() + readOption.getSize() > fileEndPosition) {
        throw new IllegalArgumentException("Invalid offset size pairs");
      }
      logger.trace("MessageReadSet entry file: {} offset: {} size: {} ", file.getAbsolutePath(), readOption.getOffset(),
          readOption.getSize());
    }
    this.readOptions = readOptions;
    this.fileChannel = fileChannel;
    this.file = file;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize)
      throws IOException {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index " + index + " out of the messageset size " + readOptions.size());
    }
    long startOffset = readOptions.get(index).getOffset() + relativeOffset;
    logger.trace("Blob Message Read Set position {} count {}", startOffset,
        Math.min(maxSize, readOptions.get(index).getSize() - relativeOffset));
    long written = fileChannel
        .transferTo(startOffset, Math.min(maxSize, readOptions.get(index).getSize() - relativeOffset), channel);
    logger.trace("Written {} bytes to the write channel from the file channel : {}", written, file.getAbsolutePath());
    return written;
  }

  @Override
  public int count() {
    return readOptions.size();
  }

  @Override
  public long sizeInBytes(int index) {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index out of the messageset for file " + file.getAbsolutePath());
    }
    return readOptions.get(index).getSize();
  }

  @Override
  public StoreKey getKeyAt(int index) {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index out of the messageset for file " + file.getAbsolutePath());
    }
    return readOptions.get(index).getStoreKey();
  }
}
