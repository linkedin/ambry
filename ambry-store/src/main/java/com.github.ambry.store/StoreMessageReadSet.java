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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A read option class that maintains the offset and size
 */
class BlobReadOptions implements Comparable<BlobReadOptions> {
  private final Long offset;
  private final Long size;
  private final Long ttl;
  private final StoreKey storeKey;
  private Logger logger = LoggerFactory.getLogger(getClass());

  private static final short version = 0;
  private static final short Version_Length = 2;
  private static final short Offset_Length = 8;
  private static final short Size_Length = 8;
  private static final short TTL_Length = 8;

  BlobReadOptions(long offset, long size, long ttl, StoreKey storeKey) {
    this.offset = offset;
    this.size = size;
    this.ttl = ttl;
    this.storeKey = storeKey;
    logger.trace("BlobReadOption offset {} size {} ttl {} storeKey {}", offset, size, ttl, storeKey);
  }

  public long getOffset() {
    return offset;
  }

  public long getSize() {
    return size;
  }

  public long getTTL() {
    return ttl;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  public MessageInfo getMessageInfo() {
    return new MessageInfo(storeKey, size, ttl);
  }

  public boolean validateFileEndOffset(long fileEndOffset) {
    return offset + size <= fileEndOffset;
  }

  @Override
  public int compareTo(BlobReadOptions o) {
    return offset.compareTo(o.getOffset());
  }

  public byte[] toBytes() {
    byte[] buf = new byte[Version_Length + Offset_Length + Size_Length + TTL_Length + storeKey.sizeInBytes()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(version);
    bufWrap.putLong(offset);
    bufWrap.putLong(size);
    bufWrap.putLong(TTL_Length);
    bufWrap.put(storeKey.toBytes());
    return buf;
  }

  public static BlobReadOptions fromBytes(DataInputStream stream, StoreKeyFactory factory) throws IOException {
    short version = stream.readShort();
    switch (version) {
      case 0:
        long offset = stream.readLong();
        long size = stream.readLong();
        long ttl = stream.readLong();
        StoreKey key = factory.getStoreKey(stream);
        return new BlobReadOptions(offset, size, ttl, key);
      default:
        throw new IOException("Unknown version encountered for BlobReadOptions");
    }
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
      long fileEndPosition) {

    Collections.sort(readOptions);
    for (BlobReadOptions readOption : readOptions) {
      if (!readOption.validateFileEndOffset(fileEndPosition)) {
        throw new IllegalArgumentException("Invalid offset size pairs");
      }
      logger.trace("MessageReadSet entry file: {} readOption: {} ", file.getAbsolutePath(), readOption);
    }
    this.readOptions = readOptions;
    this.fileChannel = fileChannel;
    this.file = file;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index " + index + " out of the messageset size " + readOptions.size());
    }
    long startOffset = readOptions.get(index).getOffset() + relativeOffset;
    long sizeToRead = Math.min(maxSize, readOptions.get(index).getSize() - relativeOffset);
    logger.trace("Blob Message Read Set position {} count {}", startOffset, sizeToRead);
    long written = fileChannel.transferTo(startOffset, sizeToRead, channel);
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
