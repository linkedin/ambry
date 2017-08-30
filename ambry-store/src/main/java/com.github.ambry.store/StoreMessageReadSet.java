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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A read option class that maintains the offset and size
 */
class BlobReadOptions implements Comparable<BlobReadOptions>, Closeable {
  private final LogSegment segment;
  private final Pair<File, FileChannel> segmentView;
  private final Offset offset;
  private final MessageInfo info;
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;

  private static final short VERSION_LENGTH = 2;
  private static final short SIZE_LENGTH = 8;
  private static final short EXPIRES_AT_MS_LENGTH = 8;

  BlobReadOptions(Log log, Offset offset, MessageInfo info) {
    segment = log.getSegment(offset.getName());
    if (offset.getOffset() + info.getSize() > segment.getEndOffset()) {
      throw new IllegalArgumentException(
          "Invalid offset [" + offset + "] and size [" + info.getSize() + "]. Segment end offset: " + "["
              + segment.getEndOffset() + "]");
    }
    segmentView = segment.getView();
    this.offset = offset;
    this.info = info;
    logger.trace("BlobReadOption offset {} size {} MessageInfo {} ", offset, info.getSize(), info);
  }

  String getLogSegmentName() {
    return offset.getName();
  }

  long getOffset() {
    return offset.getOffset();
  }

  MessageInfo getMessageInfo() {
    return info;
  }

  File getFile() {
    return segmentView.getFirst();
  }

  FileChannel getChannel() {
    return segmentView.getSecond();
  }

  @Override
  public int compareTo(BlobReadOptions o) {
    return offset.compareTo(o.offset);
  }

  /**
   * Serializes this {@link BlobReadOptions} to bytes
   * Note: This does not serialize some fields like accountId, containerId and crc
   * @return the serialized form of this {@link BlobReadOptions} in bytes
   */
  byte[] toBytes() {
    byte[] offsetBytes = offset.toBytes();
    byte[] buf = new byte[VERSION_LENGTH + offsetBytes.length + SIZE_LENGTH + EXPIRES_AT_MS_LENGTH + info.getStoreKey()
        .sizeInBytes()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(VERSION_1);
    bufWrap.put(offsetBytes);
    bufWrap.putLong(info.getSize());
    bufWrap.putLong(info.getExpirationTimeInMs());
    bufWrap.put(info.getStoreKey().toBytes());
    return buf;
  }

  /**
   * Deserialized the stream to form {@link BlobReadOptions}
   * Note: Some fields are not serialized like accountId, containerId and crc and hence will take up defaults or null
   * after deserialization
   * @param stream the {@link DataInputStream} to deserialize
   * @param factory {@link StoreKeyFactory} to use
   * @param log the {@link Log} to use
   * @return the {@link BlobReadOptions} thus deserialized from the {@code stream}
   * @throws IOException
   */
  static BlobReadOptions fromBytes(DataInputStream stream, StoreKeyFactory factory, Log log) throws IOException {
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        // backwards compatibility
        Offset offset = new Offset(log.getFirstSegment().getName(), stream.readLong());
        long size = stream.readLong();
        long expiresAtMs = stream.readLong();
        StoreKey key = factory.getStoreKey(stream);
        return new BlobReadOptions(log, offset,
            new MessageInfo(key, size, expiresAtMs, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
                Utils.Infinite_Time));
      case VERSION_1:
        offset = Offset.fromBytes(stream);
        size = stream.readLong();
        expiresAtMs = stream.readLong();
        key = factory.getStoreKey(stream);
        return new BlobReadOptions(log, offset,
            new MessageInfo(key, size, expiresAtMs, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
                Utils.Infinite_Time));
      default:
        throw new IOException("Unknown version encountered for BlobReadOptions");
    }
  }

  @Override
  public void close() {
    if (open.compareAndSet(true, false)) {
      segment.closeView();
    }
  }
}

/**
 * An implementation of MessageReadSet that maintains a list of
 * offsets from the underlying file channel
 */
class StoreMessageReadSet implements MessageReadSet {

  private final List<BlobReadOptions> readOptions;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  StoreMessageReadSet(List<BlobReadOptions> readOptions) {
    Collections.sort(readOptions);
    this.readOptions = readOptions;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index " + index + " out of the messageset size " + readOptions.size());
    }
    BlobReadOptions options = readOptions.get(index);
    long startOffset = options.getOffset() + relativeOffset;
    long sizeToRead = Math.min(maxSize, options.getMessageInfo().getSize() - relativeOffset);
    logger.trace("Blob Message Read Set position {} count {}", startOffset, sizeToRead);
    long written = options.getChannel().transferTo(startOffset, sizeToRead, channel);
    logger.trace("Written {} bytes to the write channel from the file channel : {}", written, options.getFile());
    return written;
  }

  @Override
  public int count() {
    return readOptions.size();
  }

  @Override
  public long sizeInBytes(int index) {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index [" + index + "] out of the messageset");
    }
    return readOptions.get(index).getMessageInfo().getSize();
  }

  @Override
  public StoreKey getKeyAt(int index) {
    if (index >= readOptions.size()) {
      throw new IndexOutOfBoundsException("index [" + index + "] out of the messageset");
    }
    return readOptions.get(index).getMessageInfo().getStoreKey();
  }
}
