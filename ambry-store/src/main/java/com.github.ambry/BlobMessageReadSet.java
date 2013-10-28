package com.github.ambry;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/11/13
 * Time: 4:11 PM
 * To change this template use File | Settings | File Templates.
 */

import com.sun.javaws.exceptions.InvalidArgumentException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

class BlobReadOptions implements Comparable<BlobReadOptions> {
  private final Long offset;
  private final Long size;

  BlobReadOptions(long offset, long size) {
    this.offset = offset;
    this.size = size;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getSize() {
    return this.size;
  }

  @Override
  public int compareTo(BlobReadOptions o) {
    return offset.compareTo(o.getOffset());
  }
}

public class BlobMessageReadSet implements MessageReadSet{

  private AtomicLong size;
  private final BlobReadOptions[] readOptions;
  private final FileChannel fileChannel;

  public BlobMessageReadSet(File file, FileChannel fileChannel, BlobReadOptions[] readOptions, long fileEndPosition) throws IOException {

    size = new AtomicLong(0);
    Arrays.sort(readOptions);
    for (BlobReadOptions readOption : readOptions) {
      if (readOption.getOffset() + readOption.getSize() >= fileEndPosition) {
        throw new IllegalArgumentException("Invalid offset size pairs");
      }
      size.addAndGet(readOption.getSize());
    }
    this.readOptions = readOptions;
    this.fileChannel = fileChannel;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    if (index >= readOptions.length) {
      throw new IndexOutOfBoundsException("index out of the messageset");
    }
    long startOffset = readOptions[index].getOffset() + relativeOffset;
    return fileChannel.transferTo(startOffset, Math.min(maxSize, readOptions[index].getSize() - relativeOffset), channel);
  }

  @Override
  public int count() {
    return readOptions.length;
  }

  @Override
  public long sizeInBytes(int index) {
    if (index >= readOptions.length) {
      throw new IndexOutOfBoundsException("index out of the messageset");
    }
    return readOptions[index].getSize();
  }
}
