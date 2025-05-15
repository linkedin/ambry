package com.github.ambry.store;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


public class SegmentFileTracker {
  private final RandomAccessFile randomAccessFile;

  public SegmentFileTracker(RandomAccessFile randomAccessFile) {
    this.randomAccessFile = randomAccessFile;
  }
  public boolean isOpen(){
    return this.randomAccessFile.getChannel().isOpen();
  }

  public FileChannel getRandomAccessFilechannel() {
    return randomAccessFile.getChannel();
  }


}
