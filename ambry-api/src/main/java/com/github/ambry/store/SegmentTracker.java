package com.github.ambry.store;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class SegmentTracker {
  private final RandomAccessFile randomAccessFile;


  public SegmentTracker(RandomAccessFile randomAccessFile) {
    this.randomAccessFile = randomAccessFile;
  }
  public boolean isOpen(){
    return this.randomAccessFile.getChannel().isOpen();
  }

  public FileChannel getRandomAccessFilechannel() {
    return randomAccessFile.getChannel();
  }

}
