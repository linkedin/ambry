package com.github.ambry.router.utils;

import com.github.ambry.router.RouterException;
import java.nio.ByteBuffer;


/**
 * A placeholder for buffer pool that is otpimzied for fixed-size buffer allocation.
 */
public class FixedSizeByteBufferPool implements ByteBufferPool {
  @Override
  public ByteBuffer allocate(int size, long timeToBlockMs)
      throws InterruptedException, RouterException {
    return null;
  }

  @Override
  public void deallocate(ByteBuffer buffer) {

  }

  @Override
  public long availableMemory() {
    return 0;
  }

  @Override
  public int queued() {
    return 0;
  }

  @Override
  public long capacity() {
    return 0;
  }
}
