package com.github.ambry.clustermap;

import java.util.concurrent.locks.ReentrantLock;


/**
 * A reentrant lock implementation for {@link DistributedLock}. This is a local lock that only works for one process.
 *
 */
public class DistributedLockLocalImpl implements DistributedLock {
  private final ReentrantLock lock = new ReentrantLock();

  @Override
  public boolean tryLock() {
    return lock.tryLock();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public void close() {
  }
}
