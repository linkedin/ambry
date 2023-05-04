/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
