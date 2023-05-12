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

/**
 * A distributed lock interface. Multiple hosts would use this mutually exclusive distributed lock
 * to coordinate work so that no more than one host are execute the same job.
 */
public interface DistributedLock {
  /**
   * Try to acquire the lock.
   * @return True if the lock is successfully acquired, false otherwise.
   */
  boolean tryLock();

  /**
   * Release the acquired lock. Please make sure that the lock is acquired before unlocking it.
   */
  void unlock();

  /**
   * Close this lock and release all the resources associated with this lock.
   * After close, this lock can't be used any more.
   */
  void close();
}
