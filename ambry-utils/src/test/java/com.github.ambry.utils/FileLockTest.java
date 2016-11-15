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
package com.github.ambry.utils;

import java.io.File;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for file lock
 */
public class FileLockTest {
  @Test
  public void testFileLock() throws IOException {
    File file = File.createTempFile("temp", "1");
    file.deleteOnExit();
    FileLock lock = new FileLock(file);
    lock.lock();
    Assert.assertFalse(lock.tryLock());
    lock.unlock();
    Assert.assertTrue(lock.tryLock());
    lock.unlock();
    lock.destroy();
  }
}
