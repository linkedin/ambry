package com.github.ambry.utils;

import java.io.File;
import java.io.IOException;
import org.junit.Test;
import org.junit.Assert;


/**
 * Tests for file lock
 */
public class FileLockTest {
  @Test
  public void testFileLock()
      throws IOException {
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
