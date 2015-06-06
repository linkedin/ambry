package com.github.ambry.utils;

import java.io.File;
import java.nio.file.StandardWatchEventKinds;
import org.junit.Assert;
import org.junit.Test;


public class FileWatcherTest {
  @Test
  public void testFileCreateModifyDelete() {
    FileWatcher watcher;
    try {
      String dirName = "/tmp/FileWatcherTest";
      String fileName = "testfile";
      String tempFileName = "temp";
      File dir = new File(dirName);
      if (!dir.exists()) {
        dir.mkdir();
      }
      dir.deleteOnExit();

      File testFile = new File(dirName + "/" + fileName);
      if (testFile.exists()) {
        testFile.delete();
      }
      watcher = new FileWatcher(fileName);
      watcher.register(dirName, StandardWatchEventKinds.ENTRY_CREATE);
      testFile.createNewFile();
      testFile.deleteOnExit();
      boolean ret = watcher.waitForChange(30000);
      Assert.assertTrue(ret);
      watcher.close();

      watcher = new FileWatcher(fileName);
      // When an "mv x y" is done, an ENTRY_CREATE is triggered for y on Linux, whereas an ENTRY_MODIFY is triggered on Mac.
      watcher.register(dirName, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

      File tempFile = new File(dirName + "/" + tempFileName);
      tempFile.createNewFile();
      FileWatcher watcher2 = new FileWatcher(tempFileName);
      watcher2.register(dirName, StandardWatchEventKinds.ENTRY_DELETE);

      tempFile.renameTo(testFile);
      ret = watcher2.waitForChange(30000);
      ret = watcher.waitForChange(30000);
      //Assert.assertTrue(ret);
      watcher.close();

      watcher = new FileWatcher(fileName);
      watcher.register(dirName, StandardWatchEventKinds.ENTRY_DELETE);
      testFile.delete();
      ret = watcher.waitForChange(30000);
      Assert.assertTrue(ret);

      watcher.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }
}
