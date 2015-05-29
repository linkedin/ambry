package com.github.ambry.utils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;


/**
 * A file watcher class that listens to changes to a filename in a given number of directories
 */

public class FileWatcher {
  private WatchService watchService;
  private String fileName;
  private int registeredPathsCount;

  /**
   * Initialize a file watcher class that listens to changes to the given fileName
   *
   * @param fileName The fileName of the files in the directories to listen on.
   * @throws IOException
   */
  public FileWatcher(String fileName)
      throws IOException {
    watchService = FileSystems.getDefault().newWatchService();
    registeredPathsCount = 0;
    this.fileName = fileName;
  }

  /**
   * Register a file path (directory) with this FileWatcher. Multiple paths can be registered, and the watcher
   * will listen for changes to the file with relative name fileName in all the paths registered.
   *
   * @param filePath The path to register.
   * @throws IOException
   */
  public void register(String filePath)
      throws IOException {
    final Path path = FileSystems.getDefault().getPath(filePath);
    path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
    registeredPathsCount++;
  }

  /**
   * Wait until the files with the fileName in all the registered paths change. If no paths were registered, this will
   * throw.
   *
   * @throws InterruptedException
   */
  public void waitForChange()
      throws InterruptedException {
    int count = registeredPathsCount;
    if (count == 0) {
      throw new IllegalStateException();
    }
    while (true) {
      final WatchKey wk = watchService.take();
      for (WatchEvent<?> event : wk.pollEvents()) {
        final Path changed = (Path) event.context();
        if (changed.endsWith(fileName)) {
          count--;
          break;
        }
      }
      if (count == 0) {
        wk.reset();
        break;
      }
    }
  }
}
