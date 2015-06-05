package com.github.ambry.utils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


/**
 * A file watcher class that listens to changes to a filename in a given number of directories
 */

public class FileWatcher {
  private WatchService watchService;
  private String fileName;
  private int registeredPathsCount;
  private HashMap<WatchKey, String> registeredKeys;

  /**
   * Initialize a file watcher class that listens to events on the given fileName. The events are typically create,
   * modify and delete events.
   *
   * @param fileName The fileName of the files in the directories to listen on.
   * @throws IOException
   */
  public FileWatcher(String fileName)
      throws IOException {
    watchService = FileSystems.getDefault().newWatchService();
    registeredPathsCount = 0;
    this.fileName = fileName;
    registeredKeys = new HashMap<WatchKey, String>();
  }

  /**
   * Register a file path (directory) with this FileWatcher. Multiple paths can be registered, and the watcher
   * will listen for changes to the file with relative name fileName in all the paths registered.
   *
   * @param filePath The path to register.
   * @param kinds The kinds of events to listen for: ENTRY_CREATE, ENTRY_MODIFY or ENTRY_DELETE.
   * @throws IOException
   */
  public void register(String filePath, WatchEvent.Kind<?>... kinds)
      throws IOException {
    final Path path = FileSystems.getDefault().getPath(filePath);
    WatchKey key = path.register(watchService, kinds);
    registeredKeys.put(key, filePath);
    registeredPathsCount++;
  }

  /**
   * Wait until the files with the fileName in all the registered paths received an associated event, or until the
   * timeout is reached. If no paths were registered, this will throw.
   *
   * @param timeoutMs The timeout for the wait.
   * @return true if all the associated files went through (at least one of the) registered event kinds within the given
   * timeout, false otherwise.
   *
   * @throws InterruptedException
   */
  public boolean waitForChange(int timeoutMs)
      throws InterruptedException {
    int count = registeredPathsCount;
    HashMap<WatchKey, String> keys = new HashMap<WatchKey, String>(registeredKeys);
    if (count == 0) {
      throw new IllegalStateException();
    }
    long limit = SystemTime.getInstance().milliseconds() + timeoutMs;
    while (true) {
      final WatchKey wk = watchService.poll(limit - SystemTime.getInstance().milliseconds(), TimeUnit.MILLISECONDS);
      if (wk == null) {
        return false;
      }
      if (!keys.containsKey(wk)) {
        throw new IllegalStateException("Unexpected event received");
      }
      for (WatchEvent<?> event : wk.pollEvents()) {
        final Path changed = (Path) event.context();
        if (changed.endsWith(fileName)) {
          count--;
          break;
        }
      }
      wk.reset();
      keys.remove(wk);
      if (count == 0) {
        break;
      }
    }
    return true;
  }

  /**
   * Close this watcher.
   * @throws IOException
   */
  public void close()
      throws IOException {
    watchService.close();
  }
}
