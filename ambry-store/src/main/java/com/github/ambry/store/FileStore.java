package com.github.ambry.store;

import com.github.ambry.clustermap.FileStoreException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.ambry.clustermap.FileStoreException.FileStoreErrorCode.*;


public class FileStore {
  private static boolean isRunning = false;

  public FileStore() {
  }

  public ConcurrentHashMap<String, FileChannel> fileNameToFileChannelMap;

  public void start() throws StoreException {
    isRunning = true;
  }
  public boolean isRunning() {
    return isRunning;
  }
  public void stop() {
    isRunning = false;
  }
}
