package com.github.ambry.store;

import com.github.ambry.clustermap.FileStoreException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;


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

  public void putChunkToFile(String mountPath, String fileName, ByteBuffer byteBuffer, long offset, long size) {
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", File);
    }
    if (byteBuffer == null) {
      throw new IllegalArgumentException("ByteBuffer is null");
    }
    FileChannel currentFileBuffer = fileNameToFileChannelMap.get(fileName);
    if (currentFileBuffer == null) {
      throw new IllegalArgumentException("File not found");
    }
  }


}
