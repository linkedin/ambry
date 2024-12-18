package com.github.ambry.store;

import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;


public class FileStore {
  private boolean isRunning = false;
  private ConcurrentHashMap<String, FileChannel> fileNameToFileChannelMap;

  public FileStore(String dataDir){
    fileNameToFileChannelMap = new ConcurrentHashMap<>();
    isRunning = false;
  }

  void start() {
    if(!isRunning) {
      //Start the FileStore
      isRunning = true;
    }
  }
  void stop() {
    //Implement shutdown Hook.
    isRunning = false;
  }
  boolean isRunning() {
    return isRunning;
  }
}
