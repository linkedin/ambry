/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class is responsible for interactions with Disk as Part Of File Copy Protocol.
 * It is responsible for reading and writing chunks and metadata to disk.
 */
class FileStore {
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
    //TODO: Implement shutdown Hook.
    isRunning = false;
  }
  boolean isRunning() {
    return isRunning;
  }
}
