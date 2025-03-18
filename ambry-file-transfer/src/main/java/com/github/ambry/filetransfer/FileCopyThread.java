/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;


/**
 * Thread which will run the logic for FileCopy and will notify the listener
 * whether File Copy succeeded or Failed.
 */
public class FileCopyThread extends Thread {
  private final FileCopyStatusListener fileCopyStatusListener;
  private final FileCopyHandler fileCopyHandler;

  private boolean isRunning;

  private final CountDownLatch shutDownLatch;

  FileCopyThread(@Nonnull FileCopyHandler fileCopyHandler, @Nonnull FileCopyStatusListener fileCopyStatusListener) {
    Objects.requireNonNull(fileCopyHandler, "fileCopyHandler must not be null");
    Objects.requireNonNull(fileCopyStatusListener, "fileCopyStatusListener must not be null");

    this.fileCopyStatusListener = fileCopyStatusListener;
    this.fileCopyHandler = fileCopyHandler;
    this.isRunning = true;
    this.shutDownLatch = new CountDownLatch(1);
  }

  @Override
  public void run() {
    try {
      //TODO add required params for File copy handler
      fileCopyHandler.copy();
      fileCopyStatusListener.onFileCopySuccess();
    } catch (Exception e) {
      fileCopyStatusListener.onFileCopyFailure(e);
    } finally {
      shutDownLatch.countDown();
    }
  }

  public void shutDown() throws InterruptedException {
    isRunning = false;
    shutDownLatch.await();
  }
}
