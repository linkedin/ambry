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
package com.github.ambry.filecopy;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.store.StoreException;
import java.io.IOException;
import javax.annotation.Nonnull;


public class MockNoOpFileCopyHandler implements FileCopyHandler {

  private Exception exception;

  public MockNoOpFileCopyHandler() {
    this.exception = null;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public void clearException() {
    this.exception = null;
  }

  @Override
  public void copy(@Nonnull FileCopyInfo fileCopyInfo) {
    if (exception != null) {
      try {
        throw exception;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void shutdown() throws InterruptedException {
    // no op
  }

  @Override
  public void start() throws StoreException {
    // no op
  }
}
