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

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.filetransfer.utils.FileCopyUtils;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread which will run the logic for FileCopy and will notify the listener
 * whether File Copy succeeded or Failed.
 */
public class FileCopyThread extends Thread {
  private final FileCopyStatusListener fileCopyStatusListener;
  private final FileCopyHandler fileCopyHandler;

  private boolean isRunning;

  private final CountDownLatch shutDownLatch;

  final String threadName;

  private final int START_CORRELATION_ID = 1;

  private final String CLIENT_ID = "FileCopyClient";
  /**
   * The logger for this class.
   */
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  /**
   * Constructor for FileCopyThread
   * @param fileCopyHandler the file copy handler
   * @param fileCopyStatusListener the file copy status listener
   */
  FileCopyThread(@Nonnull FileCopyHandler fileCopyHandler, @Nonnull FileCopyStatusListener fileCopyStatusListener) {
    Objects.requireNonNull(fileCopyHandler, "fileCopyHandler must not be null");
    Objects.requireNonNull(fileCopyStatusListener, "fileCopyStatusListener must not be null");

    this.fileCopyStatusListener = fileCopyStatusListener;
    this.fileCopyHandler = fileCopyHandler;
    this.threadName = "FileCopyThread-" + fileCopyStatusListener.getReplicaId().getPartitionId().toPathString();
    this.isRunning = true;
    this.shutDownLatch = new CountDownLatch(1);
  }

  @Override
  public void run() {
    logger.info("Starting FileCopyThread: {} for replicaId: {}", threadName, fileCopyStatusListener.getReplicaId());

    try {
      ReplicaId replicaId = fileCopyStatusListener.getReplicaId();
      if (replicaId == null) {
        throw new IllegalStateException("ReplicaId cannot be null");
      }

      //TODO add logic to get the source and target replica id
      ReplicaId targetReplicaId = FileCopyUtils.getPeerForFileCopy(replicaId.getPartitionId(), replicaId.getDataNodeId().getDatacenterName());

      if (targetReplicaId == null) {
        logger.warn("No peer replica found for file copy for replicaId: {}", replicaId);
        fileCopyStatusListener.onFileCopyFailure(new IOException("No peer replica found for file copy"));
        return;
      }

      FileCopyInfo fileCopyInfo = new FileCopyInfo(START_CORRELATION_ID, CLIENT_ID, replicaId, targetReplicaId);
      fileCopyHandler.start();
      // Start the file copy process

      fileCopyHandler.copy(fileCopyInfo);

      fileCopyStatusListener.onFileCopySuccess();
    } catch (Exception e) {
      fileCopyStatusListener.onFileCopyFailure(e);
    } finally {
      shutDownLatch.countDown();
    }
  }

  public void shutDown() throws InterruptedException {
    isRunning = false;
    fileCopyHandler.shutdown();
    shutDownLatch.await();
  }
}
