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

/**
 * Interface for FileCopy Based Replication Scheduler.This scheduler
 * schedules the replication of files from one node to another
 * based on the Priority of the Partition.
 */
public interface FileCopyBasedReplicationScheduler extends Runnable{
  /**
   * Start the scheduler.
   */
  void startScheduler() throws InterruptedException;

  /**
   * Shutdown the scheduler.
   */
  void shutdown() throws InterruptedException;

  /**
   * Schedule the replication of Partitions.
   */
  void scheduleFileCopy() throws InterruptedException;

  /**
   * @return the number thread pool size.
   */
  int getThreadPoolSize();

}