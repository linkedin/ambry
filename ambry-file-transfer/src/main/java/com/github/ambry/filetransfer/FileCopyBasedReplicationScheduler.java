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


/**
 * Interface for FileCopy Based Replication Scheduler.This scheduler
 * schedules the replication of files from one node to another
 * based on the Priority of the Partition.
 */
public interface FileCopyBasedReplicationScheduler {
  /**
   * Start the scheduler.
   */
  void start();

  /**
   * Shutdown the scheduler.
   */
  void shutdown();

  /**
   * Schedule the replication of Partitions.
   */
  void scheduleFileCopy();

  /**
   * Create a thread pool with the given number of threads.
   * @param numberOfThreads the number of threads to create in the thread pool
   * @return true if the thread pool was created successfully, false otherwise
   */
  boolean createThreadPool(int numberOfThreads);

  /**
   * @return the number thread pool size.
   */
  int getThreadPoolSize();

  /**
   * It takes individual replicaIds to be hydrated and
   * start hydration process on those replicas.
   * @param replicaId the replicaId to submit for hydration
   */
  void addReplica(ReplicaId replicaId);
}
