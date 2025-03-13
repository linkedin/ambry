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

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.List;

/**
 * Interface for managing dynamic Thread Pool for File Copy. This Manager
 * is responsible for creating thread pools for file copy and managing
 * assignments of partitions for hydration to threads in the thread pool.
 */

public interface FileCopyBasedReplicationThreadPoolManager {
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
   * Should return the disks on which partition hydration is either completed
   * or not started yet.
   * @return the List Of DiskIds that can be hydrated next.
   */
  List<DiskId> getDiskIdsToHydrate();

  /**
   * It takes individual replicaIds to be hydrated and
   * start hydration process on those replicas.
   * @param replicaId the replicaId to submit for hydration
   */
  void submitReplicaForHydration(ReplicaId replicaId);

  /**
   * It takes individual replicaIds to be removed from the hydration process
   * and stops hydration process on those replicas.
   * @param replicaId the replicaId to remove from hydration
   * @return true if the replica was removed successfully, false otherwise
   */
  boolean stopAndRemoveReplicaFromThreadPool(ReplicaId replicaId);
}
