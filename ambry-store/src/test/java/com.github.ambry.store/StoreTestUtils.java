/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


/**
 * Utility class for common functions used in tests of store classes.
 */
class StoreTestUtils {

  /**
   * Creates a temporary directory whose name starts with the given {@code prefix}.
   * @param prefix the prefix of the directory name.
   * @return the directory created as a {@link File} instance.
   * @throws IOException
   */
  static File createTempDirectory(String prefix) throws IOException {
    File tempDir = Files.createTempDirectory(prefix).toFile();
    tempDir.deleteOnExit();
    return tempDir;
  }

  /**
   * Creates a mock replicaId for blob store testing
   * @param storeId partitionId from replicaId.getPartitionId() will toString() to this
   * @param capacity replicaId.getCapacityInBytes() will output this
   * @param filePath replicaId.getReplicaPath() will output this
   * @return mock replicaId
   */
  static ReplicaId createMockReplicaId(String storeId, long capacity, String filePath) {
    ReplicaId replicaId = mock(ReplicaId.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(partitionId.toString()).thenReturn(storeId);
    when(replicaId.getPartitionId()).thenReturn(partitionId);
    when(replicaId.getCapacityInBytes()).thenReturn(capacity);
    when(replicaId.getReplicaPath()).thenReturn(filePath);
    return replicaId;
  }

  /**
   * Cleans up the {@code dir} and deletes it.
   * @param dir the directory to be cleaned up and deleted.
   * @param deleteDirectory if {@code true}, the directory is deleted too.
   * @throws IOException
   */
  static boolean cleanDirectory(File dir, boolean deleteDirectory) throws IOException {
    if (!dir.exists()) {
      return true;
    }
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(dir.getAbsolutePath() + " is not a directory");
    }
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IOException("Could not list files in directory: " + dir.getAbsolutePath());
    }
    boolean success = true;
    for (File file : files) {
      success = file.delete() && success;
    }
    return deleteDirectory ? dir.delete() && success : success;
  }
}
