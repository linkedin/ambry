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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;

import static org.mockito.Mockito.*;


/**
 * Utility class for common functions used in tests of store classes.
 */
class StoreTestUtils {
  static final DiskSpaceAllocator DEFAULT_DISK_SPACE_ALLOCATOR =
      new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(new MetricRegistry()));

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
   * Need mock ReplicaId to get and set isSealed state
   */
  public static class MockReplicaId implements ReplicaId {

    private String storeId;
    private long capacity;
    private String filePath;
    private PartitionId partitionId;
    private boolean isSealed = false;

    MockReplicaId(String storeId, long capacity, String filePath) {
      this.storeId = storeId;
      this.capacity = capacity;
      this.filePath = filePath;
      partitionId = mock(PartitionId.class);
      when(partitionId.toString()).thenReturn(storeId);
      when(partitionId.toPathString()).thenReturn(storeId);
    }

    @Override
    public PartitionId getPartitionId() {
      return partitionId;
    }

    @Override
    public DataNodeId getDataNodeId() {
      return null;
    }

    @Override
    public String getMountPath() {
      return null;
    }

    @Override
    public String getReplicaPath() {
      return filePath;
    }

    @Override
    public List<? extends ReplicaId> getPeerReplicaIds() {
      return null;
    }

    @Override
    public long getCapacityInBytes() {
      return capacity;
    }

    @Override
    public DiskId getDiskId() {
      return null;
    }

    @Override
    public boolean isDown() {
      return false;
    }

    @Override
    public boolean isSealed() {
      return isSealed;
    }

    @Override
    public JSONObject getSnapshot() {
      return null;
    }

    @Override
    public void markDiskDown() {
      // Null OK
    }

    @Override
    public void markDiskUp() {
      // Null OK
    }

    @Override
    public ReplicaType getReplicaType() {
      return ReplicaType.DISK_BACKED;
    }

    public void setSealedState(boolean isSealed) {
      this.isSealed = isSealed;
    }
  }

  /**
   * Creates a mock replicaId for blob store testing
   * @param storeId partitionId from replicaId.getPartitionId() will toString() to this
   * @param capacity replicaId.getCapacityInBytes() will output this
   * @param filePath replicaId.getReplicaPath() will output this
   * @return mock replicaId
   */
  static MockReplicaId createMockReplicaId(String storeId, long capacity, String filePath) {
    return new MockReplicaId(storeId, capacity, filePath);
  }

  /**
   * Cleans up the {@code dir} and deletes it.
   * @param dir the directory to be cleaned up and deleted.
   * @param deleteDirectory if {@code true}, the directory is deleted too.
   * @return {@code true if the delete was successful}. {@code false} otherwise
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

  /**
   * Create store config with given segment size.
   * @param segmentSize the size of each log segment
   * @param setFilePermission {@code true} if setting file permission is enabled. {@code false} otherwise.
   * @return {@link StoreConfig}
   */
  static StoreConfig createStoreConfig(long segmentSize, boolean setFilePermission) {
    Properties properties = new Properties();
    properties.setProperty("store.segment.size.in.bytes", Long.toString(segmentSize));
    properties.setProperty("store.set.file.permission.enabled", Boolean.toString(setFilePermission));
    return new StoreConfig(new VerifiableProperties(properties));
  }
}
