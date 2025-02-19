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

package com.github.ambry.config;

/**
 * Configuration parameters for file copy operations in Ambry.
 * This class manages settings related to:
 * - Parallel processing
 * - Thread management
 * - Timeouts
 * - Data flushing
 * - Metadata file naming
 */
public class FileCopyConfig {

  /**
   * Number of partitions that can be hydrated in parallel per disk.
   * Higher values increase parallelism but also resource usage.
   */
  public static final String FILECOPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK = "filecopy.parallel.partition.hydration.count.per.disk";
  @Config(FILECOPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK)
  public final int filecopyParallelPartitionHydrationCountPerDisk;

  /**
   * Number of threads dedicated to file copy operations.
   * Controls the level of concurrent file transfers.
   */
  public static final String FILECOPY_NUMBER_OF_FILE_COPY_THREADS = "filecopy.number.of.file.copy.threads";
  @Config(FILECOPY_NUMBER_OF_FILE_COPY_THREADS)
  public final int filecopyNumberOfFileCopyThreads;

  /**
   * Timeout duration for file chunk operations in minutes.
   * After this duration, incomplete chunk operations are considered failed.
   */
  public static final String FILECOPY_FILE_CHUNK_TIMEOUT_IN_MINUTES = "filecopy.file.chunk.timeout.in.minutes";
  @Config(FILECOPY_FILE_CHUNK_TIMEOUT_IN_MINUTES)
  public final long filecopyFileChunkTimeoutInMins;

  /**
   * The frequency at which data gets flushed to disk in megabytes.
   * Lower values increase durability but may impact performance.
   * Default: 1000MB
   */
  public static final String FILECOPY_STORE_DATA_FLUSH_INTERVAL_IN_MBS = "filecopy.store.data.flush.interval.In.MBs";
  @Config(FILECOPY_STORE_DATA_FLUSH_INTERVAL_IN_MBS)
  @Default("1000")
  public final long filecopyStoreDataFlushIntervalInMbs;

  /**
   * Name of the metadata file used for file copy operations.
   * This file stores information about sealed logs and their associated metadata.
   * Default: "logs_metadata_file"
   */
  public static final String FILECOPY_META_DATA_FILE_NAME = "filecopy.meta.data.file.name";
  @Config(FILECOPY_META_DATA_FILE_NAME)
  @Default("logs_metadata_file")
  public final String filecopyMetaDataFileName;

  /**
   * Creates a new FileCopyConfig with the provided properties.
   *
   * @param verifiableProperties Properties containing configuration values
   *        If a property is not specified, default values are used:
   *        - parallelPartitionHydrationCountPerDisk: 1
   *        - numberOfFileCopyThreads: 4
   *        - fileChunkTimeoutInMins: 5
   *        - storeDataFlushIntervalInMbs: 1000
   *        - fileCopyMetaDataFileName: "logs_metadata_file"
   */
  public FileCopyConfig(VerifiableProperties verifiableProperties) {
    filecopyMetaDataFileName = verifiableProperties.getString(FILECOPY_META_DATA_FILE_NAME, "logs_metadata_file");
    filecopyParallelPartitionHydrationCountPerDisk = verifiableProperties.getInt(
        FILECOPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK, 1);
    filecopyNumberOfFileCopyThreads = verifiableProperties.getInt(FILECOPY_NUMBER_OF_FILE_COPY_THREADS, 4);
    filecopyFileChunkTimeoutInMins = verifiableProperties.getInt(FILECOPY_FILE_CHUNK_TIMEOUT_IN_MINUTES, 5);
    filecopyStoreDataFlushIntervalInMbs = verifiableProperties.getLong(FILECOPY_STORE_DATA_FLUSH_INTERVAL_IN_MBS, 1000);
  }
}
