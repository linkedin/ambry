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
  public static final String PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK = "parallel.partition.hydration.count.per.disk";
  @Config(PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK)
  public final int parallelPartitionHydrationCountPerDisk;

  /**
   * Number of threads dedicated to file copy operations.
   * Controls the level of concurrent file transfers.
   */
  public static final String NUMBER_OF_FILE_COPY_THREADS = "number.of.file.copy.threads";
  @Config(NUMBER_OF_FILE_COPY_THREADS)
  public final int numberOfFileCopyThreads;

  /**
   * Timeout duration for file chunk operations in minutes.
   * After this duration, incomplete chunk operations are considered failed.
   */
  public static final String FILE_CHUNK_TIMEOUT_IN_MINUTES = "file.chunk.timeout.in.minutes";
  @Config(FILE_CHUNK_TIMEOUT_IN_MINUTES)
  public final long fileChunkTimeoutInMins;

  /**
   * The frequency at which data gets flushed to disk in megabytes.
   * Lower values increase durability but may impact performance.
   * Default: 1000MB
   */
  public static final String STORE_DATA_FLUSH_INTERVAL_IN_MBS = "store.data.flush.interval.In.MBs";
  @Config(STORE_DATA_FLUSH_INTERVAL_IN_MBS)
  @Default("1000")
  public final long storeDataFlushIntervalInMbs;

  /**
   * Name of the metadata file used for file copy operations.
   * This file stores information about sealed logs and their associated metadata.
   * Default: "sealed_logs_metadata_file"
   */
  public static final String FILE_COPY_META_DATA_FILE_NAME = "file.copy.meta.data.file.name";
  @Config(FILE_COPY_META_DATA_FILE_NAME)
  @Default("sealed_logs_metadata_file")
  public final String fileCopyMetaDataFileName;

  /**
   * Creates a new FileCopyConfig with the provided properties.
   *
   * @param verifiableProperties Properties containing configuration values
   *        If a property is not specified, default values are used:
   *        - parallelPartitionHydrationCountPerDisk: 1
   *        - numberOfFileCopyThreads: 4
   *        - fileChunkTimeoutInMins: 5
   *        - storeDataFlushIntervalInMbs: 1000
   *        - fileCopyMetaDataFileName: "sealed_logs_metadata_file"
   */
  public FileCopyConfig(VerifiableProperties verifiableProperties) {
    fileCopyMetaDataFileName = verifiableProperties.getString(FILE_COPY_META_DATA_FILE_NAME, "sealed_logs_metadata_file");
    parallelPartitionHydrationCountPerDisk = verifiableProperties.getInt(PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK, 1);
    numberOfFileCopyThreads = verifiableProperties.getInt(NUMBER_OF_FILE_COPY_THREADS, 4);
    fileChunkTimeoutInMins = verifiableProperties.getInt(FILE_CHUNK_TIMEOUT_IN_MINUTES, 5);
    storeDataFlushIntervalInMbs = verifiableProperties.getLong(STORE_DATA_FLUSH_INTERVAL_IN_MBS, 1000);
  }
}
