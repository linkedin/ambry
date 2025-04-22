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
package com.github.ambry.config;

import java.util.Objects;


public class FileCopyBasedReplicationConfig {
  /**
   * The number of partitions that can be hydrated in parallel per disk
   */
  public static final String FILE_COPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK = "filecopy.parallel.partition.hydration.count.per.disk";
  @Config(FILE_COPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK)
  public final int fileCopyParallelPartitionHydrationCountPerDisk;

  /**
   * The number of threads that can be used to copy files
   */
  public static final String FILE_COPY_NUMBER_OF_FILE_COPY_THREADS = "filecopy.number.of.file.copy.threads";
  @Config(FILE_COPY_NUMBER_OF_FILE_COPY_THREADS)
  public final int fileCopyNumberOfFileCopyThreads;

  /**
   * The frequency at which the data gets flushed to disk
   */
  public static final String FILE_COPY_DATA_FLUSH_INTERVAL_IN_MBS = "filecopy.data.flush.interval.in.mbs";
  @Config(FILE_COPY_DATA_FLUSH_INTERVAL_IN_MBS)
  @Default("1000")
  public final long fileCopyDataFlushIntervalInMbs;

  /**
   * The name of the file that stores the metadata for the file copy
   */
  public static final String FILE_COPY_META_DATA_FILE_NAME = "filecopy.meta.data.file.name";
  @Config(FILE_COPY_META_DATA_FILE_NAME)
  @Default("segments_metadata_file")
  public final String fileCopyMetaDataFileName;

  public static final String FILE_COPY_REPLICA_TIMEOUT_SECS = "filecopy.replica.timeout.secs";
  @Config(FILE_COPY_REPLICA_TIMEOUT_SECS)
  @Default("36000")
  public final long fileCopyReplicaTimeoutSecs;

  public static final String FILE_COPY_SCHEDULER_WAIT_TIME_SECS = "filecopy.scheduler.wait.time.secs";
  @Config(FILE_COPY_SCHEDULER_WAIT_TIME_SECS)
  @Default("30")
  public final long fileCopySchedulerWaitTimeSecs;

  public static final String FILECOPYHANDLER_MAX_API_RETRIES = "filecopyhandler.max.api.retries";
  @Config(FILECOPYHANDLER_MAX_API_RETRIES)
  @Default("3")
  public final int fileCopyHandlerMaxApiRetries;

  /**
   * The backoff time in milliseconds between retries
   */
  public static final String FILECOPYHANDLER_RETRY_BACKOFF_MS = "filecopyhandler.retry.backoff.ms";
  @Config(FILECOPYHANDLER_RETRY_BACKOFF_MS)
  @Default("500")
  public final int fileCopyHandlerRetryBackoffMs;

  /**
   * The chunk size for file copy
   */
  public static final String FILECOPYHANDLER_CHUNK_SIZE = "filecopyhandler.chunk.size";
  @Config(FILECOPYHANDLER_CHUNK_SIZE)
  @Default("10485760") // 10 MB
  public final int getFileCopyHandlerChunkSize;

  public static final String FILECOPYHANDLER_CONNECTION_TIMEOUT_MS = "filecopyhandler.connection.timeout.ms";
  @Config(FILECOPYHANDLER_CONNECTION_TIMEOUT_MS)
  @Default("5000")
  public final int fileCopyHandlerConnectionTimeoutMs;


  public FileCopyBasedReplicationConfig(VerifiableProperties verifiableProperties) {
    fileCopyMetaDataFileName = verifiableProperties.getString(FILE_COPY_META_DATA_FILE_NAME, "segments_metadata_file");
    fileCopyParallelPartitionHydrationCountPerDisk = verifiableProperties.getInt(FILE_COPY_PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK, 1);
    fileCopyNumberOfFileCopyThreads = verifiableProperties.getInt(FILE_COPY_NUMBER_OF_FILE_COPY_THREADS, 1);
    fileCopyDataFlushIntervalInMbs = verifiableProperties.getLong(FILE_COPY_DATA_FLUSH_INTERVAL_IN_MBS, 1000);
    fileCopyReplicaTimeoutSecs = verifiableProperties.getLong(FILE_COPY_REPLICA_TIMEOUT_SECS, 36000);
    fileCopySchedulerWaitTimeSecs = verifiableProperties.getLong(FILE_COPY_SCHEDULER_WAIT_TIME_SECS, 30);
    Objects.requireNonNull(verifiableProperties, "verifiableProperties cannot be null");

    fileCopyHandlerMaxApiRetries = verifiableProperties.getInt(FILECOPYHANDLER_MAX_API_RETRIES, 3);
    fileCopyHandlerRetryBackoffMs = verifiableProperties.getInt(FILECOPYHANDLER_RETRY_BACKOFF_MS, 500);
    getFileCopyHandlerChunkSize = verifiableProperties.getInt(FILECOPYHANDLER_CHUNK_SIZE, 10 * 1024 * 1024); // 10 MB
    fileCopyHandlerConnectionTimeoutMs = verifiableProperties.getInt(FILECOPYHANDLER_CONNECTION_TIMEOUT_MS, 5000);
  }
}
