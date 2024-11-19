package com.github.ambry.config;

public class FileCopyConfig {

  public static final String PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK = "parallel.partition.hydration.count.per.disk";
  @Config(PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK)
  public final int parallelPartitionHydrationCountPerDisk;

  public static final String NUMBER_OF_FILE_COPY_THREADS = "number.of.file.copy.threads";
  @Config(NUMBER_OF_FILE_COPY_THREADS)
  public final int numberOfFileCopyThreads;

  public static final String FILE_CHUNK_TIMEOUT_IN_MINUTES = "file.chunk.timeout.in.minutes";
  @Config(FILE_CHUNK_TIMEOUT_IN_MINUTES)
  public final long fileChunkTimeoutInMins;

  /**
   * The frequency at which the data gets flushed to disk
   */
  public static final String STORE_DATA_FLUSH_INTERVAL_IN_MBS = "store.data.flush.size.In";
  @Config("store.data.flush.size.In")
  @Default("60")
  public final long storeDataFlushIntervalSeconds;

  public FileCopyConfig(VerifiableProperties verifiableProperties) {
    parallelPartitionHydrationCountPerDisk = verifiableProperties.getInt(PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK, 1);
    numberOfFileCopyThreads = verifiableProperties.getInt(NUMBER_OF_FILE_COPY_THREADS, 4);
    fileChunkTimeoutInMins = verifiableProperties.getInt(FILE_CHUNK_TIMEOUT_IN_MINUTES, 5);
    storeDataFlushIntervalSeconds = verifiableProperties.getLong("store.data.flush.size", 60);
  }
}
