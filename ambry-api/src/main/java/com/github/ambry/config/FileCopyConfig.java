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
  public static final String STORE_DATA_FLUSH_INTERVAL_IN_MBS = "store.data.flush.interval.In.MBs";
  @Config(STORE_DATA_FLUSH_INTERVAL_IN_MBS)
  @Default("1000")
  public final long storeDataFlushIntervalInMbs;

  public static final String FILE_COPY_META_DATA_FILE_NAME = "file.copy.meta.data.file.name";
  @Config(FILE_COPY_META_DATA_FILE_NAME)
  @Default("sealed_logs_metadata_file")
  public final String fileCopyMetaDataFileName;

  public FileCopyConfig(VerifiableProperties verifiableProperties) {
    fileCopyMetaDataFileName = verifiableProperties.getString(FILE_COPY_META_DATA_FILE_NAME, "sealed_logs_metadata_file");
    parallelPartitionHydrationCountPerDisk = verifiableProperties.getInt(PARALLEL_PARTITION_HYDRATION_COUNT_PER_DISK, 1);
    numberOfFileCopyThreads = verifiableProperties.getInt(NUMBER_OF_FILE_COPY_THREADS, 4);
    fileChunkTimeoutInMins = verifiableProperties.getInt(FILE_CHUNK_TIMEOUT_IN_MINUTES, 5);
    storeDataFlushIntervalInMbs = verifiableProperties.getLong(STORE_DATA_FLUSH_INTERVAL_IN_MBS, 1000);
  }
}