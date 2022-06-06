/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.clustermap.MySqlReportAggregatorTask;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StorageUsageRefresher} implementation on top of mysql database.
 *
 * At the beginning of each month, aggregation task will take a snapshot of container storage usage and save this snapshot
 * on a mysql table. Aggregation task will not change this table until next month. The container storage usage in this
 * table becomes a monthly base for this month's container usage. And storage usage exposed by this refresher represents
 * how many bytes containers used in this month. So it's monthly usage, not total usage.
 *
 * For example, in the beginning of Dec 2020, container A[100]_C[1]'s total container usage is 100GB, then this usage will
 * be saved in the monthly table. Then every time aggregation task is running, it will get the most up to date total usage
 * for this container and update it in a second table. The refresher will periodically fetches total container usages from
 * the second table and subtract values from the monthly table to get the usage for this month. If at the 10th days of this
 * month, the total usage of container A[100]_C[1] is 110GB, by subtracting 100GB from monthly table, we know the monthly
 * usage for this container is 10GB.
 */
public class MySqlStorageUsageRefresher implements StorageUsageRefresher {
  static final DateTimeFormatter DATE_TIME_FORMATTER = MySqlReportAggregatorTask.TIMESTAMP_FORMATTER;
  static final ZoneOffset ZONE_OFFSET = MySqlReportAggregatorTask.ZONE_OFFSET;
  static Time time = SystemTime.getInstance();

  private static final Logger logger = LoggerFactory.getLogger(MySqlStorageUsageRefresher.class);

  private final AccountStatsStore accountStatsStore;
  // Container storage usage for this month.
  private final AtomicReference<Map<String, Map<String, Long>>> containerStorageUsageForCurrentMonthRef =
      new AtomicReference<>(null);
  private final AtomicReference<Listener> listener = new AtomicReference<>(null);
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;
  private final StorageQuotaServiceMetrics metrics;
  private final BackupFileManager backupFileManager;

  private volatile Map<String, Map<String, Long>> containerStorageUsageMonthlyBase;
  private volatile int retries = 0;
  private volatile String currentMonth = getCurrentMonth();

  /**
   * Constructor to instantiate a {@link MySqlStorageUsageRefresher}.
   * @param accountStatsStore The {@link AccountStatsStore} to interact with mysql database.
   * @param scheduler The {@link ScheduledExecutorService} to schedule background tasks.
   * @param config The {@link StorageQuotaConfig}.
   * @param metrics The {@link StorageQuotaServiceMetrics} to update metrics
   * @throws IOException
   */
  public MySqlStorageUsageRefresher(AccountStatsStore accountStatsStore, ScheduledExecutorService scheduler,
      StorageQuotaConfig config, StorageQuotaServiceMetrics metrics) throws IOException {
    this.accountStatsStore = Objects.requireNonNull(accountStatsStore, "AccountStatsStore is null");
    this.scheduler = Objects.requireNonNull(scheduler, "Scheduler is null");
    this.config = config;
    this.metrics = metrics;
    this.backupFileManager =
        this.config.backupFileDir.isEmpty() ? null : new BackupFileManager(this.config.backupFileDir);
    initializeContainerStorageUsageMonthlyBase();
    scheduleStorageUsageMonthlyBaseFetcher();
    initialFetchAndSchedule();
  }

  @Override
  public Map<String, Map<String, Long>> getContainerStorageUsage() {
    return Collections.unmodifiableMap(containerStorageUsageForCurrentMonthRef.get());
  }

  @Override
  public void registerListener(Listener listener) {
    Objects.requireNonNull(listener, "Listener has to be non-null");
    if (!this.listener.compareAndSet(null, listener)) {
      throw new IllegalStateException("Listener already registered");
    }
  }

  /**
   * Initialize {@link #containerStorageUsageMonthlyBase}. Try to load it from backup files first, if backup file doesn't
   * exist for this month, then load it from mysql database.
   */
  private void initializeContainerStorageUsageMonthlyBase() {
    long startTimeMs = System.currentTimeMillis();
    // First try to get the monthly base storage usage from backup
    try {
      if (backupFileManager != null) {
        logger.info("Fetching monthly base from backup directory for this month: {}", currentMonth);
        containerStorageUsageMonthlyBase = backupFileManager.getBackupFileContent(currentMonth);
      }
    } catch (IOException e) {
      logger.error("Failed to get container monthly usage for {} from backup", currentMonth, e);
    }

    if (containerStorageUsageMonthlyBase == null) {
      try {
        // If we are here, then loading monthly base from backup file failed. We have to fetch it from database.
        logger.info("Fetching monthly base from mysql database for this month: {}", currentMonth);
        containerStorageUsageMonthlyBase =
            convertAggregatedAccountStorageStatsToMap(accountStatsStore.queryMonthlyAggregatedAccountStorageStats(),
                config.usePhysicalStorage);
        // If the monthly base is indeed for this month, then try to persist it in the backup file.
        // There is a chance that the database has a snapshot from last month since the aggregation task is executed
        // every few minutes(maybe hours). Before the first aggregation task of this month is executed, the database
        // would have the snapshot of last month.
        if (currentMonth.equals(accountStatsStore.queryRecordedMonth())) {
          tryPersistMonthlyUsage();
        }
      } catch (Exception e) {
        throw new IllegalStateException("Unable to fetch monthly storage usage from mysql", e);
      }
    }
    metrics.mysqlRefresherInitTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Try to persist the monthly base usage in the backup file and ignore any exceptions.
   */
  private void tryPersistMonthlyUsage() {
    if (backupFileManager != null) {
      try {
        backupFileManager.persistentBackupFile(currentMonth, containerStorageUsageMonthlyBase);
        logger.info("Persisted monthly container usage for {}", currentMonth);
      } catch (IOException e) {
        // Error already been logged from backup file manager.
      }
    }
  }

  /**
   * Fetch the container monthly storage usage from mysql and update the in memory cache. If somehow it fails, we will
   * retry for several times before giving up.
   */
  private void fetchMonthlyStorageUsageAndMaybeRetry() {
    boolean shouldRetry = false;
    try {
      String monthValue = accountStatsStore.queryRecordedMonth();
      if (monthValue.equals(currentMonth)) {
        logger.info("Fetching monthly base from mysql database in periodical thread for this month: {}", currentMonth);
        containerStorageUsageMonthlyBase =
            convertAggregatedAccountStorageStatsToMap(accountStatsStore.queryMonthlyAggregatedAccountStorageStats(),
                config.usePhysicalStorage);
      } else {
        logger.info("Current month [{}] is not the same as month [{}]recorded in mysql database", currentMonth,
            monthValue);
        shouldRetry = true;
      }
    } catch (Exception e) {
      logger.error("Failed to refresh monthly storage usage", e);
      shouldRetry = true;
    }

    if (shouldRetry) {
      if (retries >= config.mysqlStoreRetryMaxCount) {
        logger.error("Failed to refresh monthly storage usage after {} retries, will skip for month: {}", retries,
            currentMonth);
        retries = 0;
        scheduleStorageUsageMonthlyBaseFetcher();
        return;
      }
      retries++;
      if (config.mysqlStoreRetryBackoffMs != 0) {
        logger.info("Schedule to retry to fetch monthly base from mysql database after {} ms, the retry count: {}",
            config.mysqlStoreRetryBackoffMs, retries);
        scheduler.schedule(this::fetchMonthlyStorageUsageAndMaybeRetry, config.mysqlStoreRetryBackoffMs,
            TimeUnit.MILLISECONDS);
      }
    } else {
      retries = 0;
      tryPersistMonthlyUsage();
      // sleep for two seconds so we can move on to next tick
      sleepFor(2000);
      scheduleStorageUsageMonthlyBaseFetcher();
    }
  }

  /**
   * Sleep for the given duration
   * @param durationInMs
   */
  private void sleepFor(long durationInMs) {
    try {
      Thread.sleep(durationInMs);
    } catch (Exception e) {
    }
  }

  /**
   * Schedule the task to fetch storage usage monthly base for next month.
   */
  private void scheduleStorageUsageMonthlyBaseFetcher() {
    long sleepDurationInSecs = secondsToNextTick(currentMonth, config.mysqlMonthlyBaseFetchOffsetSec);
    logger.info("Schedule to fetch container storage monthly base after {} seconds", sleepDurationInSecs);
    scheduler.schedule(this::fetchStorageUsageMonthlyBase, sleepDurationInSecs, TimeUnit.SECONDS);
  }

  /**
   * The task to fetch storage usage monthly base.
   */
  void fetchStorageUsageMonthlyBase() {
    currentMonth = getCurrentMonth();
    fetchMonthlyStorageUsageAndMaybeRetry();
  }

  /**
   * Return {@link #containerStorageUsageMonthlyBase}. Only used in test.
   * @return the {@link #containerStorageUsageMonthlyBase}.
   */
  Map<String, Map<String, Long>> getContainerStorageUsageMonthlyBase() {
    return Collections.unmodifiableMap(containerStorageUsageMonthlyBase);
  }

  /**
   * Return {@link BackupFileManager}. Only used in test.
   * @return {@link BackupFileManager}.
   */
  BackupFileManager getBackupFileManager() {
    return backupFileManager;
  }

  /**
   * Fetch the storage usage and schedule a task to periodically refresh the storage usage.
   */
  private void initialFetchAndSchedule() {
    Runnable updater = () -> {
      try {
        long startTimeMs = System.currentTimeMillis();
        Map<String, Map<String, Long>> base = containerStorageUsageMonthlyBase;
        Map<String, Map<String, Long>> storageUsage =
            convertAggregatedAccountStorageStatsToMap(accountStatsStore.queryAggregatedAccountStorageStats(),
                config.usePhysicalStorage);
        if (storageUsage != null) {
          subtract(storageUsage, base);
          containerStorageUsageForCurrentMonthRef.set(storageUsage);
          if (listener.get() != null) {
            listener.get().onNewContainerStorageUsage(Collections.unmodifiableMap(storageUsage));
          }
          metrics.mysqlRefresherRefreshUsageTimeMs.update(System.currentTimeMillis() - startTimeMs);
        }
      } catch (Exception e) {
        logger.error("Failed to retrieve the container usage from mysql", e);
        // If we already have a container usage map in memory, then don't replace it with empty map.
        containerStorageUsageForCurrentMonthRef.compareAndSet(null, Collections.EMPTY_MAP);
      }
    };
    updater.run();

    if (scheduler != null) {
      int initialDelay = new Random().nextInt(config.refresherPollingIntervalMs + 1);
      scheduler.scheduleAtFixedRate(updater, initialDelay, config.refresherPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info(
          "Background storage usage updater will fetch storage usage from remote starting {} ms from now and repeat with interval={} ms",
          initialDelay, config.refresherPollingIntervalMs);
    }
  }

  /**
   * Subtract values of {@code base} from values of {@code containerStorageUsage}.
   * If values exist in {@code containerStorageUsage} but not in {@code base}, then nothing happens.
   * If values exist in {@code base} but not in {@code containerStorageUsage}, then nothing happens either.
   * If values in {@code containerStorageUsage} are less than values in {@code base}, then the result is 0.
   * @param storageUsage
   * @param base
   */
  static void subtract(Map<String, Map<String, Long>> storageUsage, Map<String, Map<String, Long>> base) {
    for (Map.Entry<String, Map<String, Long>> accountStorageUsageEntry : storageUsage.entrySet()) {
      String accountId = accountStorageUsageEntry.getKey();
      if (base.containsKey(accountId)) {
        Map<String, Long> containerUsage = accountStorageUsageEntry.getValue();
        Map<String, Long> baseContainerUsage = base.get(accountId);
        containerUsage.replaceAll((k, v) -> Math.max(v - baseContainerUsage.getOrDefault(k, (long) 0), 0));
      }
    }
  }

  /**
   * Get current month in "yyyy-MM" format.
   * @return The string representation of current month.
   */
  static String getCurrentMonth() {
    return LocalDateTime.ofEpochSecond(time.seconds(), 0, ZONE_OFFSET).format(DATE_TIME_FORMATTER);
  }

  /**
   * Seconds to reach the next starting time to fetch container storage monthly base.
   * @param currentMonthValue The current month in string format.
   * @param offsetInSecond The offset in seconds to add to first day.
   * @return Seconds to reach the next staring time to fetch container Storage monthly base.
   */
  static long secondsToNextTick(String currentMonthValue, long offsetInSecond) {
    long secondsToCurrentMonthTick = secondsToCurrentMonthTick(currentMonthValue, offsetInSecond);
    if (secondsToCurrentMonthTick < 0) {
      return secondsToNextMonthTick(currentMonthValue, offsetInSecond);
    } else {
      return secondsToCurrentMonthTick;
    }
  }

  /**
   * Seconds to the first day of the current month, plus the offset in seconds. The result might be negative.
   * @param currentMonthValue The current month in string format.
   * @param offsetInSecond The offset in second to add to the first day of current month.
   * @return Seconds to reach first day plus {@code offsetInSecond} of current month.
   */
  static long secondsToCurrentMonthTick(String currentMonthValue, long offsetInSecond) {
    long currentSecond = time.seconds();
    String[] parts = currentMonthValue.split("-");
    int year = Integer.parseInt(parts[0]);
    int month = Integer.parseInt(parts[1]);
    LocalDateTime currentMonthDateTime = LocalDateTime.of(year, Month.of(month), 1, 0, 0);
    long currentMonthTick = currentMonthDateTime.plusSeconds(offsetInSecond).atOffset(ZONE_OFFSET).toEpochSecond();
    return currentMonthTick - currentSecond;
  }

  /**
   * Seconds to the first day of the next month, plus the offset in seconds.
   * @param currentMonthValue The current month in string format.
   * @param offsetInSecond The offset in second to add to the first day of next month.
   * @return Seconds to reach first day plus {@code offsetInSecond} of next month.
   */
  static long secondsToNextMonthTick(String currentMonthValue, long offsetInSecond) {
    // date time formatter only has year and month, so when incrementing the month by one, the day will
    // still be 0, which would be the first day of next month.
    // For example, if the currentMonth is "2020-11", then the currentMonthDateTime would be LocalDate(2020, 11, 0).
    // When calling plusMonth, it will return (2020, 12, 0), which is the first day of the next month.
    String[] parts = currentMonthValue.split("-");
    int year = Integer.parseInt(parts[0]);
    int month = Integer.parseInt(parts[1]);
    LocalDateTime currentMonthDateTime = LocalDateTime.of(year, Month.of(month), 1, 0, 0);
    long secondsNextMonth =
        currentMonthDateTime.plusMonths(1).plusSeconds(offsetInSecond).atOffset(ZONE_OFFSET).toEpochSecond();
    return secondsNextMonth - time.seconds();
  }

  /**
   * Convert an {@link AggregatedAccountStorageStats} to a map from account id to container id to storage usage.
   * The account id and container id are keys of the outer map and the inner map, they are both in string format.
   * @param aggregatedAccountStorageStats The {@link AggregatedAccountStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage, false to use logical storage usage.
   * @return A map from account id to container id to storage usage.
   */
  static Map<String, Map<String, Long>> convertAggregatedAccountStorageStatsToMap(
      AggregatedAccountStorageStats aggregatedAccountStorageStats, boolean usePhysicalStorageUsage) {
    Map<String, Map<String, Long>> result = new HashMap<>();
    if (aggregatedAccountStorageStats == null) {
      return result;
    }
    Map<Short, Map<Short, ContainerStorageStats>> storageStats = aggregatedAccountStorageStats.getStorageStats();
    for (short accountId : storageStats.keySet()) {
      Map<String, Long> containerMap = storageStats.get(accountId)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(ent -> String.valueOf(ent.getKey()),
              ent -> usePhysicalStorageUsage ? ent.getValue().getPhysicalStorageUsage()
                  : ent.getValue().getLogicalStorageUsage()));
      result.put(String.valueOf(accountId), containerMap);
    }
    return result;
  }

  /**
   * A monthly storage usage backup file manager. {@link MySqlStorageUsageRefresher} fetches monthly storage usage
   * at the beginning of each month, so the frequency is really low. Once the storage usage is fetched, a backup file
   * will be saved in local disk for recovery if the process restarts.
   *
   * CurrentMonth, in YYYY-MM format, will be used as filename for backup files. It's also the value stored in database.
   */
  static class BackupFileManager {
    static final ObjectMapper objectMapper = new ObjectMapper();
    static final String TEMP_FILE_SUFFIX = ".tmp";
    static final Pattern filenamePattern = Pattern.compile("^\\d{4}-\\d{2}$");
    static final Pattern tempFilenamePattern = Pattern.compile("^\\d{4}-\\d{2}" + TEMP_FILE_SUFFIX + "$");
    private final Path backupDirPath;
    private final Set<String> backupFiles = new HashSet<>();
    private final TypeReference<Map<String, Map<String, Long>>> typeReference =
        new TypeReference<Map<String, Map<String, Long>>>() {
        };

    /**
     * Constructor to create a {@link BackupFileManager}.
     * @param dir The directory for all backup files.
     * @throws IOException
     */
    BackupFileManager(String dir) throws IOException {
      if (dir == null || dir.isEmpty()) {
        throw new IllegalArgumentException("Backup directory path empty");
      }
      backupDirPath = Files.createDirectories(Paths.get(dir));
      loadBackupFiles();
    }

    /**
     * Persist the {@code usage} in map under the backup directory with the given {@code filename}. Filename has to match
     * the date pattern.
     * @param filename The filename to save the given usage.
     * @param usage The usage to save.
     * @throws IOException
     */
    void persistentBackupFile(String filename, Map<String, Map<String, Long>> usage) throws IOException {
      if (filename == null || filename.isEmpty() || !filenamePattern.matcher(filename).matches()) {
        throw new IllegalArgumentException("Invalid filename :" + filename);
      }
      if (usage == null) {
        throw new IllegalArgumentException("Invalid usage map");
      }

      if (backupFiles.contains(filename)) {
        return;
      }
      logger.trace("Persist container usage for {}", filename);
      String tempFileName = filename + TEMP_FILE_SUFFIX;
      Path tempFilePath = backupDirPath.resolve(tempFileName);
      Path filePath = backupDirPath.resolve(filename);

      // First save the usage in JSON format in the a temp file and then rename this temp file
      // to the target file since renaming file is atomic in POSIX.
      try (FileChannel channel = FileChannel.open(tempFilePath, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        String usageMapInJson = objectMapper.writeValueAsString(usage);
        channel.write(ByteBuffer.wrap(usageMapInJson.getBytes(StandardCharsets.UTF_8)));
        Files.move(tempFilePath, filePath);
      } catch (Exception e) {
        logger.error("Failed to serialize and persist usage map to file {}", filePath, e);
        throw e;
      }

      backupFiles.add(filename);
    }

    /**
     * Load all the backup files under the backup directory to a list.
     */
    private void loadBackupFiles() {
      logger.info("Loading mysql monthly storage usage backup file from directory {}", backupDirPath);
      File backupDir = backupDirPath.toFile();
      // First remove all the temp files
      FileFilter tempFileFilter = (File pathname) -> tempFilenamePattern.matcher(pathname.getName()).matches();
      File[] files = backupDir.listFiles(tempFileFilter);
      if (files != null) {
        for (File file : files) {
          logger.trace("Delete temp file {}", file.getName());
          tryDeleteFile(file.toPath());
        }
      }

      // Then add all the files
      FileFilter fileFilter = (File pathname) -> filenamePattern.matcher(pathname.getName()).matches();
      files = backupDir.listFiles(fileFilter);
      if (files != null) {
        for (File file : files) {
          backupFiles.add(file.getName());
        }
      }
      logger.info("Loaded {} backup files", backupFiles.size());
      logger.trace("Backup files {}", backupFiles);
    }

    /**
     * Return the container usage in map from given {@code filename}.
     * @param filename The filename to load container usage.
     * @return A map that represents container usage.
     * @throws IOException
     */
    Map<String, Map<String, Long>> getBackupFileContent(String filename) throws IOException {
      if (!backupFiles.contains(filename)) {
        return null;
      }
      Path filePath = backupDirPath.resolve(filename);
      Map<String, Map<String, Long>> content = objectMapper.readValue(filePath.toFile(), typeReference);
      return content;
    }

    Set<String> getBackupFiles() {
      return Collections.unmodifiableSet(backupFiles);
    }

    /***
     * Delete file identified by the given {@link Path}.
     * @param toDelete The path of file to be deleted.
     */
    private void tryDeleteFile(Path toDelete) {
      try {
        Files.delete(toDelete);
      } catch (NoSuchFileException e) {
        logger.error("File doesn't exist while deleting: {}", toDelete.toString(), e);
      } catch (IOException e) {
        logger.error("Encounter an I/O error while deleting file: {}", toDelete.toString(), e);
      } catch (Exception e) {
        logger.error("Encounter an unexpected error while deleting file: {}", toDelete.toString(), e);
      }
    }
  }
}
