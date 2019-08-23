/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.github.ambry.config.HelixAccountServiceConfig;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.helix.ZNRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A helper class to manage {@link Account} metadata backup. Notice this is not a thread-safe class and all the methods
 * should be protected by holding an external lock.
 *
 * Every backup file is associated with a {@link ZNRecord} version number and modified time. Version number is used to
 * identify each mutation to the {@link Account} metadata and modified time is used to show the order of each backup in
 * a human readable manner.
 *
 * Previously, {@link HelixAccountService} only keeps a backup when there is a update {@link Account} HTTP request
 * received by this instance. It doesn't backup mutations made by other instances. Since HTTP requests to update
 * {@link Account} are rare, latest backup files often holds a out-of-date view of the {@link Account} metadata.
 * In order to keep backup file up to date, in the new implementation, each mutation to the {@link Account} metadata
 * will be persisted with {@link LocalBackup}. Thus, whenever {@link HelixAccountService} fetches the {@link Account}
 * metadata, it tries to persist it.
 *
 * However {@link LocalBackup} only try it's best to persist the {@link Account} metadata and will give up at any
 * exception. The reasons to not guarantee the persistence of backup is
 * <ul>
 *   <li>Backup file is not the source of truth of {@link Account} metadata.</li>
 *   <li>There is no guarantee that {@link HelixAccountService} would persist all the revisions of {@link Account} metadata.</li>
 * </ul>
 *
 * {@link LocalBackup} also have to clean up the backup files in the old format. It keeps a predefined number of backup
 * files in the local storage and remove all the oldest backup files.
 */
class LocalBackup {
  static final String OLD_STATE_SUFFIX = "old";
  static final String NEW_STATE_SUFFIX = "new";
  static final String TEMP_FILE_SUFFIX = "tmp";
  static final String SEP = ".";
  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
  static final ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
  static final Pattern versionFilenamePattern = Pattern.compile("^(\\d+)\\.(\\d{8}T\\d{6})$");
  static final Pattern oldStateFilenamePattern = Pattern.compile("^(\\d{8}T\\d{6})\\." + OLD_STATE_SUFFIX + "$");
  static final Pattern newStateFilenamePattern = Pattern.compile("^(\\d{8}T\\d{6})\\." + NEW_STATE_SUFFIX + "$");

  private static final Logger logger = LoggerFactory.getLogger(LocalBackup.class);
  private final AccountServiceMetrics accountServiceMetrics;
  private final Path backupDirPath;
  private final HelixAccountServiceConfig config;
  private final ConcurrentSkipListMap<Integer, BackupFile> backupFiles;

  /**
   * Constructor to create an instance of {@link LocalBackup}.
   * @param accountServiceMetrics The {@link AccountServiceMetrics}
   * @param config The {@link HelixAccountServiceConfig}
   * @throws IOException if I/O error occurs
   */
  public LocalBackup(AccountServiceMetrics accountServiceMetrics, HelixAccountServiceConfig config) throws IOException {
    this.accountServiceMetrics = accountServiceMetrics;
    this.config = config;
    backupDirPath = config.backupDir.isEmpty() ? null : Files.createDirectories(Paths.get(config.backupDir));
    backupFiles = new ConcurrentSkipListMap<>();
    cleanupBackupFiles();
  }

  /**
   * Clean up the backup files. It cleans up
   * <ul>
   *   <li>Temporary files when creating a backup file</li>
   *   <li>Old backup files with version number</li>
   *   <li>Backup files in old format without version number</li>
   * </ul>
   */
  private void cleanupBackupFiles() {
    if (backupDirPath == null) {
      return;
    }
    File backupDir = backupDirPath.toFile();

    // First get all the file with temp file suffix and remove all of them
    final FileFilter tempFileFilter = (File pathname) -> pathname.getName().endsWith(SEP + TEMP_FILE_SUFFIX);
    for (File file : backupDir.listFiles(tempFileFilter)) {
      logger.trace("Delete temp file " + file.getName());
      file.delete();
    }

    // Then get all the file with version number and local timestamp
    final FileFilter versionFileFilter = (File pathname) -> versionFilenamePattern.matcher(pathname.getName()).find();
    for (File file : backupDir.listFiles(versionFileFilter)) {
      Matcher m = versionFilenamePattern.matcher(file.getName());
      logger.trace("Starting processing version backup file " + file.getName());
      if (m.find()) {
        int version = Integer.parseInt(m.group(1));
        long modifiedTimeInSecond = LocalDateTime.parse(m.group(2), TIMESTAMP_FORMATTER).toEpochSecond(zoneOffset);
        BackupFile currentBackup = new BackupFile(version, file.getName(), modifiedTimeInSecond);

        if (backupFiles.size() < config.maxBackupFileCount) {
          backupFiles.put(version, currentBackup);
        } else if (backupFiles.firstEntry().getKey() < version) {
          // remove the old version file and remove this entry from the map
          Map.Entry<Integer, BackupFile> entry = backupFiles.firstEntry();
          BackupFile toRemove = entry.getValue();
          logger.trace("Remove the oldest backup {} at version {}", toRemove.getFilename(), toRemove.getVersion());
          entry.getValue().tryRemove();
          backupFiles.remove(entry.getKey());
          // add new entry to the map
          backupFiles.put(version, currentBackup);
        } else {
          // remove the current file
          currentBackup.tryRemove();
        }
      } else {
        // We should never reach here, but sanity check
        logger.error("File {} doesn't match version pattern. This is not possible", file.getName());
      }
    }

    final FileFilter oldStateFileFilter = (File pathname) -> oldStateFilenamePattern.matcher(pathname.getName()).find();
    for (File file : backupDir.listFiles(oldStateFileFilter)) {
      logger.trace("Delete old state file " + file.getName());
      file.delete();
    }

    // Lastly, if we have enough files, we will just remove all the backup file without version number.
    // Otherwise, sort the file based on the modified time.
    final FileFilter newStateFileFilter = (File pathname) -> newStateFilenamePattern.matcher(pathname.getName()).find();
    File[] allNewStateFiles = backupDir.listFiles(newStateFileFilter);
    if (backupFiles.size() >= config.maxBackupFileCount) {
      logger.trace("More than {} versioned backup found, remove all the backup files in old format");
      for (File file : allNewStateFiles) {
        logger.trace("Delete new state file " + file.getName());
        file.delete();
      }
    } else {
      int start = 0;
      int size = allNewStateFiles.length;
      if (backupFiles.size() + size > config.maxBackupFileCount) {
        // Sort all the files based on the filename. Since the filename follows the DateTime formatter, sorting filename
        // is equivalent to sorting modified time.
        Arrays.sort(allNewStateFiles, new Comparator<File>() {
          @Override
          public int compare(File o1, File o2) {
            return o1.getName().compareTo(o2.getName());
          }
        });
        start = Math.max(backupFiles.size() + size - config.maxBackupFileCount, 0);
        logger.info("Found {} old format backup file, only need {}", size, size - start);
      }
      for (int i = 0; i < size; i++) {
        File file = allNewStateFiles[i];
        if (i < start) {
          logger.trace("Delete new state file " + file.getName());
          file.delete();
        } else {
          Matcher m = newStateFilenamePattern.matcher(file.getName());
          if (m.find()) {
            int version = i - size;
            long modifiedTimeInSecond = LocalDateTime.parse(m.group(1), TIMESTAMP_FORMATTER).toEpochSecond(zoneOffset);
            backupFiles.put(version, new BackupFile(version, file.getName(), modifiedTimeInSecond));
          } else {
            // sanity check
            logger.error("File {} doesn't match new state pattern. This is not possible", file.getName());
          }
        }
      }
    }
  }

  /**
   * Persist account map to local storage, with associated {@link ZNRecord} information.
   * @param state
   * @param record
   */
  void persistState(Map<String, String> state, ZNRecord record) {
    if (backupDirPath == null) {
      return;
    }
    Objects.requireNonNull(state, "Invalid account state");
    Objects.requireNonNull(record, "Invalid ZNRecord");
    int version = record.getVersion();
    if (backupFiles.containsKey(version)) {
      logger.trace("Version {} already has a backup file {}, skip persisting the state", version,
          backupFiles.get(version).getFilename());
      return;
    }
    if (!backupFiles.isEmpty() && backupFiles.firstEntry().getKey() > version) {
      logger.trace("Version {} is out of date, the smallest version is {}", version, backupFiles.firstEntry().getKey());
      return;
    }

    String fileName = getBackupFilenameFromZNRecord(record);
    String tempFileName = fileName + SEP + TEMP_FILE_SUFFIX;
    Path filePath = backupDirPath.resolve(fileName);
    Path tempFilePath = backupDirPath.resolve(tempFileName);

    long startTimeInMs = System.currentTimeMillis();
    if (!writeStateToFile(tempFilePath, state)) {
      accountServiceMetrics.backupErrorCount.inc();
      return;
    } else {
      try {
        Files.move(tempFilePath, filePath);
      } catch (IOException e) {
        logger.error("Failed to move temporary file " + tempFileName + " to file " + fileName, e);
        accountServiceMetrics.backupErrorCount.inc();
        return;
      }
    }
    accountServiceMetrics.backupWriteTimeInMs.update(System.currentTimeMillis() - startTimeInMs);

    while (backupFiles.size() >= config.maxBackupFileCount) {
      Map.Entry<Integer, BackupFile> entry = backupFiles.firstEntry();
      entry.getValue().tryRemove();
      backupFiles.remove(entry.getKey());
    }
    backupFiles.put(version, new BackupFile(version, fileName, record.getModifiedTime()));
  }

  /**
   * Return true if there is no backup file found.
   * @return True if there is no backup file found.
   */
  boolean isEmpty() {
    return backupFiles.isEmpty();
  }

  /**
   * Return the number of backup files.
   * @return The number of backup files.
   */
  int size() {
    return backupFiles.size();
  }

  /**
   * Return the account map persisted in the latest backup file, but prior to the given {@param afterTimeInSecond}.
   * If the latest backup file is older then the given timetamp, then return null. This is to prevent that caller of
   * this function load up a out-of-date account map from the backup file.
   * <p>
   *   If data from the latest backup is corrupted, then this function returns null;
   * </p>
   * @param afterTimeInSecond The unix epoch time which the latest backup's modifiedTime must be greater than.
   * @return The account map from the latest backup file.
   */
  Map<String, String> getLatestState(long afterTimeInSecond) {
    if (backupDirPath == null) {
      return null;
    }
    Map.Entry<Integer, BackupFile> entry = backupFiles.lastEntry();
    if (entry == null) {
      logger.warn("No backup file found");
      return null;
    }
    if (entry.getKey() < 0) {
      // This is a backup file without version number.
      // It's very hard to deserialize the bytes to a map, so just return null;
      logger.warn("Latest backup is in old format that doesn't have version number");
      return null;
    }
    BackupFile backupFile = entry.getValue();
    if (backupFile.getModifiedTimeInSecond() < afterTimeInSecond) {
      logger.warn("The latest backup was changed at timestamp: {}, but the requested time is {}",
          backupFile.getModifiedTimeInSecond(), afterTimeInSecond);
      return null;
    }

    Path filepath = backupDirPath.resolve(backupFile.getFilename());
    try {
      long startTimeInMs = System.currentTimeMillis();
      byte[] bytes = Files.readAllBytes(filepath);
      accountServiceMetrics.backupReadTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
      return deserializeState(bytes);
    } catch (IOException e) {
      accountServiceMetrics.backupErrorCount.inc();
      logger.error("Failed to read all bytes out from file " + filepath + " " + e.getMessage());
      return null;
    }
  }

  static String getBackupFilenameFromZNRecord(ZNRecord record) {
    long mtime = record.getModifiedTime();
    // The ModifiedTime is a unix timestamp in seconds
    String timestamp = LocalDateTime.ofEpochSecond(mtime, 0, zoneOffset).format(TIMESTAMP_FORMATTER);
    String fileName = record.getVersion() + SEP + timestamp;
    return fileName;
  }

  static boolean writeStateToFile(Path filepath, Map<String, String> state) {
    boolean persisted = false;
    try (FileChannel channel = FileChannel.open(filepath, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = serializeState(state);
      channel.write(buffer);
      persisted = true;
    } catch (IOException e) {
      // Failed to persist file
      logger.error("Failed to persist state to file " + filepath, e);
    }
    return persisted;
  }

  /**
   * Serialize the given account map to a json-formatted {@link ByteBuffer}.
   * @param state The account map.
   * @return {@link ByteBuffer} that contains the serialized bytes.
   */
  static ByteBuffer serializeState(Map<String, String> state) {
    JSONObject object = new JSONObject();
    for (Map.Entry<String, String> entry : state.entrySet()) {
      object.put(entry.getKey(), entry.getValue());
    }
    return ByteBuffer.wrap(object.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Deserialize the given byte array to an account map, which essentially just a map from string to string.
   * It returns null at any exception. This function assume the bytes are in json format.
   * @param bytes The byte array to deserialize.
   * @return An account map.
   */
  static Map<String, String> deserializeState(byte[] bytes) {
    try {
      JSONObject object = new JSONObject(new String(bytes, StandardCharsets.UTF_8));
      Map<String, String> result = new HashMap<>();
      for (String key : object.keySet()) {
        result.put(key, object.getString(key));
      }
      return result;
    } catch (JSONException e) {
      logger.error("Failed to deserialized bytes to account map: " + e.getMessage());
      return null;
    }
  }

  /**
   * BackupFile encapsulates the information about the backup files persisted in the local storage.
   * Since every local backup file would have a {@link ZNRecord} version and modifiedTime as part of the filename,
   * every instance of class would have the same information.
   * <p>
   *   Use negative number as version of backup file in old format. Since all the {@link ZNRecord}'s version should
   *   be positive, using negative number for older backup enforce the order of backups.
   * </p>
   */
  class BackupFile {
    private final int version;
    private final String filename;
    private final long modifiedTimeInSecond;

    /**
     *  Constructor to create a {@link BackupFile}.
     * @param version The {@link ZNRecord} version associated with this backup file.
     * @param filename The filename of this file.
     * @param modifiedTimeInSecond The {@link ZNRecord} modifiedTime associated with this backup file.
     */
    BackupFile(int version, String filename, long modifiedTimeInSecond) {
      this.version = version;
      this.filename = filename;
      this.modifiedTimeInSecond = modifiedTimeInSecond;
    }

    /**
     * Return the version number;
     * @return The version number;
     */
    int getVersion() {
      return version;
    }

    /**
     * Return the filename;
     * @return The filename;
     */
    String getFilename() {
      return filename;
    }

    /**
     * Return the modified time in seconds.
     * @return The modified time in seconds.
     */
    long getModifiedTimeInSecond() {
      return modifiedTimeInSecond;
    }

    /**
     * Try to remove the backup from local storage. This function doesn't guarantee file would be removed. It exits at
     * any exception, since file would be removed next time.
     */
    void tryRemove() {
      try {
        Files.delete(backupDirPath.resolve(filename));
      } catch (IOException e) {
        logger.error("Failed to delete file " + filename, e);
      }
    }
  }
}
