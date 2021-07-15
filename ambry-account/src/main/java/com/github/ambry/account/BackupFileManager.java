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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.HelixAccountServiceConfig;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A helper class to manage {@link Account} metadata backup. Notice this is not a thread-safe class and all the methods
 * should be protected by holding an external lock.
 *
 * Every backup file is associated with the version number and modified time. Version number is used to identify each
 * mutation to the {@link Account} metadata and modified time is used to show the order of each backup in a human readable manner.
 *
 * However {@link BackupFileManager} only tries its best to persist the {@link Account} metadata and will give up at any
 * exception. The reasons to not guarantee the persistence of backup are
 * <ul>
 *   <li>Backup file is not the source of truth for {@link Account} metadata.</li>
 *   <li>There is no guarantee that {@link AccountService} would publish all mutations to {@link Account} metadata.</li>
 * </ul>
 *
 * {@link BackupFileManager} also has to clean up the backup files in the old format. It keeps a predefined maximum number of
 * backup files in the local storage and removes the oldest backup files when the number of files exceeds the predefined
 * maximum number.
 */
class BackupFileManager {
  static final String OLD_STATE_SUFFIX = "old";
  static final String NEW_STATE_SUFFIX = "new";
  static final String TEMP_FILE_SUFFIX = "tmp";
  static final String SEP = ".";
  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
  static final ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
  static final Pattern versionFilenamePattern = Pattern.compile("^(\\d+)\\.(\\d{8}T\\d{6})$");
  static final Pattern oldStateFilenamePattern = Pattern.compile("^(\\d{8}T\\d{6})\\." + OLD_STATE_SUFFIX + "$");
  static final Pattern newStateFilenamePattern = Pattern.compile("^(\\d{8}T\\d{6})\\." + NEW_STATE_SUFFIX + "$");

  private static final Logger logger = LoggerFactory.getLogger(BackupFileManager.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final AccountServiceMetrics accountServiceMetrics;
  private final Path backupDirPath;
  private final int maxBackupFileCount;
  private final ConcurrentSkipListMap<Integer, BackupFileInfo> backupFileInfos;

  static {
    objectMapper.writer(new DefaultPrettyPrinter());
  }

  /**
   * Constructor to create an instance of {@link BackupFileManager}.
   * @param accountServiceMetrics The {@link AccountServiceMetrics}
   * @param backupDir The {@link HelixAccountServiceConfig}
   * @param maxBackupFileCount maximum number of back up files to store
   * @throws IOException if I/O error occurs
   */
  public BackupFileManager(AccountServiceMetrics accountServiceMetrics, String backupDir, int maxBackupFileCount)
      throws IOException {
    this.accountServiceMetrics = accountServiceMetrics;
    backupDirPath = backupDir.isEmpty() ? null : Files.createDirectories(Paths.get(backupDir));
    this.maxBackupFileCount = maxBackupFileCount;
    backupFileInfos = new ConcurrentSkipListMap<>();
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
    FileFilter tempFileFilter = (File pathname) -> pathname.getName().endsWith(SEP + TEMP_FILE_SUFFIX);
    File[] files = backupDir.listFiles(tempFileFilter);
    if (files != null) {
      for (File file : files) {
        logger.trace("Delete temp file {}", file.getName());
        tryDeleteFile(file);
      }
    }

    // Then get all the file with version number and local timestamp
    FileFilter versionFileFilter = (File pathname) -> versionFilenamePattern.matcher(pathname.getName()).find();
    files = backupDir.listFiles(versionFileFilter);
    if (files != null) {
      for (File file : files) {
        Matcher m = versionFilenamePattern.matcher(file.getName());
        m.find();
        logger.trace("Starting processing version backup file {}", file.getName());
        int version = Integer.parseInt(m.group(1));
        long modifiedTimeInSecond = LocalDateTime.parse(m.group(2), TIMESTAMP_FORMATTER).toEpochSecond(zoneOffset);
        BackupFileInfo currentBackup = new BackupFileInfo(version, file.getName(), modifiedTimeInSecond);

        if (backupFileInfos.size() < maxBackupFileCount) {
          // When the number of backup files are under the maximum value, just add the current backupFile in the map.
          backupFileInfos.put(version, currentBackup);
        } else {
          // When the number of backup files exceeds the maximum value, we have to remove the one backupFile.
          if (backupFileInfos.firstEntry().getKey() < version) {
            // The current backupFile's version is larger than the smallest one in the backupFileInfos map, then remove the
            // smallest backupFile and it's entry from map and add the current backupFile in the map.
            Map.Entry<Integer, BackupFileInfo> entry = backupFileInfos.firstEntry();
            BackupFileInfo toRemove = entry.getValue();
            logger.trace("Remove the oldest backup {} at version {}", toRemove.getFilename(), toRemove.getVersion());
            tryDeleteBackupFile(entry.getValue());
            backupFileInfos.remove(entry.getKey());
            backupFileInfos.put(version, currentBackup);
          } else {
            // The current backupFile's version is smaller than the smallest version in the map, then remove the
            // current backupFile.
            tryDeleteBackupFile(currentBackup);
          }
        }
      }
    }

    FileFilter oldStateFileFilter = (File pathname) -> oldStateFilenamePattern.matcher(pathname.getName()).find();
    files = backupDir.listFiles(oldStateFileFilter);
    if (files != null) {
      for (File file : files) {
        logger.trace("Delete old state file {}", file.getName());
        tryDeleteFile(file);
      }
    }

    // Lastly, if we have enough files, we will just remove all the backup file without version number.
    // Otherwise, sort the file based on the modified time.
    FileFilter newStateFileFilter = (File pathname) -> newStateFilenamePattern.matcher(pathname.getName()).find();
    File[] allNewStateFiles = backupDir.listFiles(newStateFileFilter);
    if (allNewStateFiles != null) {
      if (backupFileInfos.size() >= maxBackupFileCount) {
        logger.trace("More than {} versioned backup found, remove all the backup files in old format",
            maxBackupFileCount);
        for (File file : allNewStateFiles) {
          logger.trace("Delete new state file {}", file.getName());
          tryDeleteFile(file);
        }
      } else {
        int startIndexToPreserveBackupFile = 0;
        int size = allNewStateFiles.length;
        if (backupFileInfos.size() + size > maxBackupFileCount) {
          // Sort all the files based on the filename. Since the filename follows the DateTime formatter, sorting filename
          // is equivalent to sorting modified time.
          Arrays.sort(allNewStateFiles, Comparator.comparing(File::getName));
          startIndexToPreserveBackupFile = Math.max(backupFileInfos.size() + size - maxBackupFileCount, 0);
          logger.info("Found {} old format backup file, only need {}", size, size - startIndexToPreserveBackupFile);
        }
        for (int i = 0; i < size; i++) {
          File file = allNewStateFiles[i];
          if (i < startIndexToPreserveBackupFile) {
            logger.trace("Delete new state file {}", file.getName());
            tryDeleteFile(file);
          } else {
            Matcher m = newStateFilenamePattern.matcher(file.getName());
            m.find();
            int version = i - size;
            long modifiedTimeInSecond = LocalDateTime.parse(m.group(1), TIMESTAMP_FORMATTER).toEpochSecond(zoneOffset);
            backupFileInfos.put(version, new BackupFileInfo(version, file.getName(), modifiedTimeInSecond));
          }
        }
      }
    }
  }

  /**
   * Persist account map to local storage, with associated version and modified time information.
   * @param accounts The account collection
   * @param version The version of the account
   * @param modifiedTimeInSec modified time in seconds
   */
  void persistAccountMap(Collection<Account> accounts, int version, long modifiedTimeInSec) {
    if (backupDirPath == null) {
      return;
    }
    Objects.requireNonNull(accounts, "Invalid account collection");
    if (backupFileInfos.containsKey(version)) {
      logger.trace("Version {} already has a backup file {}, skip persisting the state", version,
          backupFileInfos.get(version).getFilename());
      return;
    }
    if (!backupFileInfos.isEmpty() && backupFileInfos.firstEntry().getKey() > version) {
      logger.error("Version {} is out of date, the smallest version is {}", version,
          backupFileInfos.firstEntry().getKey());
      return;
    }

    String fileName = getBackupFilename(version, modifiedTimeInSec);
    String tempFileName = fileName + SEP + TEMP_FILE_SUFFIX;
    Path filePath = backupDirPath.resolve(fileName);
    Path tempFilePath = backupDirPath.resolve(tempFileName);

    long startTimeInMs = System.currentTimeMillis();
    try {
      writeAccountsToFile(tempFilePath, accounts);
      Files.move(tempFilePath, filePath);
    } catch (IOException e) {
      logger.error("Failed to persist state to file: {}", fileName, e);
      accountServiceMetrics.backupErrorCount.inc();
      return;
    }
    accountServiceMetrics.backupWriteTimeInMs.update(System.currentTimeMillis() - startTimeInMs);

    while (backupFileInfos.size() >= maxBackupFileCount) {
      Map.Entry<Integer, BackupFileInfo> entry = backupFileInfos.firstEntry();
      tryDeleteBackupFile(entry.getValue());
      backupFileInfos.remove(entry.getKey());
    }
    backupFileInfos.put(version, new BackupFileInfo(version, fileName, modifiedTimeInSec));
  }

  /**
   * Return true if there is no backup file found.
   * @return True if there is no backup file found.
   */
  boolean isEmpty() {
    return backupFileInfos.isEmpty();
  }

  /**
   * Return the number of backup files.
   * @return The number of backup files.
   */
  int size() {
    return backupFileInfos.size();
  }

  /**
   * Return the account map persisted in the latest backup file, but prior to the given {@param afterTimeInSecond}.
   * If the latest backup file is older then the given timetamp, then return null. This is to prevent that caller of
   * this function load up a out-of-date account map from the backup file.
   * <p>
   *   If data from the latest backup is corrupted, then this function returns null;
   * </p>
   * @param latestTimeAllowedInSecond The unix epoch time which the latest backup's modifiedTime must be greater than.
   * @return The account map from the latest backup file.
   */
  Collection<Account> getLatestAccounts(long latestTimeAllowedInSecond) {
    if (backupDirPath == null) {
      return null;
    }
    Map.Entry<Integer, BackupFileInfo> entry = backupFileInfos.lastEntry();
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
    BackupFileInfo backupFileInfo = entry.getValue();
    if (backupFileInfo.getModifiedTimeInSecond() < latestTimeAllowedInSecond) {
      logger.warn("The latest backup was changed at timestamp: {}, but the requested time is {}",
          backupFileInfo.getModifiedTimeInSecond(), latestTimeAllowedInSecond);
      return null;
    }

    Path filepath = backupDirPath.resolve(backupFileInfo.getFilename());
    try {
      long startTimeInMs = System.currentTimeMillis();
      byte[] bytes = Files.readAllBytes(filepath);
      accountServiceMetrics.backupReadTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
      return deserializeAccounts(bytes);
    } catch (IOException e) {
      accountServiceMetrics.backupErrorCount.inc();
      logger.error("Failed to read all bytes out from file {} {}", filepath, e.getMessage());
      return null;
    }
  }

  /**
   * Delete the given file and log out error when there is any.
   * @param file The file to delete.
   */
  private void tryDeleteFile(File file) {
    deleteFile(file.toPath());
  }

  /**
   * Try to delete the backup from local storage. This function doesn't guarantee file would be removed. It exits at
   * any exception, since file would be removed next time.
   */
  private void tryDeleteBackupFile(BackupFileInfo backupFileInfo) {
    Path toDelete = backupDirPath.resolve(backupFileInfo.getFilename());
    deleteFile(toDelete);
  }

  /***
   * Delete file identified by the given {@link Path}.
   * @param toDelete The path of file to be deleted.
   */
  private void deleteFile(Path toDelete) {
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

  /**
   * Generate the backup filename with version number and modified time.
   * @param version The version of backup filename
   * @param modifiedTimeInSec modified time in seconds
   * @return The filename.
   */
  static String getBackupFilename(int version, long modifiedTimeInSec) {
    String timestamp = LocalDateTime.ofEpochSecond(modifiedTimeInSec, 0, zoneOffset).format(TIMESTAMP_FORMATTER);
    return version + SEP + timestamp;
  }

  /**
   * Persist the account collection to the given file.
   * @param filepath The filepath to persist account map.
   * @param accounts The {@link Collection} of {@link Account}s
   * @throws IOException Any I/O error.
   */
  static void writeAccountsToFile(Path filepath, Collection<Account> accounts) throws IOException {
    try (FileChannel channel = FileChannel.open(filepath, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = serializeAccounts(accounts);
      channel.write(buffer);
    } catch (IOException e) {
      // Failed to persist file
      logger.error("Failed to persist accounts to file {}", filepath, e);
      throw e;
    }
  }

  /**
   * Serialize given account collection in json format in a {@link ByteBuffer}.
   * @param accounts The {@link Collection} of {@link Account}s.
   * @return {@link ByteBuffer} that contains the serialized bytes.
   */
  static ByteBuffer serializeAccounts(Collection<Account> accounts) throws IOException {
    return ByteBuffer.wrap(objectMapper.writeValueAsBytes(accounts));
  }

  /**
   * Deserialize the given byte array to an account collection.
   * It returns null at any exception. This function assume the bytes are in json format.
   * @param bytes The byte array to deserialize.
   * @return The {@link Collection} of {@link Account}s.
   */
  static Collection<Account> deserializeAccounts(byte[] bytes) {
    try {
      Collection<Account> accounts = objectMapper.readValue(bytes, new TypeReference<Collection<Account>>() {
      });
      return accounts;
    } catch (IOException e) {
      logger.error("Failed to deserialized bytes to account collection: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Gets the latest version number of back up files
   * @return
   */
  public int getLatestVersion() {
    return backupFileInfos.isEmpty() ? 0 : backupFileInfos.lastKey();
  }

  /**
   * BackupFileInfo encapsulates the information about the backup files persisted in the local storage.
   * Since every local backup file would have a version and modifiedTime as part of the filename,
   * every instance of class would have the same information.
   * <p>
   *   Use negative number as version of backup file in old format. Since all the version should
   *   be positive, using negative number for older backup enforce the order of backups.
   * </p>
   */
  class BackupFileInfo {
    private final int version;
    private final String filename;
    private final long modifiedTimeInSecond;

    /**
     *  Constructor to create a {@link BackupFileInfo}.
     * @param version The version associated with this backup file.
     * @param filename The filename of this file.
     * @param modifiedTimeInSecond The modifiedTime associated with this backup file.
     */
    BackupFileInfo(int version, String filename, long modifiedTimeInSecond) {
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
     * Return the modified time in second.
     * @return The modified time in second.
     */
    long getModifiedTimeInSecond() {
      return modifiedTimeInSecond;
    }
  }
}
