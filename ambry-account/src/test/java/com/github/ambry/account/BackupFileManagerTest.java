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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link BackupFileManager}
 */
public class BackupFileManagerTest {
  private final AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
  private final Properties helixConfigProps = new Properties();
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 20000;
  private final int maxBackupFile = 10;
  private final Path accountBackupDir;
  private VerifiableProperties vHelixConfigProps;
  private HelixAccountServiceConfig config;
  private Account refAccount;

  /**
   * Constructor to create a BackupFileManagerTest
   * @throws IOException if I/O error occurs
   */
  public BackupFileManagerTest() throws IOException {
    accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    helixConfigProps.setProperty(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY, accountBackupDir.toString());
    helixConfigProps.setProperty(HelixAccountServiceConfig.MAX_BACKUP_FILE_COUNT, String.valueOf(maxBackupFile));
    helixConfigProps.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY,
        String.valueOf(ZK_CLIENT_CONNECTION_TIMEOUT_MS));
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    config = new HelixAccountServiceConfig(vHelixConfigProps);
    refAccount = createRandomAccount();
  }

  /**
   * Delete all the backup files and backup directory.
   * @throws IOException if I/O error occurs
   */
  @After
  public void cleanUp() throws IOException {
    if (Files.exists(accountBackupDir)) {
      Files.walk(accountBackupDir).map(Path::toFile).forEach(File::delete);
      Files.deleteIfExists(accountBackupDir);
    }
    assertFalse("Backup directory still exist", Files.exists(accountBackupDir));
  }

  /**
   * Test disable backup. Every method from {@link BackupFileManager} should return null or have no effect.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testDisableBackupDir() throws IOException {
    helixConfigProps.remove(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY);
    VerifiableProperties vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    HelixAccountServiceConfig config = new HelixAccountServiceConfig(vHelixConfigProps);
    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);

    // getLatestAccountMap should return null
    assertNull("Disabled backup shouldn't have any state", backup.getLatestAccountMap(0));

    // persistAccountMap should not create any backup
    Stat stat = new Stat();
    stat.setVersion(1);
    stat.setMtime(System.currentTimeMillis());

    backup.persistAccountMap(new HashMap<String, String>(), stat);
    assertTrue("Disabled backup shouldn't add any backup", backup.isEmpty());
  }

  /**
   * Test getLatestAccountMap function when all the backup files are files with version number.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testGetLatestState_BackupFilesAllWithVersion() throws IOException {
    final int startVersion = 1;
    final int endVersion = startVersion + maxBackupFile / 2;
    final long baseModifiedTime = System.currentTimeMillis() / 1000 - 100;
    final long interval = 2;
    String[] filenames =
        createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion, baseModifiedTime, interval, false);
    saveAccountsToFile(Collections.singleton(refAccount), accountBackupDir.resolve(filenames[filenames.length - 1]));

    Map<String, String> expected = new HashMap<>();
    expected.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());

    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    Map<String, String> obtained = backup.getLatestAccountMap(0);
    assertTwoStringMapsEqual(expected, obtained);
    assertFalse("There are backup files", backup.isEmpty());

    obtained = backup.getLatestAccountMap(System.currentTimeMillis() / 1000);
    assertNull("No backup file should have a modified time later than now", obtained);
    assertEquals(backup.size(), endVersion - startVersion + 1);
  }

  /**
   * Test getLatestAccountMap function when all the backup files are files without version number.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testGetLatestAccountMap_BackupFilesAllWithoutVersion() throws IOException {
    final long baseModifiedTime = System.currentTimeMillis() / 1000 - 100;
    final long interval = 2;
    final int count = maxBackupFile / 2;
    String[] filenames = createBackupFilesWithoutVersion(accountBackupDir, count, baseModifiedTime, interval, false);
    saveAccountsToFile(Collections.singleton(refAccount), accountBackupDir.resolve(filenames[filenames.length - 1]));

    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    Map<String, String> obtained = backup.getLatestAccountMap(0);
    assertNull("No state should be loaded from backup file from old format", obtained);
    assertFalse("Backup should not be empty since there are files from old format", backup.isEmpty());
    assertEquals(backup.size(), count);
  }

  /**
   * Test getLatestAccountMap function when some of the files are with version number and some are not.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testGetLatestAccountMap_MixedBackupFiles() throws IOException {
    final long baseModifiedTime = System.currentTimeMillis() / 1000 - 100;
    final long interval = 2;
    // Backup files without version should be created before files with version;
    final int backupFileNoVersionCount = 2;
    createBackupFilesWithoutVersion(accountBackupDir, backupFileNoVersionCount, baseModifiedTime, interval, false);
    final int startVersion = 1;
    final int endVersion = 3;
    String[] filenames = createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion,
        baseModifiedTime + 2 * backupFileNoVersionCount * interval, interval, false);
    saveAccountsToFile(Collections.singleton(refAccount), accountBackupDir.resolve(filenames[filenames.length - 1]));

    Map<String, String> expected = new HashMap<>();
    expected.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());

    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    Map<String, String> obtained = backup.getLatestAccountMap(0);
    assertTwoStringMapsEqual(expected, obtained);
    assertFalse("There are backup files", backup.isEmpty());
    assertEquals(backup.size(), endVersion - startVersion + 1 + backupFileNoVersionCount);
  }

  /**
   * Test if BackupFileManager correctly cleans up the backup files, when there are more than {@link #maxBackupFile} backup
   * files with version number.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testCleanup_EnoughBackupFilesWithVersion() throws IOException {
    final int startVersion = 1;
    final int endVersion = startVersion + maxBackupFile * 2;
    final long baseModifiedTime = System.currentTimeMillis() / 1000 - 100;
    final long interval = 2;
    // Create backup files with version number
    String[] filenames =
        createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion, baseModifiedTime, interval, false);
    saveAccountsToFile(Collections.singleton(refAccount), accountBackupDir.resolve(filenames[filenames.length - 1]));

    // Create temporary backup files with version number
    createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion, baseModifiedTime, interval, true);

    // Create backup files without version number
    createBackupFilesWithoutVersion(accountBackupDir, 10, baseModifiedTime, interval, false);
    createBackupFilesWithoutVersion(accountBackupDir, 10, baseModifiedTime, interval, true);

    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    assertEquals("Number of backup files mismatch", backup.size(), maxBackupFile);

    File[] remainingFiles = accountBackupDir.toFile().listFiles();
    assertEquals("Number of backup files in storage mismatch", remainingFiles.length, maxBackupFile);
    for (File file : remainingFiles) {
      // All the files should
      assertTrue("Filename " + file.getName() + " should follow version pattern ",
          BackupFileManager.versionFilenamePattern.matcher(file.getName()).find());
    }
  }

  /**
   * Test if BackupFileManager correctly cleans up the backup files, when there are less than {@link #maxBackupFile} backup
   * files with version number.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testCleanup_NotEnoughBackupFilesWithVersion() throws IOException {
    final int startVersion = 1;
    final int endVersion = startVersion + maxBackupFile / 2;
    final long baseModifiedTime = System.currentTimeMillis() / 1000 - 100;
    final long interval = 2;
    final int backupFileNoVersionCount = maxBackupFile;
    // Create backup files without version number
    createBackupFilesWithoutVersion(accountBackupDir, backupFileNoVersionCount, baseModifiedTime, interval, false);
    createBackupFilesWithoutVersion(accountBackupDir, backupFileNoVersionCount, baseModifiedTime, interval, true);
    // Create temporary backup files with version number
    createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion,
        baseModifiedTime + 2 * backupFileNoVersionCount * interval, interval, true);
    // Create backup files with version number
    String[] filenames = createBackupFilesWithVersion(accountBackupDir, startVersion, endVersion,
        baseModifiedTime + 2 * backupFileNoVersionCount * interval, interval, false);
    saveAccountsToFile(Collections.singleton(refAccount), accountBackupDir.resolve(filenames[filenames.length - 1]));

    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    assertEquals("Number of backup files mismatch", backup.size(), maxBackupFile);

    File[] remainingFiles = accountBackupDir.toFile().listFiles();
    assertEquals("Number of backup files in storage mismatch", remainingFiles.length, maxBackupFile);
    int versionFileCount = 0;
    int nonVersionFileCount = 0;
    for (File file : remainingFiles) {
      if (BackupFileManager.versionFilenamePattern.matcher(file.getName()).find()) {
        versionFileCount++;
      } else {
        nonVersionFileCount++;
      }
    }
    assertEquals("Number of backup files with version mismatch", versionFileCount, endVersion - startVersion + 1);
    assertEquals("Number of backup files without version ", nonVersionFileCount,
        maxBackupFile - (endVersion - startVersion + 1));
  }

  /**
   * Test {@link BackupFileManager#persistAccountMap(Map, Stat)} and then recover {@link BackupFileManager} from the same backup directory.
   * @throws IOException if I/O error occurs
   */
  @Test
  public void testPersistAccountMapAndRecover() throws IOException {
    BackupFileManager backup = new BackupFileManager(accountServiceMetrics, config);
    final int numberAccounts = maxBackupFile * 2;
    final Map<String, String> accounts = new HashMap<>(numberAccounts);
    final Stat stat = new Stat();
    final long interval = 1;
    long modifiedTime = System.currentTimeMillis() / 1000 - numberAccounts * 2;
    for (int i = 0; i < numberAccounts; i++) {
      Account account = createRandomAccount();
      accounts.put(String.valueOf(account.getId()), account.toJson(true).toString());
      stat.setVersion(i + 1);
      stat.setMtime(modifiedTime * 1000);
      backup.persistAccountMap(accounts, stat);
      modifiedTime += interval;
    }

    for (int i = 0; i < 2; i++) {
      assertEquals("Number of backup file mismatch", maxBackupFile, backup.size());
      Map<String, String> obtained = backup.getLatestAccountMap(0);
      assertTwoStringMapsEqual(accounts, obtained);
      File[] remaingingFiles = accountBackupDir.toFile().listFiles();
      assertEquals("Remaining backup mismatch", maxBackupFile, remaingingFiles.length);

      // Recover BackupFileManager from the same backup dir
      backup = new BackupFileManager(accountServiceMetrics, config);
    }
  }

  /**
   * Test serialization and deserialization of {@link Account} metadata.
   * @throws JSONException if {@link Account} can't convert to json
   */
  @Test
  public void testSerializationAndDeserialization() throws JSONException {
    // Since the account metadata is stored in an map from string to string, so here we only test map<string, string>
    Map<String, String> state = new HashMap<>();
    ByteBuffer buffer = BackupFileManager.serializeAccountMap(state);
    Map<String, String> deserializedState = BackupFileManager.deserializeAccountMap(buffer.array());
    assertTwoStringMapsEqual(state, deserializedState);

    // Put a real account's json string
    state.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());
    buffer = BackupFileManager.serializeAccountMap(state);
    deserializedState = BackupFileManager.deserializeAccountMap(buffer.array());
    assertTwoStringMapsEqual(state, deserializedState);
  }

  /**
   * Assert the given two maps equal to each other.
   * @param m1 First map.
   * @param m2 Second map.
   */
  private void assertTwoStringMapsEqual(Map<String, String> m1, Map<String, String> m2) {
    assertEquals("Two maps don't have the same size", m1.size(), m2.size());
    for (Map.Entry<String, String> entry : m1.entrySet()) {
      assertTrue(entry.getKey() + " doesn't exist in second map", m2.containsKey(entry.getKey()));
      assertEquals("Values of key " + entry.getKey() + " differ", entry.getValue(), m2.get(entry.getKey()));
    }
  }

  /**
   * Create backup files with version number without any content and return all the filenames.
   * @param backupDir The directory to create files under.
   * @param startVersion The starting version, inclusive.
   * @param endVersion The ending version, inclusive.
   * @param baseModifiedTime The starting version file's modified time.
   * @param interval The interval by which different backup files' modified time increases.
   * @param isTemp True if all the backup files should be temporary files.
   * @return Array of filenames.
   */
  private String[] createBackupFilesWithVersion(Path backupDir, int startVersion, int endVersion, long baseModifiedTime,
      long interval, boolean isTemp) {
    String[] filenames = new String[endVersion - startVersion + 1];
    for (int i = startVersion; i <= endVersion; i++) {
      Stat stat = new Stat();
      stat.setVersion(i);
      stat.setMtime((baseModifiedTime + (i - startVersion) * interval) * 1000);
      String filename = BackupFileManager.getBackupFilenameFromStat(stat);
      if (isTemp) {
        filename = filename + BackupFileManager.SEP + BackupFileManager.TEMP_FILE_SUFFIX;
      }
      try {
        Files.createFile(backupDir.resolve(filename));
        filenames[i - startVersion] = filename;
      } catch (IOException e) {
        fail("Fail to create file " + filename);
      }
    }
    return filenames;
  }

  /**
   * Create backup files in old format, without version number.
   * @param backupDir The directory to create files under.
   * @param count The number of backup files to create.
   * @param baseModifiedTime The first file's modified time.
   * @param interval The interval by which different backup files' modified time increases.
   * @param isOld True if all backup files should be suffix with .old.
   * @return Array of filenames.
   */
  private String[] createBackupFilesWithoutVersion(Path backupDir, int count, long baseModifiedTime, long interval,
      boolean isOld) {
    String[] filenames = new String[count];
    for (int i = 0; i < count; i++) {
      long modifiedTime = baseModifiedTime + i * interval;
      String timestamp = LocalDateTime.ofEpochSecond(modifiedTime, 0, BackupFileManager.zoneOffset)
          .format(BackupFileManager.TIMESTAMP_FORMATTER);
      String filename = timestamp + BackupFileManager.SEP + BackupFileManager.NEW_STATE_SUFFIX;
      if (isOld) {
        filename = timestamp + BackupFileManager.SEP + BackupFileManager.OLD_STATE_SUFFIX;
      }
      try {
        Files.createFile(backupDir.resolve(filename));
        filenames[i] = filename;
      } catch (IOException e) {
        fail("Fail to create file " + filename);
      }
    }
    return filenames;
  }

  /**
   * Create an random {@link Account} with one random {@link Container}.
   * @return The {@link Account} just created.
   */
  private Account createRandomAccount() {
    final Random random = new Random();
    Short accountID = Utils.getRandomShort(random);
    String accountName = UUID.randomUUID().toString();
    Short containerID = Utils.getRandomShort(random);
    String containerName = UUID.randomUUID().toString();
    String containerDescription = UUID.randomUUID().toString();
    boolean containerCaching = random.nextBoolean();
    boolean containerEncryption = random.nextBoolean();
    boolean containerPreviousEncryption = containerEncryption || random.nextBoolean();
    boolean containerMediaScanDisabled = random.nextBoolean();
    String replicationPolicy = TestUtils.getRandomString(10);
    boolean containerTtlRequired = random.nextBoolean();
    Container container =
        new ContainerBuilder(containerID, containerName, Container.ContainerStatus.ACTIVE, containerDescription,
            accountID).setEncrypted(containerEncryption)
            .setPreviouslyEncrypted(containerPreviousEncryption)
            .setCacheable(containerCaching)
            .setMediaScanDisabled(containerMediaScanDisabled)
            .setReplicationPolicy(replicationPolicy)
            .setTtlRequired(containerTtlRequired)
            .build();
    return new AccountBuilder(accountID, accountName, Account.AccountStatus.ACTIVE).addOrUpdateContainer(container)
        .build();
  }

  /**
   * Save the collection of {@link Account} to the given file.
   * @param accounts The collection of {@link Account}.
   * @param filePath The given file.
   */
  private static void saveAccountsToFile(Collection<Account> accounts, Path filePath) {
    Map<String, String> accountMap = new HashMap<>();
    try {
      accounts.stream()
          .forEach((account) -> accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString()));
    } catch (JSONException e) {
      fail("Fail to get a json format of accounts");
    }
    try {
      BackupFileManager.writeAccountMapToFile(filePath, accountMap);
    } catch (Exception e) {
      fail("Fail to write state to file: " + filePath.toString());
    }
  }
}
