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
package com.github.ambry.quota;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.mysql.AccountReportsDao;
import com.github.ambry.server.mysql.AccountStatsMySqlStore;
import com.github.ambry.server.mysql.AccountStatsMySqlStoreFactory;
import com.github.ambry.server.mysql.AggregatedAccountReportsDao;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration test for {@link MySqlStorageUsageRefresher}.
 * It's also the unit test. The reason to have unit test here is we can only be sure mysql server is running
 * in integration test.
 */
public class MySqlStorageUsageRefresherTest {
  private static final String CLUSTER_NAME = "Ambry-test";
  private static final String HOSTNAME = "ambry1.github.com";
  private static final int SERVER_PORT = 12345;
  private static final int MYSQL_RETRY_BACKOFF_MS = 2000;

  private Properties properties;
  private static ScheduledExecutorService scheduler = Utils.newScheduler(1, "storage-usage-refresher", false);
  private final AccountStatsMySqlStore accountStatsMySqlStore;

  public MySqlStorageUsageRefresherTest() throws Exception {
    properties = createProperties();
    accountStatsMySqlStore = createAccountStatsMySqlStore();
  }

  @Before
  public void before() throws Exception {
    cleanup(accountStatsMySqlStore.getMySqlDataAccessor());
  }

  @AfterClass
  public static void afterClass() {
    scheduler.shutdownNow();
  }

  /**
   * Test backup file manager.
   * @throws Exception
   */
  @Test
  public void testBackupFileManager() throws Exception {
    Path tempDir = Files.createTempDirectory("MySqlStorageUsageRefresherTest");
    Path localBackupFilePath = tempDir.resolve("backupfilemanager");
    MySqlStorageUsageRefresher.BackupFileManager manager =
        new MySqlStorageUsageRefresher.BackupFileManager(localBackupFilePath.toString());

    // It shouldn't have any backup file now.
    assertTrue(manager.getBackupFiles().isEmpty());
    assertNull(manager.getBackupFileContent("2020-01"));

    // persist file
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    manager.persistentBackupFile("2020-01", containerStorageUsages);
    assertEquals(manager.getBackupFiles().size(), 1);
    // persist same file again
    manager.persistentBackupFile("2020-01", containerStorageUsages);
    assertEquals(manager.getBackupFiles().size(), 1);
    try {
      manager.persistentBackupFile("badfilename", containerStorageUsages);
      fail("should fail due to bad filename");
    } catch (IllegalArgumentException e) {
    }
    try {
      manager.persistentBackupFile("2020-02", null);
      fail("should fail due to bad usage");
    } catch (IllegalArgumentException e) {
    }

    manager.persistentBackupFile("2020-02", containerStorageUsages);

    // Now we have two backup files
    // Create a temp file
    Path tmpFilePath =
        localBackupFilePath.resolve("2020-03" + MySqlStorageUsageRefresher.BackupFileManager.TEMP_FILE_SUFFIX);
    Files.createFile(tmpFilePath);
    manager = new MySqlStorageUsageRefresher.BackupFileManager(localBackupFilePath.toString());
    assertEquals(manager.getBackupFiles().size(), 2);
    TestUtils.assertContainerMap(containerStorageUsages, manager.getBackupFileContent("2020-01"));
    assertFalse(Files.exists(tmpFilePath));
  }

  /**
   * Tests to start refresher with or without back up files.
   * @throws Exception
   */
  @Test
  public void testStartRefresher() throws Exception {
    // Store something to mysql database as container usage and monthly container usage
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
    accountStatsMySqlStore.storeAggregatedStats(snapshot);
    accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME,
        MySqlStorageUsageRefresher.getCurrentMonth());

    StorageQuotaConfig storageQuotaConfig = new StorageQuotaConfig(new VerifiableProperties(properties));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MySqlStorageUsageRefresher refresher =
        new MySqlStorageUsageRefresher(accountStatsMySqlStore, scheduler, storageQuotaConfig, clusterMapConfig);

    // we should get an container storage usage full of zero
    Map<String, Map<String, Long>> usage = refresher.getContainerStorageUsage();
    assertContainerUsageMapAllZero(usage);

    // we should have backup files now
    Path backupDirPath = Paths.get(properties.getProperty(StorageQuotaConfig.BACKUP_FILE_DIR));
    Path backupFilePath = backupDirPath.resolve(MySqlStorageUsageRefresher.getCurrentMonth());
    Map<String, Map<String, Long>> backupContainerStorageUsages =
        new ObjectMapper().readValue(backupFilePath.toFile(), new TypeReference<Map<String, Map<String, Long>>>() {
        });
    TestUtils.assertContainerMap(containerStorageUsages, backupContainerStorageUsages);

    // recreate a refresher, but change the monthly container usages, new refresher should load it from backup
    containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    accountStatsMySqlStore.storeAggregatedStats(
        TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages));
    accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME,
        MySqlStorageUsageRefresher.getCurrentMonth());
    refresher = new MySqlStorageUsageRefresher(accountStatsMySqlStore, scheduler, storageQuotaConfig, clusterMapConfig);
    Map<String, Map<String, Long>> currentMonthlyStorageUsages = refresher.getContainerStorageUsageMonthlyBase();
    TestUtils.assertContainerMap(backupContainerStorageUsages, currentMonthlyStorageUsages);
  }

  /**
   * Test {@link MySqlStorageUsageRefresher#subtract} method.
   */
  @Test
  public void testMapSubtract() {
    // Subtract it's own clone should result in all 0 values
    Map<String, Map<String, Long>> storageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    Map<String, Map<String, Long>> clone = cloneMap(storageUsages);
    MySqlStorageUsageRefresher.subtract(storageUsages, clone);
    assertContainerUsageMapAllZero(storageUsages);

    // Base has more values, but it will not impact anything
    storageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    clone = cloneMap(storageUsages);
    Map<String, Map<String, Long>> extraAccountMap = TestUtils.makeStorageMap(1, 1, 100000, 1000);
    clone.put("11", extraAccountMap.get("1"));
    clone.get("1").put("11", 1000L);
    MySqlStorageUsageRefresher.subtract(storageUsages, clone);
    assertContainerUsageMapAllZero(storageUsages);

    // Base has less value, the extra values will be kept
    storageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    clone = cloneMap(storageUsages);
    clone.remove("10");
    clone.get("1").remove("10");
    MySqlStorageUsageRefresher.subtract(storageUsages, clone);
    Map<String, Long> partialUsageMap = storageUsages.remove("10");
    long usage = storageUsages.get("1").remove("10");
    assertEquals(10, partialUsageMap.values().stream().filter(v -> v >= 1000 && v <= 100000).count());
    assertTrue(usage >= 1000 && usage <= 100000);
    assertContainerUsageMapAllZero(storageUsages);

    // Base's values are larger than the storage
    storageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    clone = cloneMap(storageUsages);
    clone.values().forEach(m -> m.keySet().forEach(k -> m.put(k, m.get(k) + 1)));
    MySqlStorageUsageRefresher.subtract(storageUsages, clone);
    assertContainerUsageMapAllZero(storageUsages);
  }

  /**
   * Test when updating container total usage.
   * @throws Exception
   */
  @Test
  public void testRefresherUpdateAndListener() throws Exception {
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
    accountStatsMySqlStore.storeAggregatedStats(snapshot);
    accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME,
        MySqlStorageUsageRefresher.getCurrentMonth());

    // Set polling interval to 2 seconds
    properties.setProperty(StorageQuotaConfig.REFRESHER_POLLING_INTERVAL_MS, "2000");
    StorageQuotaConfig storageQuotaConfig = new StorageQuotaConfig(new VerifiableProperties(properties));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MySqlStorageUsageRefresher refresher =
        new MySqlStorageUsageRefresher(accountStatsMySqlStore, scheduler, storageQuotaConfig, clusterMapConfig);

    AtomicReference<Map<String, Map<String, Long>>> containerUsageRef = new AtomicReference<>(null);
    AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(null);
    refresher.registerListener(containerStorageUsage -> {
      containerUsageRef.set(containerStorageUsage);
      latchRef.get().countDown();
    });
    // Keep storage usage unchanged, listener should get an all-zero map
    accountStatsMySqlStore.storeAggregatedStats(
        TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages));
    CountDownLatch latch1 = new CountDownLatch(1);
    latchRef.set(latch1);
    latch1.await(10, TimeUnit.SECONDS);
    assertContainerUsageMapAllZero(containerUsageRef.get());

    // Change some usage, listener should get
    containerStorageUsages.get("1").compute("1", (k, v) -> v + 1L);
    accountStatsMySqlStore.storeAggregatedStats(
        TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages));
    CountDownLatch latch2 = new CountDownLatch(1);
    latchRef.set(latch2);
    latch2.await(10, TimeUnit.SECONDS);
    assertEquals((long) containerUsageRef.get().get("1").get("1"), 1L);
    Map<String, Map<String, Long>> clone = cloneMap(containerUsageRef.get());
    clone.get("1").put("1", 0L);
    assertContainerUsageMapAllZero(clone);
  }

  /**
   * Test to update container storage usage monthly base.
   */
  @Test
  public void testFetchMonthlyStorageUsage() throws Exception {
    MockTime mockTime = new MockTime(SystemTime.getInstance().milliseconds());
    MySqlStorageUsageRefresher.time = mockTime;
    try {
      String currentMonth = MySqlStorageUsageRefresher.getCurrentMonth();
      Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);

      accountStatsMySqlStore.storeAggregatedStats(snapshot);
      accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME, currentMonth);
      properties.remove(StorageQuotaConfig.REFRESHER_POLLING_INTERVAL_MS);
      StorageQuotaConfig storageQuotaConfig = new StorageQuotaConfig(new VerifiableProperties(properties));
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
      MySqlStorageUsageRefresher refresher =
          new MySqlStorageUsageRefresher(accountStatsMySqlStore, scheduler, storageQuotaConfig, clusterMapConfig);

      // Fetch monthly storage usage
      refresher.fetchStorageUsageMonthlyBase();
      TestUtils.assertContainerMap(containerStorageUsages, refresher.getContainerStorageUsageMonthlyBase());

      // Change the month
      String notCurrentMonth = "1970-01";
      Map<String, Map<String, Long>> newContainerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(newContainerStorageUsages);
      accountStatsMySqlStore.storeAggregatedStats(snapshot);
      accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME, notCurrentMonth);
      refresher.fetchStorageUsageMonthlyBase();
      // Monthly storage usage still the old one
      TestUtils.assertContainerMap(containerStorageUsages, refresher.getContainerStorageUsageMonthlyBase());

      // Change the month back to the current month
      accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME, currentMonth);
      // Wait for schedule to retry
      Thread.sleep(MYSQL_RETRY_BACKOFF_MS * 2);
      TestUtils.assertContainerMap(newContainerStorageUsages, refresher.getContainerStorageUsageMonthlyBase());

      // Forward the time to next month
      mockTime.sleep((MySqlStorageUsageRefresher.secondsToNextMonthTick(currentMonth,
          storageQuotaConfig.mysqlMonthlyBaseFetchOffsetSec) + 10) * 1000);
      String nextMonth = MySqlStorageUsageRefresher.getCurrentMonth();

      Function<String, Integer> stringMonthToInteger = (monthInStr) -> {
        String[] parts = monthInStr.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        return year * 12 + month;
      };
      assertEquals(stringMonthToInteger.apply(currentMonth) + 1, (int) stringMonthToInteger.apply(nextMonth));
      // Update the month to next month
      Map<String, Map<String, Long>> nextContainerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(nextContainerStorageUsages);
      accountStatsMySqlStore.storeAggregatedStats(snapshot);
      accountStatsMySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(CLUSTER_NAME, nextMonth);
      refresher.fetchStorageUsageMonthlyBase();
      TestUtils.assertContainerMap(nextContainerStorageUsages, refresher.getContainerStorageUsageMonthlyBase());
      // A backup file should be create as well
      TestUtils.assertContainerMap(nextContainerStorageUsages,
          refresher.getBackupFileManager().getBackupFileContent(nextMonth));
    } finally {
      MySqlStorageUsageRefresher.time = SystemTime.getInstance();
    }
  }

  /**
   * clean up the tables in database.
   */
  private void cleanup(MySqlDataAccessor dataAccessor) throws SQLException {
    Connection dbConnection = dataAccessor.getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    statement.executeUpdate("DELETE FROM " + AccountReportsDao.ACCOUNT_REPORTS_TABLE);
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE);
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE);
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE);
  }

  private Properties createProperties() throws Exception {
    Path tempDir = Files.createTempDirectory("MySqlStorageUsageRefresherTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");
    Properties configProps = Utils.loadPropsFromResource("mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, CLUSTER_NAME);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, HOSTNAME);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(SERVER_PORT));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "localhost:1234");
    configProps.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    configProps.setProperty(StorageQuotaConfig.BACKUP_FILE_DIR, localBackupFilePath.toString());
    configProps.setProperty(StorageQuotaConfig.MYSQL_STORE_RETRY_BACKOFF_MS, String.valueOf(MYSQL_RETRY_BACKOFF_MS));
    configProps.setProperty(StorageQuotaConfig.MYSQL_STORE_RETRY_MAX_COUNT, "1");
    return configProps;
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore() throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new AccountStatsMySqlStoreFactory(verifiableProperties, new ClusterMapConfig(verifiableProperties),
        new StatsManagerConfig(verifiableProperties), new MetricRegistry()).getAccountStatsMySqlStore();
  }

  private Map<String, Map<String, Long>> cloneMap(Map<String, Map<String, Long>> origin) {
    if (origin == null) {
      return null;
    }
    Map<String, Map<String, Long>> clone = new HashMap<>();
    origin.entrySet().forEach(ent -> clone.put(ent.getKey(), new HashMap<>(ent.getValue())));
    return clone;
  }

  private void assertContainerUsageMapAllZero(Map<String, Map<String, Long>> usageMap) {
    boolean allZero =
        usageMap.values().stream().flatMapToLong(m -> m.values().stream().mapToLong(v -> v)).allMatch(v -> v == 0);
    assertTrue(allZero);
  }
}
