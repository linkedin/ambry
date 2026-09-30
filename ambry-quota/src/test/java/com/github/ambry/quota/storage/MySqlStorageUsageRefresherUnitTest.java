/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.accountstats.AggregatedAccountReportsState;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for recovered monthly snapshot propagation in {@link MySqlStorageUsageRefresher}.
 */
public class MySqlStorageUsageRefresherUnitTest {
  @Test
  public void testSnapshotVersionChangeRefreshesMonthlyBase() throws Exception {
    String currentMonth = MySqlStorageUsageRefresher.getCurrentMonth();
    AggregatedAccountStorageStats initialStats = createStats(10);
    AggregatedAccountStorageStats recoveredStats = createStats(20);
    AggregatedAccountReportsState initialState =
        new AggregatedAccountReportsState(currentMonth, 1000L, null, 1);
    AggregatedAccountReportsState recoveredState =
        new AggregatedAccountReportsState(currentMonth, 2000L, null, 2);
    AccountStatsStore store = mock(AccountStatsStore.class);
    when(store.queryMonthlyAggregatedAccountStorageStatsAndState()).thenReturn(new Pair<>(initialStats, initialState),
        new Pair<>(recoveredStats, recoveredState));
    when(store.queryAggregatedAccountReportsState()).thenReturn(initialState, recoveredState);
    when(store.queryAggregatedAccountStorageStats()).thenReturn(initialStats);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    Path backupDir = Files.createTempDirectory("versioned-monthly-backup");
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.BACKUP_FILE_DIR, backupDir.toString());
    StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(properties));

    MySqlStorageUsageRefresher refresher =
        new MySqlStorageUsageRefresher(store, scheduler, config,
            new StorageQuotaServiceMetrics(new MetricRegistry()));
    assertEquals(Long.valueOf(10), refresher.getContainerStorageUsageMonthlyBase().get("1").get("1"));

    Path backupFile = backupDir.resolve(currentMonth);
    Files.delete(backupFile);
    Files.createDirectory(backupFile);
    refresher.fetchStorageUsageMonthlyBase();
    assertEquals(Long.valueOf(20), refresher.getContainerStorageUsageMonthlyBase().get("1").get("1"));

    Files.delete(backupFile);
    refresher.fetchStorageUsageMonthlyBase();

    assertEquals(MySqlStorageUsageRefresher.convertAggregatedAccountStorageStatsToMap(recoveredStats, false),
        refresher.getBackupFileManager().getBackupFileContent(currentMonth));
    verify(store, times(2)).queryMonthlyAggregatedAccountStorageStatsAndState();
  }

  @Test
  public void testMonthChangeRefreshesWhenLegacyVersionIsUnchanged() throws Exception {
    long augustTimeMs =
        LocalDateTime.of(2026, 8, 31, 23, 0).toInstant(MySqlStorageUsageRefresher.ZONE_OFFSET).toEpochMilli();
    MockTime mockTime = new MockTime(augustTimeMs);
    MySqlStorageUsageRefresher.time = mockTime;
    try {
      AggregatedAccountStorageStats augustStats = createStats(10);
      AggregatedAccountStorageStats septemberStats = createStats(20);
      AggregatedAccountReportsState augustState =
          new AggregatedAccountReportsState("2026-08", augustTimeMs, null, 1);
      AggregatedAccountReportsState septemberState =
          new AggregatedAccountReportsState("2026-09", augustTimeMs + TimeUnit.HOURS.toMillis(2), null, 1);
      AccountStatsStore store = mock(AccountStatsStore.class);
      when(store.queryMonthlyAggregatedAccountStorageStatsAndState()).thenReturn(new Pair<>(augustStats, augustState),
          new Pair<>(septemberStats, septemberState));
      when(store.queryAggregatedAccountReportsState()).thenReturn(augustState, septemberState);
      when(store.queryAggregatedAccountStorageStats()).thenReturn(augustStats);
      MySqlStorageUsageRefresher refresher =
          new MySqlStorageUsageRefresher(store, mock(ScheduledExecutorService.class),
              new StorageQuotaConfig(new VerifiableProperties(new Properties())),
              new StorageQuotaServiceMetrics(new MetricRegistry()));

      mockTime.sleep(TimeUnit.HOURS.toMillis(2));
      refresher.fetchStorageUsageMonthlyBase();

      assertEquals(Long.valueOf(20), refresher.getContainerStorageUsageMonthlyBase().get("1").get("1"));
      verify(store, times(2)).queryMonthlyAggregatedAccountStorageStatsAndState();
    } finally {
      MySqlStorageUsageRefresher.time = SystemTime.getInstance();
    }
  }

  @Test
  public void testBackupForSameMonthIsAtomicallyReplaced() throws Exception {
    Path backupPath = Files.createTempDirectory("monthly-backup").resolve("storage-usage");
    MySqlStorageUsageRefresher.BackupFileManager manager =
        new MySqlStorageUsageRefresher.BackupFileManager(backupPath.toString());
    Map<String, Map<String, Long>> initialUsage = TestUtils.makeStorageMap(2, 2, 100, 10);
    Map<String, Map<String, Long>> recoveredUsage = deepCopy(initialUsage);
    recoveredUsage.get("1").put("1", recoveredUsage.get("1").get("1") + 1);

    manager.persistentBackupFile("2026-09", initialUsage);
    manager.persistentBackupFile("2026-09", recoveredUsage);

    assertEquals(recoveredUsage, manager.getBackupFileContent("2026-09"));
    assertEquals(1, manager.getBackupFiles().size());
  }

  private AggregatedAccountStorageStats createStats(long usage) {
    AggregatedAccountStorageStats stats = new AggregatedAccountStorageStats(null);
    stats.addContainerStorageStats((short) 1, new ContainerStorageStats((short) 1, usage, usage, 1));
    return stats;
  }

  private Map<String, Map<String, Long>> deepCopy(Map<String, Map<String, Long>> source) {
    Map<String, Map<String, Long>> copy = new HashMap<>();
    source.forEach((account, usage) -> copy.put(account, new HashMap<>(usage)));
    return copy;
  }
}
