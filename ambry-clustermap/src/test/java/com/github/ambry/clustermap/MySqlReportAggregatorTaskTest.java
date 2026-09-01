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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.accountstats.AggregatedAccountReportsState;
import com.github.ambry.accountstats.InmemoryAccountStatsStore;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.MockTime;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests monthly snapshot recovery in {@link MySqlReportAggregatorTask}.
 */
public class MySqlReportAggregatorTaskTest {
  private static final String CURRENT_MONTH = "2026-09";
  private static final String PREVIOUS_MONTH = "2026-08";
  private static final long CURRENT_TIME_MS =
      LocalDateTime.of(2026, 9, 1, 12, 0).toInstant(MySqlReportAggregatorTask.ZONE_OFFSET).toEpochMilli();

  @Test
  public void testNormalMonthRollover() throws Exception {
    InmemoryAccountStatsStore store = createStore(PREVIOUS_MONTH, CURRENT_TIME_MS - TimeUnit.HOURS.toMillis(1), null);
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());

    AggregatedAccountReportsState state = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, state.getMonth());
    assertNull(state.getMonthlyBaselineRecoveryMonth());
    assertEquals(2, state.getSnapshotVersion());
  }

  @Test
  public void testLongGapRecoveryAtMonthRolloverIsIdempotent() throws Exception {
    long thresholdMs = TimeUnit.HOURS.toMillis(16);
    InmemoryAccountStatsStore store = createStore(PREVIOUS_MONTH, CURRENT_TIME_MS - thresholdMs, null);
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());
    AggregatedAccountReportsState deferredState = store.queryAggregatedAccountReportsState();
    assertEquals(PREVIOUS_MONTH, deferredState.getMonth());
    assertEquals(CURRENT_MONTH, deferredState.getMonthlyBaselineRecoveryMonth());
    assertEquals(1, deferredState.getSnapshotVersion());

    task.updateMonthlySnapshot(deferredState);
    AggregatedAccountReportsState recoveredState = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, recoveredState.getMonth());
    assertNull(recoveredState.getMonthlyBaselineRecoveryMonth());
    assertEquals(2, recoveredState.getSnapshotVersion());

    task.updateMonthlySnapshot(recoveredState);
    assertEquals(2, store.queryAggregatedAccountReportsState().getSnapshotVersion());
  }

  @Test
  public void testGapBelowThresholdDoesNotDeferRollover() throws Exception {
    long belowThresholdMs = TimeUnit.HOURS.toMillis(16) - 1;
    InmemoryAccountStatsStore store = createStore(PREVIOUS_MONTH, CURRENT_TIME_MS - belowThresholdMs, null);
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());

    AggregatedAccountReportsState state = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, state.getMonth());
    assertNull(state.getMonthlyBaselineRecoveryMonth());
    assertEquals(2, state.getSnapshotVersion());
  }

  @Test
  public void testSameMonthGapDoesNotResetBaseline() throws Exception {
    long laterCurrentTimeMs = CURRENT_TIME_MS + TimeUnit.DAYS.toMillis(1);
    InmemoryAccountStatsStore store =
        createStore(CURRENT_MONTH, CURRENT_TIME_MS, null);
    MySqlReportAggregatorTask task = createTask(store, laterCurrentTimeMs);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());

    AggregatedAccountReportsState state = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, state.getMonth());
    assertNull(state.getMonthlyBaselineRecoveryMonth());
    assertEquals(1, state.getSnapshotVersion());
  }

  @Test
  public void testLegacyMonthRolloverArmsRecovery() throws Exception {
    InmemoryAccountStatsStore store =
        createStore(CURRENT_MONTH, CURRENT_TIME_MS - TimeUnit.HOURS.toMillis(24), null);
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());
    AggregatedAccountReportsState deferredState = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, deferredState.getMonth());
    assertEquals(CURRENT_MONTH, deferredState.getMonthlyBaselineRecoveryMonth());
    assertEquals(1, deferredState.getSnapshotVersion());

    task.updateMonthlySnapshot(deferredState);
    AggregatedAccountReportsState recoveredState = store.queryAggregatedAccountReportsState();
    assertNull(recoveredState.getMonthlyBaselineRecoveryMonth());
    assertEquals(2, recoveredState.getSnapshotVersion());
  }

  @Test
  public void testUninitializedStateDoesNotTriggerRecovery() throws Exception {
    InmemoryAccountStatsStore store = new InmemoryAccountStatsStore("test", "localhost");
    store.storeAggregatedAccountStorageStats(new AggregatedAccountStorageStats());
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(store.queryAggregatedAccountReportsState());

    AggregatedAccountReportsState state = store.queryAggregatedAccountReportsState();
    assertEquals(CURRENT_MONTH, state.getMonth());
    assertNull(state.getMonthlyBaselineRecoveryMonth());
    assertEquals(1, state.getSnapshotVersion());
  }

  @Test
  public void testStateQueryFailureDoesNotSuppressCurrentAggregation() throws Exception {
    AccountStatsStore store = mock(AccountStatsStore.class);
    when(store.queryAggregatedAccountReportsState()).thenThrow(new SQLException("state unavailable"));
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getInstancesInCluster("test")).thenReturn(Collections.emptyList());
    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("test");
    when(manager.getClusterManagmentTool()).thenReturn(helixAdmin);
    MySqlReportAggregatorTask task =
        new MySqlReportAggregatorTask(manager, 60, StatsReportType.ACCOUNT_REPORT, store, null, createConfig(),
            new MetricRegistry(), new MockTime(CURRENT_TIME_MS));

    assertEquals(org.apache.helix.task.TaskResult.Status.FAILED, task.run().getStatus());
    verify(store).storeAggregatedAccountStorageStats(any(AggregatedAccountStorageStats.class));
    verify(store, never()).updateAggregatedAccountReportsState(any(), anyString(), anyLong(), any(), anyBoolean(),
        anyBoolean());
  }

  @Test
  public void testStaleConcurrentTaskCannotRearmRecovery() throws Exception {
    AccountStatsStore store = mock(AccountStatsStore.class);
    AggregatedAccountReportsState staleState =
        new AggregatedAccountReportsState(PREVIOUS_MONTH, CURRENT_TIME_MS - TimeUnit.HOURS.toMillis(20), null, 1);
    AggregatedAccountReportsState recoveredState =
        new AggregatedAccountReportsState(CURRENT_MONTH, CURRENT_TIME_MS - 1, null, 2);
    when(store.updateAggregatedAccountReportsState(eq(staleState), anyString(), anyLong(), any(), anyBoolean(),
        anyBoolean())).thenReturn(false);
    when(store.updateAggregatedAccountReportsState(eq(recoveredState), anyString(), anyLong(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    when(store.queryAggregatedAccountReportsState()).thenReturn(recoveredState);
    MySqlReportAggregatorTask task = createTask(store);

    task.updateMonthlySnapshot(staleState);

    verify(store, times(1)).updateAggregatedAccountReportsState(eq(staleState), eq(CURRENT_MONTH),
        eq(CURRENT_TIME_MS), eq(CURRENT_MONTH), eq(false), anyBoolean());
    verify(store).updateAggregatedAccountReportsState(eq(recoveredState), eq(CURRENT_MONTH), eq(CURRENT_TIME_MS),
        isNull(), eq(false), anyBoolean());
  }

  private InmemoryAccountStatsStore createStore(String month, long lastAggregationTimeMs, String recoveryMonth)
      throws Exception {
    InmemoryAccountStatsStore store = new InmemoryAccountStatsStore("test", "localhost");
    store.storeAggregatedAccountStorageStats(new AggregatedAccountStorageStats());
    AggregatedAccountReportsState emptyState = store.queryAggregatedAccountReportsState();
    store.updateAggregatedAccountReportsState(emptyState, month, lastAggregationTimeMs, recoveryMonth, true, false);
    return store;
  }

  private MySqlReportAggregatorTask createTask(AccountStatsStore store) {
    return createTask(store, CURRENT_TIME_MS);
  }

  private MySqlReportAggregatorTask createTask(AccountStatsStore store, long currentTimeMs) {
    return new MySqlReportAggregatorTask(null, 60, StatsReportType.ACCOUNT_REPORT, store, null, createConfig(),
        new MetricRegistry(), new MockTime(currentTimeMs));
  }

  private ClusterMapConfig createConfig() {
    Properties properties = new Properties();
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, "test");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, "12345");
    properties.setProperty(ClusterMapConfig.ENABLE_AGGREGATED_MONTHLY_ACCOUNT_REPORT, "true");
    return new ClusterMapConfig(new VerifiableProperties(properties));
  }
}
