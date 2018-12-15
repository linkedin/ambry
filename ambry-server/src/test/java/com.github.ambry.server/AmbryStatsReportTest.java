/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.MockTime;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


public class AmbryStatsReportTest {
  private static final long AGGREGATE_INTERVAL_MINS = 60;

  @Test
  public void testAmbryStatsReport() throws StoreException, IOException {
    StatsManagerConfig config = new StatsManagerConfig(new VerifiableProperties(new Properties()));
    StatsManager testStatsManager =
        new StatsManager(new StatsManagerTest.MockStorageManager(Collections.emptyMap()), Collections.emptyList(),
            new MetricRegistry(), config, new MockTime());
    // test account stats report
    AmbryStatsReport ambryStatsReport =
        new AmbryStatsReport(testStatsManager, AGGREGATE_INTERVAL_MINS, StatsReportType.ACCOUNT_REPORT);
    assertEquals("Mismatch in aggregation time interval", AGGREGATE_INTERVAL_MINS,
        ambryStatsReport.getAggregateIntervalInMinutes());
    assertEquals("Mismatch in report name", "AccountReport", ambryStatsReport.getReportName());
    assertEquals("Mismatch in stats field name", "AccountStats", ambryStatsReport.getStatsFieldName());

    // test partition class stats report
    ambryStatsReport =
        new AmbryStatsReport(testStatsManager, AGGREGATE_INTERVAL_MINS, StatsReportType.PARTITION_CLASS_REPORT);
    assertEquals("Mismatch in aggregation time interval", AGGREGATE_INTERVAL_MINS,
        ambryStatsReport.getAggregateIntervalInMinutes());
    assertEquals("Mismatch in report name", "PartitionClassReport", ambryStatsReport.getReportName());
    assertEquals("Mismatch in stats field name", "PartitionClassStats", ambryStatsReport.getStatsFieldName());
  }
}
