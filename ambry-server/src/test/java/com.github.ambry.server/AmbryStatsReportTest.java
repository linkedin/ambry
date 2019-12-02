/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.MockTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


public class AmbryStatsReportTest {
  private static final long AGGREGATE_INTERVAL_MINS = 60;

  @Test
  public void testAmbryStatsReport() throws Exception {
    StatsManagerConfig config = new StatsManagerConfig(new VerifiableProperties(new Properties()));
    StatsManager testStatsManager = new StatsManager(new MockStorageManager(Collections.emptyMap(),
        new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
            Collections.singletonList("/tmp"), "DC1")), Collections.emptyList(), new MetricRegistry(), config,
        new MockTime(), null);
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

  @Test
  public void testStatsToPublishConfig() {
    // use the same logic/code in AmbryServer to test that server only accepts valid stats types and won't fail on startup.
    Set<String> validStatsTypes = new HashSet<>();
    for (StatsReportType type : StatsReportType.values()) {
      validStatsTypes.add(type.toString());
    }
    List<String> acceptedStatsTypes = new ArrayList<>();
    String statsReportsToPublishStr = "";
    List<String> statsReportsTypes = Arrays.asList(statsReportsToPublishStr.split(","));
    statsReportsTypes.forEach(e -> {
      if (validStatsTypes.contains(e)) {
        acceptedStatsTypes.add(e);
      }
    });
    assertTrue("The accepted stats type list should be empty", acceptedStatsTypes.isEmpty());
    statsReportsToPublishStr = "ACCOUNT_REPORT";
    statsReportsTypes = Arrays.asList(statsReportsToPublishStr.split(","));
    statsReportsTypes.forEach(e -> {
      if (validStatsTypes.contains(e)) {
        acceptedStatsTypes.add(e);
      }
    });
    assertEquals("Mismatch in the size of accepted stats type list", 1, acceptedStatsTypes.size());
  }
}
