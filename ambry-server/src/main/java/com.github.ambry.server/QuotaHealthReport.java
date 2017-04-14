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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.healthcheck.HealthReportProvider;


/**
 * Customized Helix Health Report for collecting quota stats per node (instance).
 */
class QuotaHealthReport extends HealthReportProvider implements HealthReport {
  private static final String REPORT_NAME = "QuotaReport";
  private static final String FIELD_NAME = "QuotaStats";
  private final StatsManager statsManager;
  private final long aggregatePeriodInMinutes;

  QuotaHealthReport(StatsManager statsManager, long aggregatePeriodInMinutes) {
    this.statsManager = statsManager;
    this.aggregatePeriodInMinutes = aggregatePeriodInMinutes;
  }

  /**
   * Get the node wide aggregated quota stats in this node
   * @return a {@link Map} with the aggregated quota stats mapped with {@link QuotaHealthReport}'s static key
   */
  @Override
  public Map<String, String> getRecentHealthReport() {
    Map<String, String> report = new HashMap<>();
    report.put(FIELD_NAME, statsManager.getNodeStatsInJSON());
    return report;
  }

  @Override
  public String getReportName() {
    return REPORT_NAME;
  }

  @Override
  public String getFieldName() {
    return FIELD_NAME;
  }

  @Override
  public long getAggregatePeriodInMinutes() {
    return aggregatePeriodInMinutes;
  }

  @Override
  public void resetStats() {
    // no op
  }
}
