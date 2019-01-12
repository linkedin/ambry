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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.healthcheck.HealthReportProvider;


/**
 * Customized Helix Health Report for collecting account usage stats per node (instance).
 */
class AmbryStatsReport extends HealthReportProvider implements AmbryHealthReport {
  private static final String REPORT_NAME_SUFFIX = "Report";
  private static final String STATS_FIELD_SUFFIX = "Stats";
  private final String reportName;
  private final String statsFieldName;
  private final StatsManager statsManager;
  private final long aggregatePeriodInMinutes;
  private final StatsReportType statsReportType;

  AmbryStatsReport(StatsManager statsManager, long aggregatePeriodInMinutes, StatsReportType type) {
    this.statsManager = statsManager;
    this.aggregatePeriodInMinutes = aggregatePeriodInMinutes;
    StringBuilder sb = new StringBuilder();
    String[] reportTypeStrs = type.toString().split("_");
    for (int i = 0; i < reportTypeStrs.length - 1; ++i) {
      sb.append(toProperCase(reportTypeStrs[i]));
    }
    reportName = sb.toString() + REPORT_NAME_SUFFIX;
    statsFieldName = sb.toString() + STATS_FIELD_SUFFIX;
    statsReportType = type;
  }

  /**
   * Get the node wide aggregated quota stats in this node
   * @return a {@link Map} with the aggregated quota stats mapped with {@link AmbryStatsReport}'s static key
   */
  @Override
  public Map<String, String> getRecentHealthReport() {
    Map<String, String> report = new HashMap<>();
    report.put(statsFieldName, statsManager.getNodeStatsInJSON(statsReportType));
    return report;
  }

  @Override
  public String getReportName() {
    return reportName;
  }

  @Override
  public String getStatsFieldName() {
    return statsFieldName;
  }

  @Override
  public StatsReportType getStatsReportType() {
    return statsReportType;
  }

  @Override
  public long getAggregateIntervalInMinutes() {
    return aggregatePeriodInMinutes;
  }

  @Override
  public void resetStats() {
    // no op
  }

  /**
   * Convert input string to proper format. That is, capitalize first letter and keep remaining letters lowercase.
   * @param s the input string to convert.
   * @return capitalized string.
   */
  private String toProperCase(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
  }
}
