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
public class AmbryStatsReport extends HealthReportProvider implements AmbryHealthReport {
  public static final String REPORT_NAME_SUFFIX = "Report";
  private static final String STATS_FIELD_SUFFIX = "Stats";
  private final String reportName;
  private final String statsFieldName;
  private final StatsManager statsManager;
  private final long aggregatePeriodInMinutes;
  private final StatsReportType statsReportType;

  AmbryStatsReport(StatsManager statsManager, long aggregatePeriodInMinutes, StatsReportType type) {
    this.statsManager = statsManager;
    this.aggregatePeriodInMinutes = aggregatePeriodInMinutes;
    String reportTypeStr = convertStatsReportTypeToProperString(type);
    reportName = reportTypeStr + REPORT_NAME_SUFFIX;
    statsFieldName = reportTypeStr + STATS_FIELD_SUFFIX;
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
   * Convert input stats report type to proper format (i.e PARTITION_CLASS_REPORT -> PartitionClass). That is:
   * 1. remove underscores.
   * 2. capitalize first letter and keep remaining letters lowercase.
   * @param type the stats report type.
   * @return converted string.
   */
  public static String convertStatsReportTypeToProperString(StatsReportType type) {
    StringBuilder sb = new StringBuilder();
    String[] reportTypeStrs = type.toString().split("_");
    for (int i = 0; i < reportTypeStrs.length - 1; ++i) {
      sb.append(toProperCase(reportTypeStrs[i]));
    }
    return sb.toString();
  }

  /**
   * Convert input string to proper format. That is, capitalize first letter and keep remaining letters lowercase.
   * @param s the input string to convert.
   * @return capitalized string.
   */
  private static String toProperCase(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
  }
}
