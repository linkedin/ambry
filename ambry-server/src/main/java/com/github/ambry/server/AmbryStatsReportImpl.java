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

/**
 * Customized Helix Stats Report for collecting account usage stats per node (instance).
 */
public class AmbryStatsReportImpl implements AmbryStatsReport {
  public static final String REPORT_NAME_SUFFIX = "Report";
  private final String reportName;
  private final long aggregatePeriodInMinutes;
  private final StatsReportType statsReportType;

  AmbryStatsReportImpl(long aggregatePeriodInMinutes, StatsReportType type) {
    this.aggregatePeriodInMinutes = aggregatePeriodInMinutes;
    String reportTypeStr = convertStatsReportTypeToProperString(type);
    reportName = reportTypeStr + REPORT_NAME_SUFFIX;
    statsReportType = type;
  }

  @Override
  public String getReportName() {
    return reportName;
  }

  @Override
  public StatsReportType getStatsReportType() {
    return statsReportType;
  }

  @Override
  public long getAggregateIntervalInMinutes() {
    return aggregatePeriodInMinutes;
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
