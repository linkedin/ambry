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

import java.util.Map;


/**
 * Health report for reporting various stats within a node.
 */
public interface AmbryHealthReport {
  /**
   * Get the field name (accessor) to fetch stats in the health report.
   * @return the key in the health report that holds the stats
   */
  String getStatsFieldName();

  /**
   * Get the name of the health report. One Health Report can have many fields and {@link #getStatsFieldName()} is
   * one such field. The report's structure is {@link Map<String,String>}.
   * @return the name of the health report
   */
  String getReportName();

  /**
   * Get the type of stats report specified in this health report. The stats report type is defined in {@link StatsReportType}
   * @return the type of stats represented by this health report
   */
  StatsReportType getStatsReportType();

  /**
   * Get the most recent health report in the form of a {@link Map}.
   * @return a {@link Map} of String to String containing the content of the health report
   */
  public Map<String, String> getRecentHealthReport();

  /**
   * Get the cluster wide aggregate period for the health report in minutes.
   * @return the cluster wide aggregate period in minutes
   */
  long getAggregateIntervalInMinutes();
}
