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

/**
 * Health report for reporting various stats within a node.
 */
public interface AmbryStatsReport {
  /**
   * Get the name of the stats report.
   * @return the name of the stats report
   */
  String getReportName();

  /**
   * Get the type of stats report . The stats report type is defined in {@link StatsReportType}
   * @return the type of stats
   */
  StatsReportType getStatsReportType();

  /**
   * Get the cluster wide aggregate period for the stats report in minutes.
   * @return the cluster wide aggregate period in minutes
   */
  long getAggregateIntervalInMinutes();
}
