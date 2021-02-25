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

package com.github.ambry.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * The configs for stats.
 */
public class StatsManagerConfig {
  public static final String STATS_OUTPUT_FILE_PATH = "stats.output.file.path";
  public static final String STATS_PUBLISH_PERIOD_IN_SECS = "stats.publish.period.in.secs";
  public static final String STATS_INITIAL_DELAY_UPPER_BOUND_IN_SECS = "stats.initial.delay.upper.bound.in.secs";
  public static final String STATS_ENABLE_MYSQL_REPORT = "stats.enable.mysql.report";
  public static final String STATS_HEALTH_REPORT_EXCLUDE_ACCOUNT_NAMES = "stats.health.report.exclude.account.names";
  public static final String STATS_PUBLISH_EXCLUDE_ACCOUNT_NAMES = "stats.publish.exclude.account.names";
  public static final String STATS_PUBLISH_PARTITION_CLASS_REPORT_PERIOD_IN_SECS =
      "stats.publish.partition.class.report.period.in.secs";

  /**
   * The file path (including filename) to be used for publishing the stats.
   */
  @Config(STATS_OUTPUT_FILE_PATH)
  @Default("/tmp/stats_output.json")
  public final String outputFilePath;

  /**
   * The time period in seconds that configures how often stats are published.
   */
  @Config(STATS_PUBLISH_PERIOD_IN_SECS)
  @Default("7200")
  public final long publishPeriodInSecs;

  /**
   * The upper bound for the initial delay in seconds before the first stats collection is triggered. The delay is a
   * random number b/w 0 (inclusive) and this number (exclusive). If no initial delay is desired, this can be set to 0.
   */
  @Config(STATS_INITIAL_DELAY_UPPER_BOUND_IN_SECS)
  @Default("600")
  public final int initialDelayUpperBoundInSecs;

  /**
   * True to enable publishing stats to mysql database
   */
  @Config(STATS_ENABLE_MYSQL_REPORT)
  @Default("false")
  public final boolean enableMysqlReport;

  /**
   * The account names to exclude from the health report. Multiple account names should be separated with ",".
   */
  @Config(STATS_HEALTH_REPORT_EXCLUDE_ACCOUNT_NAMES)
  @Default("")
  public final List<String> healthReportExcludeAccountNames;

  /**
   * The account names to exclude from publishing to local disk and mysql database. Multiple account names should be
   * separated with ",".
   */
  @Config(STATS_PUBLISH_EXCLUDE_ACCOUNT_NAMES)
  @Default("")
  public final List<String> publishExcludeAccountNames;

  /**
   * The time period in seconds that configures how often partition class stats are published to mysql. Set it to 0 to
   * disable publishing partition class reports to mysql database.
   */
  @Config(STATS_PUBLISH_PARTITION_CLASS_REPORT_PERIOD_IN_SECS)
  @Default("0")
  public final long publishPartitionClassReportPeriodInSecs;

  public StatsManagerConfig(VerifiableProperties verifiableProperties) {
    outputFilePath = verifiableProperties.getString(STATS_OUTPUT_FILE_PATH, "/tmp/stats_output.json");
    publishPeriodInSecs = verifiableProperties.getLongInRange(STATS_PUBLISH_PERIOD_IN_SECS, 7200, 0, Long.MAX_VALUE);
    initialDelayUpperBoundInSecs =
        verifiableProperties.getIntInRange(STATS_INITIAL_DELAY_UPPER_BOUND_IN_SECS, 600, 0, Integer.MAX_VALUE);
    enableMysqlReport = verifiableProperties.getBoolean(STATS_ENABLE_MYSQL_REPORT, false);
    String excludeNames = verifiableProperties.getString(STATS_HEALTH_REPORT_EXCLUDE_ACCOUNT_NAMES, "").trim();
    healthReportExcludeAccountNames =
        excludeNames.isEmpty() ? Collections.EMPTY_LIST : Arrays.asList(excludeNames.split(","));
    excludeNames = verifiableProperties.getString(STATS_PUBLISH_EXCLUDE_ACCOUNT_NAMES, "").trim();
    publishExcludeAccountNames =
        excludeNames.isEmpty() ? Collections.EMPTY_LIST : Arrays.asList(excludeNames.split(","));
    publishPartitionClassReportPeriodInSecs =
        verifiableProperties.getLongInRange(STATS_PUBLISH_PARTITION_CLASS_REPORT_PERIOD_IN_SECS, 0, 0, Long.MAX_VALUE);
    if (publishPartitionClassReportPeriodInSecs != 0 && !enableMysqlReport) {
      throw new IllegalStateException(
          "Bad configuration, you have to enableMysqlReport if you set a non-zero value for publishPartitionClassReportPeriodInSecs");
    }
  }
}
