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

/**
 * The configs for stats.
 */
public class StatsManagerConfig {

  /**
   * The file path (including filename) to be used for publishing store stats to local file.
   */
  @Config("storestats.output.file.path")
  @Default("/tmp/stats_output.json")
  public final String outputFilePath;

  /**
   * The time period in seconds that configures how often are store stats published.
   */
  @Config("storestats.publish.period.in.secs")
  @Default("7200")
  public final long publishPeriodInSecs;

  /**
   * The option to enable or disable publishing store stats.
   */
  @Config("storestats.publish.enabled")
  @Default("false")
  public final boolean publishEnabled;

  public StatsManagerConfig(VerifiableProperties verifiableProperties) {
    outputFilePath = verifiableProperties.getString("storestats.output.file.path", "/tmp/stats_output.json");
    publishPeriodInSecs = verifiableProperties.getLong("storestats.publish.period.in.secs", 7200);
    publishEnabled = verifiableProperties.getBoolean("storestats.publish.enabled", false);
  }
}
