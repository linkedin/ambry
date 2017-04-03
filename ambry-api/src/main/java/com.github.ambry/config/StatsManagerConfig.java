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
   * The file path (including filename) to be used for publishing the stats.
   */
  @Config("stats.output.file.path")
  @Default("/tmp/stats_output.json")
  public final String outputFilePath;

  /**
   * The time period in seconds that configures how often stats are published.
   */
  @Config("stats.publish.period.in.secs")
  @Default("7200")
  public final long publishPeriodInSecs;

  /**
   * The upper bound for the initial delay in seconds before the first stats collection is triggered. The delay is a
   * random number b/w 0 (inclusive) and this number (exclusive). If no initial delay is desired, this can be set to 0.
   */
  @Config("stats.initial.delay.upper.bound.in.secs")
  @Default("600")
  public final int initialDelayUpperBoundInSecs;

  public StatsManagerConfig(VerifiableProperties verifiableProperties) {
    outputFilePath = verifiableProperties.getString("stats.output.file.path", "/tmp/stats_output.json");
    publishPeriodInSecs = verifiableProperties.getLongInRange("stats.publish.period.in.secs", 7200, 0, Long.MAX_VALUE);
    initialDelayUpperBoundInSecs =
        verifiableProperties.getIntInRange("stats.initial.delay.upper.bound.in.secs", 600, 0, Integer.MAX_VALUE);
  }
}
