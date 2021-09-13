/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.github.ambry.server.storagestats.HostAccountStorageStats;


/**
 * A wrapper model object that contains a {@link HostAccountStorageStats} and a {@link StatsHeader} with metadata.
 */
@JsonPropertyOrder({"header", "stats"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class HostAccountStorageStatsWrapper {
  private StatsHeader header;
  private HostAccountStorageStats stats;

  public HostAccountStorageStatsWrapper(StatsHeader header, HostAccountStorageStats stats) {
    this.header = header;
    this.stats = stats;
  }

  /**
   * Empty constructor for Jackson
   */
  public HostAccountStorageStatsWrapper() {
  }

  /**
   * Return {@link StatsHeader}.
   * @return
   */
  public StatsHeader getHeader() {
    return header;
  }

  /**
   * Return {@link HostAccountStorageStats}.
   * @return
   */
  public HostAccountStorageStats getStats() {
    return stats;
  }
}
