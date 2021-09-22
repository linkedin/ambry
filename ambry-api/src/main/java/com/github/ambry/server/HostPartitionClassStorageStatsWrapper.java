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

import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;


/**
 * A wrapper model object that contains a {@link HostPartitionClassStorageStats} and a {@link StatsHeader} with metadata.
 */
public class HostPartitionClassStorageStatsWrapper {
  private final StatsHeader header;
  private final HostPartitionClassStorageStats stats;

  public HostPartitionClassStorageStatsWrapper(StatsHeader header, HostPartitionClassStorageStats stats) {
    this.header = header;
    this.stats = stats;
  }

  /**
   * Return {@link StatsHeader}.
   * @return
   */
  public StatsHeader getHeader() {
    return header;
  }

  /**
   * Return {@link HostPartitionClassStorageStats}.
   * @return
   */
  public HostPartitionClassStorageStats getStats() {
    return stats;
  }
}
