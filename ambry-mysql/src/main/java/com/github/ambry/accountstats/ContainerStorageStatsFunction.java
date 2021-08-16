/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.accountstats;

import com.github.ambry.server.storagestats.ContainerStorageStats;


/**
 * A callback function to call when processing container storage stats.
 */
@FunctionalInterface
public interface ContainerStorageStatsFunction {

  /**
   * Process container storage stats.
   * @param partitionId The partition id.
   * @param accountId The account id.
   * @param containerStats The {@link ContainerStorageStats}
   * @param updatedAtMs The timestamp in milliseconds when this data is updated.
   */
  void apply(int partitionId, short accountId, ContainerStorageStats containerStats, long updatedAtMs);
}
