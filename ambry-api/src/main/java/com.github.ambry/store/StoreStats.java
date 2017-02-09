/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.Pair;


/**
 * Exposes important stats related to a {@link Store}.
 */
interface StoreStats {

  /**
   * Gets the size of valid data at at a particular point in time. The caller specifies a reference time and acceptable resolution
   * for the stats in the form of a {@code timeRange}. The store will return data for a point in time within the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size
   */
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange);

  /**
   * Gets the current used capacity of the {@link Store}. Total bytes that are not available for new content are
   * considered to be used.
   * @return the used data size of the {@link Store}
   */
  public long getUsedCapacity();

  /**
   * Gets the total capacity of the {@link Store}. This represents the total disk capacity occupied by the {@link Store}
   * @return the total capacity of the {@link Store}.
   */
  public long getTotalCapacity();
}
