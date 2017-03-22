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

package com.github.ambry.store;

import com.github.ambry.utils.Pair;


/**
 * Exposes important stats related to a {@link Store}.
 */
public interface StoreStats {
  /**
   * Gets the size of valid data at a particular point in time. The caller specifies a reference time and acceptable
   * resolution for the stats in the form of a {@link TimeRange}. The store will return valid data size for a point
   * in time within the specified range.
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_exp_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_del_ref < t_delete
   * 4. DELETE record
   * For this API, t_del_ref is based on the given {@link TimeRange} and t_exp_ref is the time when the API is called.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size
   */
  Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException;

  /**
   * Fetches stats for the corresponding {@link Store} as a {@link StatsSnapshot}.
   * @return a {@link StatsSnapshot} with relevant stats
   * @throws StoreException
   */
  StatsSnapshot getStatsSnapshot() throws StoreException;
}
