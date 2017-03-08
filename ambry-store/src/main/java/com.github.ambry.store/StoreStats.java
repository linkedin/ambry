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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;


/**
 * Exposes important stats related to a {@link Store}.
 */
public interface StoreStats {
  /**
   * Gets the size of valid data at a particular point in time. The caller specifies a reference time and acceptable resolution
   * for the stats in the form of a {@link TimeRange}. The store will return data for a point in time within the specified range.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time and the acceptable resolution
   * @return a {@link Pair} whose first element is the time at which stats was collected and whose second element is the valid data size
   */
  Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException;

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected and whose second element is the valid data
   * size for each segment in the form of a {@link Map} of segment names to valid data sizes.
   */
  Pair<Long, Map<String, Long>> getValidSizeByLogSegment(TimeRange timeRange) throws StoreException;

  /**
   * Gets the size of valid data for all serviceIds and their respective containerIds in the store as of now (the time when the API is called).
   * @return the valid data size for each container in the form of a nested {@link Map} of serviceIds to another map of
   * containerIds to valid data size.
   */
  Map<String, Map<String, Long>> getValidDataSizeByContainers() throws StoreException;

}
