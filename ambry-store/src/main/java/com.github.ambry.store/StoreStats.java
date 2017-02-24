package com.github.ambry.store;

import com.github.ambry.utils.Pair;
import java.util.HashMap;
import java.util.SortedMap;


/**
 * Exposes important stats related to a {@link Store}.
 */
public interface StoreStats {

  /**
   * Gets the size of valid data at at a particular point in time. The caller specifies a reference time and acceptable resolution
   * for the stats in the form of a {@code timeRange}. The store will return data for a point in time within the specified range.
      * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size
   */
  Pair<Long, Long> getValidDataSize(TimeRange timeRange);

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size for each segment in the form of a {@link SortedMap} of segment names to valid data sizes.
   */
  Pair<Long, SortedMap<String, Long>> getValidDataSizeBySegment(TimeRange timeRange);

  /**
   * Gets the size of valid data for all containers in the store as of now (the time when the API is called).
   * @return the valid data size for each container in the form of a nested {@link HashMap} of serviceIds to another map of
   * containerIds to valid data sizes.
   */
  HashMap<String, HashMap<String, Long>> getValidDataSize();

  /**
   * Gets the current used capacity of the {@link Store}.
   * @return the current used capacity of the {@link Store}.
   */
  long getUsedCapacity();

  /**
   * Gets the used capacity of each log segment.
   * @return the used capacity of each log segment.
   */
  SortedMap<String, Long> getUsedCapacityBySegment();

  /**
   * Gets the total capacity of the {@link Store}.
   * @return the total capacity of the {@link Store}.
   */
  long getTotalCapacity();
}