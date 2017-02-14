package com.github.ambry.store;

import com.github.ambry.utils.Pair;


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
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange);

  /**
   * Gets the current used capacity of the {@link Store}.
   * @return the current used capacity of the {@link Store}.
   */
  public long getUsedCapacity();

  /**
   * Gets the total capacity of the {@link Store}.
   * @return the total capacity of the {@link Store}.
   */
  public long getTotalCapacity();
}