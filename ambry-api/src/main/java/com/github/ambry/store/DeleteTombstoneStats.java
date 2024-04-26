/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A wrapper class to carry delete tombstone stats between different components
 */
public class DeleteTombstoneStats {
  // The number of delete tombstones who have ttl (doesn't have to be expired)
  public final long expiredCount;
  // The size of delete tombstones who have ttl (doesn't have to be expired)
  public final long expiredSize;
  // The number of delete tombstones who don't have ttl
  public final long permanentCount;
  // The size of delete tombstones who don't have ttl
  public final long permanentSize;
  // The size of delete tombstones who don't have ttl and was deleted within one day
  public final long permanentSizeOneDay;
  // The size of delete tombstones who don't have ttl and was deleted within two days
  public final long permanentSizeTwoDays;
  // The size of delete tombstones who don't have ttl and was deleted within three days
  public final long permanentSizeThreeDays;
  // The size of delete tombstones who don't have ttl and was deleted within five days
  public final long permanentSizeFiveDays;
  // The size of delete tombstones who don't have ttl and was deleted within seven days
  public final long permanentSizeSevenDays;
  // The size of delete tombstones who don't have ttl and was deleted within ten days
  public final long permanentSizeTenDays;
  // The size of delete tombstones who don't have ttl and was deleted within fourteen days
  public final long permanentSizeFourteenDays;

  /**
   * A static instance representing all zero stats.
   */
  public static final DeleteTombstoneStats BASE = new DeleteTombstoneStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

  /**
   * Private constructor to create a {@link DeleteTombstoneStats}.
   * @param expiredCount
   * @param expiredSize
   * @param permanentCount
   * @param permanentSize
   * @param permanentSizeOneDay
   * @param permanentSizeTwoDays
   * @param permanentSizeThreeDays
   * @param permanentSizeFiveDays
   * @param permanentSizeSevenDays
   * @param permanentSizeTenDays
   * @param permanentSizeFourteenDays
   */
  private DeleteTombstoneStats(long expiredCount, long expiredSize, long permanentCount, long permanentSize,
      long permanentSizeOneDay, long permanentSizeTwoDays, long permanentSizeThreeDays, long permanentSizeFiveDays,
      long permanentSizeSevenDays, long permanentSizeTenDays, long permanentSizeFourteenDays) {
    this.expiredCount = expiredCount;
    this.expiredSize = expiredSize;
    this.permanentCount = permanentCount;
    this.permanentSize = permanentSize;
    this.permanentSizeOneDay = permanentSizeOneDay;
    this.permanentSizeTwoDays = permanentSizeTwoDays;
    this.permanentSizeThreeDays = permanentSizeThreeDays;
    this.permanentSizeFiveDays = permanentSizeFiveDays;
    this.permanentSizeSevenDays = permanentSizeSevenDays;
    this.permanentSizeTenDays = permanentSizeTenDays;
    this.permanentSizeFourteenDays = permanentSizeFourteenDays;
  }

  /**
   * Merge the given {@link DeleteTombstoneStats} to create a new {@link DeleteTombstoneStats}.
   * @param other The {@link DeleteTombstoneStats} to merge
   * @return A new {@link DeleteTombstoneStats} whose stats are the sum of this stats and the given stats.
   */
  public DeleteTombstoneStats merge(DeleteTombstoneStats other) {
    //@formatter:off
    return new DeleteTombstoneStats(
        this.expiredCount + other.expiredCount,
        this.expiredSize + other.expiredSize,
        this.permanentCount + other.permanentCount,
        this.permanentSize + other.permanentSize,
        this.permanentSizeOneDay + other.permanentSizeOneDay,
        this.permanentSizeTwoDays + other.permanentSizeTwoDays,
        this.permanentSizeThreeDays + other.permanentSizeThreeDays,
        this.permanentSizeFiveDays + other.permanentSizeFiveDays,
        this.permanentSizeSevenDays + other.permanentSizeSevenDays,
        this.permanentSizeTenDays + other.permanentSizeTenDays,
        this.permanentSizeFourteenDays + other.permanentSizeFourteenDays
    );
    //@formatter:on
  }

  /**
   * A builder to build {@link DeleteTombstoneStats}.
   */
  public static class Builder {
    private long expiredCount = 0;
    private long expiredSize = 0;
    private long permanentCount = 0;
    private long permanentSize = 0;
    //@formatter:off
    private final List<Pair<Long, AtomicLong>> deltas = Arrays.asList(
        Pair.of(TimeUnit.DAYS.toMillis(1), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(2), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(3), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(5), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(7), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(10), new AtomicLong(0)),
        Pair.of(TimeUnit.DAYS.toMillis(14), new AtomicLong(0))
    );
    //@formatter:on
    private final Time time;

    /**
     * Constructor to create a builder.
     */
    public Builder() {
      this(SystemTime.getInstance());
    }

    /**
     * Constructor to create a builder.
     * @param time The {@link Time} instance.
     */
    public Builder(Time time) {
      this.time = time;
    }

    /**
     * Generate some random value to initialize delete tombstone count and size. This is only used for testing.
     * @param random The {@link Random} generator.
     * @param countLimit The upper bound of delete tombstone count.
     * @param sizeLimit The upper bound of delete tombstone size.
     * @return This builder.
     */
    public Builder random(Random random, int countLimit, int sizeLimit) {
      this.expiredCount = random.nextInt(countLimit);
      this.permanentCount = random.nextInt(countLimit);
      this.expiredSize = random.nextInt(sizeLimit);
      this.permanentSize = random.nextInt(sizeLimit);
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#expiredCount} by one.
     * @return This builder
     */
    public Builder expiredCountInc() {
      this.expiredCount++;
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#expiredCount} by given count.
     * @return This builder
     */
    public Builder expiredCountInc(long count) {
      this.expiredCount += count;
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#permanentCount} by one.
     * @return This builder.
     */
    public Builder permanentCountInc() {
      this.permanentCount++;
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#permanentCount} by given count.
     * @return This builder.
     */
    public Builder permanentCountInc(long count) {
      this.permanentCount += count;
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#expiredSize} by the given size.
     * @param size The size to increment
     * @return
     */
    public Builder expiredSizeInc(long size) {
      this.expiredSize += size;
      return this;
    }

    /**
     * Increment {@link DeleteTombstoneStats#permanentSize} by the given size, as well as the other permanent size based on time.
     * @param size The size to increment
     * @param operationTime The operation time of the delete tombstone
     * @return
     */
    public Builder permanentSizeInc(long size, long operationTime) {
      this.permanentSize += size;
      if (operationTime != Utils.Infinite_Time) {
        long d = time.milliseconds() - operationTime;
        for (Pair<Long, AtomicLong> p : deltas) {
          if (p.getFirst() >= d) {
            p.getSecond().addAndGet(size);
            break;
          }
        }
      }
      return this;
    }

    /**
     * Build a {@link DeleteTombstoneStats}.
     * @return
     */
    public DeleteTombstoneStats build() {
      //@formatter:off
      return new DeleteTombstoneStats(
          this.expiredCount,
          this.expiredSize,
          this.permanentCount,
          this.permanentSize,
          this.deltas.get(0).getSecond().get(),
          this.deltas.get(1).getSecond().get(),
          this.deltas.get(2).getSecond().get(),
          this.deltas.get(3).getSecond().get(),
          this.deltas.get(4).getSecond().get(),
          this.deltas.get(5).getSecond().get(),
          this.deltas.get(6).getSecond().get()
      );
      //@formatter:on
    }
  }
}
