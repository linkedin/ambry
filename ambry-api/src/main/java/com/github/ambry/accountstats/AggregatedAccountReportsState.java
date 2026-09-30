/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import java.util.Objects;


/**
 * Persisted state for the account report aggregation task and its monthly snapshot.
 */
public class AggregatedAccountReportsState {
  private final String month;
  private final Long lastAggregationTimeMs;
  private final String monthlyBaselineRecoveryMonth;
  private final long snapshotVersion;

  /**
   * @param month the month represented by the monthly snapshot.
   * @param lastAggregationTimeMs completion time of the last successful aggregation, or {@code null} if uninitialized.
   * @param monthlyBaselineRecoveryMonth month waiting for a post-gap recovery snapshot, or {@code null}.
   * @param snapshotVersion version incremented whenever the monthly snapshot changes.
   */
  public AggregatedAccountReportsState(String month, Long lastAggregationTimeMs, String monthlyBaselineRecoveryMonth,
      long snapshotVersion) {
    this.month = month;
    this.lastAggregationTimeMs = lastAggregationTimeMs;
    this.monthlyBaselineRecoveryMonth = monthlyBaselineRecoveryMonth;
    this.snapshotVersion = snapshotVersion;
  }

  public String getMonth() {
    return month;
  }

  public Long getLastAggregationTimeMs() {
    return lastAggregationTimeMs;
  }

  public String getMonthlyBaselineRecoveryMonth() {
    return monthlyBaselineRecoveryMonth;
  }

  public long getSnapshotVersion() {
    return snapshotVersion;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AggregatedAccountReportsState)) {
      return false;
    }
    AggregatedAccountReportsState that = (AggregatedAccountReportsState) other;
    return snapshotVersion == that.snapshotVersion && Objects.equals(month, that.month)
        && Objects.equals(lastAggregationTimeMs, that.lastAggregationTimeMs)
        && Objects.equals(monthlyBaselineRecoveryMonth, that.monthlyBaselineRecoveryMonth);
  }

  @Override
  public int hashCode() {
    return Objects.hash(month, lastAggregationTimeMs, monthlyBaselineRecoveryMonth, snapshotVersion);
  }
}
