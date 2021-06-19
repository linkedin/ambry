/**
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
package com.github.ambry.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * The {@link CompactionPolicy} info to determine when to use which {@link CompactionPolicy}.
 */
@JsonPropertyOrder({"lastCompactionTime", "nextRoundIsCompactAllPolicy"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class CompactionPolicySwitchInfo {
  private long lastCompactAllTime;
  private boolean nextRoundIsCompactAllPolicy;

  /**
   * Constructor to create {@link CompactionPolicySwitchInfo} object.
   * @param lastCompactAllTime last time when {@link CompactAllPolicy} has been selected.
   * @param nextRoundIsCompactAllPolicy {@code True} if the next round of compaction policy is compactAll. {@code False} Otherwise.
   */
  CompactionPolicySwitchInfo(long lastCompactAllTime, boolean nextRoundIsCompactAllPolicy) {
    this.lastCompactAllTime = lastCompactAllTime;
    this.nextRoundIsCompactAllPolicy = nextRoundIsCompactAllPolicy;
  }

  /**
   * make sure objectMapper can work correctly
   */
  CompactionPolicySwitchInfo() {
  }

  /**
   * Get the last time when {@link CompactAllPolicy} has been selected.
   * @return the last time when {@link CompactAllPolicy} has been selected.
   */
  long getLastCompactAllTime() {
    return this.lastCompactAllTime;
  }

  /**
   * Set the last time when {@link CompactAllPolicy} has been selected.
   * @param lastCompactAllTime last time when {@link CompactAllPolicy} has been selected.
   */
  void setLastCompactAllTime(long lastCompactAllTime) {
    this.lastCompactAllTime = lastCompactAllTime;
  }

  /**
   * @return {@code True} if the next round of compaction policy is compactAll. {@code False} Otherwise.
   */
  boolean isNextRoundCompactAllPolicy() {
    return this.nextRoundIsCompactAllPolicy;
  }

  /**
   * Set the nextRoundIsCompactAllPolicy to {@code True} if the next round of compaction policy is compactAll.
   */
  void setNextRoundIsCompactAllPolicy(boolean isNextRoundCompactAllPolicy) {
    this.nextRoundIsCompactAllPolicy = isNextRoundCompactAllPolicy;
  }
}
