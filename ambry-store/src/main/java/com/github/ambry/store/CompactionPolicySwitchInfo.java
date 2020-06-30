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

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * The {@link CompactionPolicy} info to determine when to use which {@link CompactionPolicy}.
 */
@JsonPropertyOrder({"compactionPolicyCounter", "lastCompactionTime"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class CompactionPolicySwitchInfo {
  private CompactionPolicyCounter compactionPolicyCounter;
  private long lastCompactAllTime;

  /**
   * Constructor to create {@link CompactionPolicySwitchInfo} object.
   * @param compactionPolicyCounter Counter used to switch {@link CompactAllPolicy}
   * @param lastCompactAllTime last time when {@link CompactAllPolicy} has been selected.
   */
  CompactionPolicySwitchInfo(CompactionPolicyCounter compactionPolicyCounter, long lastCompactAllTime) {
    this.compactionPolicyCounter = compactionPolicyCounter;
    this.lastCompactAllTime = lastCompactAllTime;
  }

  /**
   * make sure objectMapper can work correctly
   */
  CompactionPolicySwitchInfo() {
  }

  /**
   * Get current {@link CompactionPolicyCounter}
   * @return {@link CompactionPolicyCounter}
   */
  CompactionPolicyCounter getCompactionPolicyCounter() {
    return this.compactionPolicyCounter;
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
}
