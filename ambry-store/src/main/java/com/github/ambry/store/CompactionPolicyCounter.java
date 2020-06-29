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
 * A counter used to switch {@link CompactAllPolicy}.
 */
@JsonPropertyOrder({"storeCompactionPolicySwitchPeriod", "counter"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
class CompactionPolicyCounter {
  private int storeCompactionPolicySwitchPeriod;
  private int counter;

  CompactionPolicyCounter(int storeCompactionPolicySwitchPeriod) {
    this.storeCompactionPolicySwitchPeriod = storeCompactionPolicySwitchPeriod;
  }

  //make sure objectMapper can work correctly
  CompactionPolicyCounter() {
  }

  public int getValue() {
    return counter;
  }

  public void setValue(int val) {
    this.counter = val;
  }

  public void increment() {
    counter = (counter + 1) % storeCompactionPolicySwitchPeriod;
  }
}
