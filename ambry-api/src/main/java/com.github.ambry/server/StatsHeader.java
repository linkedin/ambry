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

package com.github.ambry.server;

import java.util.List;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * A model object that contains metadata information about some reported stats. For example, the kind of stats that is
 * being reported, timestamp and etc.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonPropertyOrder({"description", "timestamp", "storesContactedCount", "storesRespondedCount", "unreachableStores"})
public class StatsHeader {
  public enum StatsDescription {
    STORED_DATA_SIZE
  }

  private StatsDescription description;
  private long timestamp;
  private int storesContactedCount;
  private int storesRespondedCount;
  private List<String> unreachableStores;

  public StatsHeader(StatsDescription description, long timestamp, int storesContactedCount, int storesRespondedCount,
      List<String> unreachableStores) {
    this.description = description;
    this.timestamp = timestamp;
    this.storesContactedCount = storesContactedCount;
    this.storesRespondedCount = storesRespondedCount;
    this.unreachableStores = unreachableStores;
  }

  public StatsHeader() {
    // empty constructor for Jackson deserialization
  }

  /**
   * @return the reference timestamp at which the stats are collected
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the list of unreachable stores generated in the stats report
   */
  public List<String> getUnreachableStores() {
    return unreachableStores;
  }
}
