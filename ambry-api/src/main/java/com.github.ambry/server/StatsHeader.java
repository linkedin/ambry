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


/**
 * A model object that contains metadata information about some reported stats. For example, the kind of stats that is
 * being reported, timestamp and etc.
 */
public class StatsHeader {
  public enum StatsDescription {
    QUOTA
  }

  private StatsDescription description;
  private long timestamp;
  private int storeContacted;
  private int storeResponded;
  private List<String> unreachableStores;

  StatsHeader(StatsDescription description, long timestamp, int storeContacted, int storeResponded,
      List<String> unreachableStores) {
    this.description = description;
    this.timestamp = timestamp;
    this.storeContacted = storeContacted;
    this.storeResponded = storeResponded;
    this.unreachableStores = unreachableStores;
  }

  public StatsDescription getDescription() {
    return description;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getStoreContacted() {
    return storeContacted;
  }

  public int getStoreResponded() {
    return storeResponded;
  }

  public List<String> getUnreachableStores() {
    return unreachableStores;
  }
}
