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

import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * A model object to encapsulate stats in the form of a directory or tree. A example use case would be quota related
 * stats which will be two levels deep. The first level's value will be the total valid data size for all accounts and
 * the subMap field will contain a mapping of accountIds to {@link StatsSnapshot}. Each mapped {@link StatsSnapshot}'s
 * value will be the total valid data size for all containers that belong to the account that is mapped with. The mapped
 * {@link StatsSnapshot}'s subMap will contain all the containers in the account that is mapped with. At the leaf level
 * {@link StatsSnapshot}'s subMap will be null.
 */
@JsonPropertyOrder({"value", "subMap"})
public class StatsSnapshot {
  private long value;
  private Map<String, StatsSnapshot> subMap;

  /**
   * Performs recursive aggregation of two {@link StatsSnapshot} and stores the result in the first one.
   * @param baseSnapshot one of the addends and where the result will be
   * @param newSnapshot the other addend to be added into the first {@link StatsSnapshot}
   */
  public static void aggregate(StatsSnapshot baseSnapshot, StatsSnapshot newSnapshot) {
    baseSnapshot.setValue(baseSnapshot.getValue() + newSnapshot.getValue());
    if (baseSnapshot.getSubMap() == null) {
      baseSnapshot.setSubMap(newSnapshot.getSubMap());
    } else if (newSnapshot.getSubMap() != null) {
      for (Map.Entry<String, StatsSnapshot> entry : newSnapshot.getSubMap().entrySet()) {
        if (!baseSnapshot.getSubMap().containsKey(entry.getKey())) {
          baseSnapshot.getSubMap().put(entry.getKey(), new StatsSnapshot(0L, null));
        }
        aggregate(baseSnapshot.getSubMap().get(entry.getKey()), entry.getValue());
      }
    }
  }

  public StatsSnapshot(Long value, Map<String, StatsSnapshot> subMap) {
    this.value = value;
    this.subMap = subMap;
  }

  public StatsSnapshot() {
    // empty constructor for Jackson deserialization
  }

  public long getValue() {
    return value;
  }

  @JsonAnyGetter
  public Map<String, StatsSnapshot> getSubMap() {
    return subMap;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public void setSubMap(Map<String, StatsSnapshot> subMap) {
    this.subMap = subMap;
  }

  @JsonAnySetter
  public void add(String str, StatsSnapshot statsSnapshot) {
    subMap = subMap == null ? new HashMap<>() : subMap;
    subMap.put(str, statsSnapshot);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StatsSnapshot that = (StatsSnapshot) o;

    if (value != that.value) {
      return false;
    }
    return subMap != null ? subMap.equals(that.subMap) : that.subMap == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (value ^ (value >>> 32));
    result = 31 * result + (subMap != null ? subMap.hashCode() : 0);
    return result;
  }
}
