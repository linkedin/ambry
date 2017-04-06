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

import java.util.Map;


/**
 * A model object to encapsulate stats in the form of a directory or tree. A example use case would be quota related
 * stats which will be two levels deep. The first level's value will be the total valid data size for all accounts and
 * the subtree field will contain a mapping of accountIds to {@link StatsSnapshot}. Each mapped {@link StatsSnapshot}'s
 * value will be the total valid data size for all containers that belong to the account that is mapped with. The mapped
 * {@link StatsSnapshot}'s subtree will contain all the containers in the account that is mapped with. At the leaf level
 * {@link StatsSnapshot}'s subtree will be null.
 */
public class StatsSnapshot {
  private long value;
  private Map<String, StatsSnapshot> subtree;

  public StatsSnapshot(Long value, Map<String, StatsSnapshot> subtree) {
    this.value = value;
    this.subtree = subtree;
  }

  public long getValue() {
    return value;
  }

  public Map<String, StatsSnapshot> getSubtree() {
    return subtree;
  }

  void setValue(long value) {
    this.value = value;
  }

  void setSubtree(Map<String, StatsSnapshot> subtree) {
    this.subtree = subtree;
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
    return subtree != null ? subtree.equals(that.subtree) : that.subtree == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (value ^ (value >>> 32));
    result = 31 * result + (subtree != null ? subtree.hashCode() : 0);
    return result;
  }
}
