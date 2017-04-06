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

  /**
   * Walk through two {@link StatsSnapshot} and check if they are equal. Two {@link StatsSnapshot} are considered equal
   * if they have the same value and subtree mapping at each level.
   * @param a the {@link StatsSnapshot} to be compared with b
   * @param b the {@link StatsSnapshot} to be compared with a
   * @return
   */
  public static boolean isEqual(StatsSnapshot a, StatsSnapshot b) {
    boolean retVal = true;
    if (a == null || b == null || a.getValue() != b.getValue()) {
      retVal = false;
    } else {
      boolean aSubtreeIsNull = a.getSubtree() == null;
      boolean bSubtreeIsNull = b.getSubtree() == null;
      // either both subtree have to be null or neither, otherwise it's definitely not equal
      if (aSubtreeIsNull != bSubtreeIsNull) {
        retVal = false;
      } else if (!aSubtreeIsNull) {
        for (Map.Entry<String, StatsSnapshot> entry : a.getSubtree().entrySet()) {
          if (b.getSubtree().containsKey(entry.getKey())) {
            retVal = isEqual(entry.getValue(), b.getSubtree().get(entry.getKey()));
            if (!retVal) {
              break;
            }
          } else {
            retVal = false;
            break;
          }
        }
      }
    }
    return retVal;
  }

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

  public void setValue(long value) {
    this.value = value;
  }

  public void setSubtree(Map<String, StatsSnapshot> subtree) {
    this.subtree = subtree;
  }
}
