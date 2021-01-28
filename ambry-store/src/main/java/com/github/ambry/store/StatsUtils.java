/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.store;

import java.util.HashMap;
import java.util.Map;


/**
 * Helper methods for stats collection.
 */
class StatsUtils {
  /**
   * Helper function to update nested map data structure.
   * @param nestedMap nested {@link Map} to be updated
   * @param firstKey of the nested map
   * @param secondKey of the nested map
   * @param value the value to be added at the corresponding entry
   */
  public static <T1, T2> void updateNestedMapHelper(Map<T1, Map<T2, Long>> nestedMap, T1 firstKey, T2 secondKey,
      Long value) {
    if (!nestedMap.containsKey(firstKey)) {
      nestedMap.put(firstKey, new HashMap<>());
    }
    updateMapHelper(nestedMap.get(firstKey), secondKey, value);
  }

  /**
   * Helper function to update map data structure.
   * @param map {@link Map} to be updated
   * @param key of the map
   * @param value the value to be added at the corresponding entry
   */
  public static <T> void updateMapHelper(Map<T, Long> map, T key, Long value) {
    Long newValue = map.containsKey(key) ? map.get(key) + value : value;
    map.put(key, newValue);
  }
}
