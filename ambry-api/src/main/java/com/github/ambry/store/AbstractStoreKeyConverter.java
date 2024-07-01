/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractStoreKeyConverter implements StoreKeyConverter {
  private final Map<StoreKey, StoreKey> conversionCache = new HashMap<>();
  private final Map<StoreKey, Integer> cachedKeyCount = new HashMap<>();

  @Override
  public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> input) throws Exception {
    List<StoreKey> alreadyPresentStoreKeys = new ArrayList<>();
    List<StoreKey> storeKeysTobeConverted = new ArrayList<>();

    input.forEach((storeKey) -> {
      if (conversionCache.containsKey(storeKey)) {
        alreadyPresentStoreKeys.add(storeKey);
      } else {
        storeKeysTobeConverted.add(storeKey);
      }
    });

    Map<StoreKey, StoreKey> map = convertKeys(storeKeysTobeConverted);

    alreadyPresentStoreKeys.forEach((storeKey -> {
      map.put(storeKey, conversionCache.get(storeKey));
    }));

    map.forEach((storeKey, convertedStoreKey) -> {
      cachedKeyCount.put(storeKey, cachedKeyCount.getOrDefault(storeKey, 0) + 1);
    });

    conversionCache.putAll(map);
    return Collections.unmodifiableMap(map);
  }

  @Override
  public StoreKey getConverted(StoreKey storeKey) {
    return getConvertedKey(storeKey, conversionCache.containsKey(storeKey), conversionCache.get(storeKey));
  }


  @Override
  public void remove(Collection<? extends StoreKey> storeKeys) {
    storeKeys.forEach(storeKey -> {
      if (!cachedKeyCount.containsKey(storeKey)) {
        return;
      }
      cachedKeyCount.put(storeKey, cachedKeyCount.get(storeKey) - 1);
      if (cachedKeyCount.get(storeKey) != 0) {
        return;
      }
      conversionCache.remove(storeKey);
      cachedKeyCount.remove(storeKey);
    });
  }

  @Override
  public void dropCache() {
    cachedKeyCount.clear();
    conversionCache.clear();
  }

  protected abstract Map<StoreKey, StoreKey> convertKeys(Collection<? extends StoreKey> input) throws Exception;

  protected abstract StoreKey getConvertedKey(StoreKey storeKey, Boolean isKeyPresent, StoreKey cachedMapping);
}
