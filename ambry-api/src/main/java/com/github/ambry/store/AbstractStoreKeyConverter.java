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


/**
 * This is a counting cache based abstract implementation of {@link StoreKeyConverter}
 * that can be used to convert store keys across different formats.
 * </p>
 * In this implementation keys will be cached when {@link #convert)} is called,
 * and you need to call {@link #tryDropCache(Collection)} for the key same time as convert is called to remove it from the cache.
 * You can also drop all cache in a single call to {@link #dropCache()}
 */
public abstract class AbstractStoreKeyConverter implements StoreKeyConverter {
  private final Map<StoreKey, StoreKey> conversionCache = new HashMap<>();
  private final Map<StoreKey, Integer> cachedKeyCount = new HashMap<>();

  /**
   * Even if any key is present in cache, conversion will be attempted for all the keys
   * @param input the {@link StoreKey}s that need to be converted.
   * @return {@link Map} map of storeKeys to converted keys
   * @throws Exception
   */
  @Override
  public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> input) throws Exception {
    if (input == null) {
      return new HashMap<>();
    }

    Map<StoreKey, StoreKey> map = convertKeys(input);

    map.forEach((storeKey, convertedStoreKey) -> {
      cachedKeyCount.put(storeKey, cachedKeyCount.getOrDefault(storeKey, 0) + 1);
    });

    conversionCache.putAll(map);
    return Collections.unmodifiableMap(map);
  }

  /**
   * @param storeKey storeKey you want the converted version of.  If the key was not apart
   *                 of a previous {@link #convert(Collection)} call, method may throw
   *                 IllegalStateException
   * @return
   */
  @Override
  public StoreKey getConverted(StoreKey storeKey) {
    return getConvertedKey(storeKey, conversionCache.containsKey(storeKey), conversionCache.get(storeKey));
  }

  /**
   * This will remove the given keys from cache when it is called as
   * same number of times as {@link #convert(Collection)} for a particular key
   * @param storeKeys storeKeys you want to try to remove from cache
   */
  @Override
  public void tryDropCache(Collection<? extends StoreKey> storeKeys) {
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

  /**
   * Drops the Cache reference used by {@link AbstractStoreKeyConverter}
   */
  @Override
  public void dropCache() {
    cachedKeyCount.clear();
    conversionCache.clear();
  }

  /**
   * This method will be called for the keys which are not present in cache
   * @param input StoreKeys to be converted
   */
  protected abstract Map<StoreKey, StoreKey> convertKeys(Collection<? extends StoreKey> input) throws Exception;

  /**
   * This methods response will be returned from {@link #getConverted(StoreKey)} method.
   * @param storeKey  StoreKey that was queried
   * @param isKeyPresent whether key is present in cache
   * @param cachedMapping convertedMapping that is stored in cache. Will be null if if key is not present in cache.
   * @return
   */
  protected abstract StoreKey getConvertedKey(StoreKey storeKey, boolean isKeyPresent, StoreKey cachedMapping);
}
