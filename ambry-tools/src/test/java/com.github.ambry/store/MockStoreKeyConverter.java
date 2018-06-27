/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Mock StoreKeyConverter, basically a wrapper over a hashmap.
 * Not found StoreKeys are returned with the value == key
 */
class MockStoreKeyConverter implements StoreKeyConverter {

  private boolean throwException = false;

  private final Map<StoreKey, StoreKey> map = new HashMap<>();

  void put(StoreKey key, StoreKey value) {
    map.put(key, value);
  }

  void setThrowException(boolean bool) {
    throwException = bool;
  }

  @Override
  public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> collection) throws Exception {
    if (throwException) {
      throw new MockStoreKeyConverterException();
    }
    Map<StoreKey, StoreKey> answer = new HashMap<>();
    for (StoreKey storeKey : collection) {
      if (!map.containsKey(storeKey)) {
        answer.put(storeKey, storeKey);
      } else {
        answer.put(storeKey, map.get(storeKey));
      }
    }
    return answer;
  }

  /**
   * Same as Exception; just used to identify exceptions
   * thrown by the outer class
   */
  static class MockStoreKeyConverterException extends Exception {

  }
}
