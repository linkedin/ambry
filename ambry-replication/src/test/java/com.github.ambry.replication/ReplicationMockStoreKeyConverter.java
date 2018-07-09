/**
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

package com.github.ambry.replication;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


class ReplicationMockStoreKeyConverter implements StoreKeyConverter {

  private boolean throwException = false;

  private final Map<StoreKey, StoreKey> map = new HashMap<>();
  private Map<StoreKey, StoreKey> lastAnswer = new HashMap<>();

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
    lastAnswer = answer;
    return answer;
  }

  @Override
  public StoreKey getConverted(StoreKey storeKey) {
    if (!lastAnswer.containsKey(storeKey)) {
      throw new IllegalStateException("Did not convert key yet.  Key: " + storeKey);
    }
    return lastAnswer.get(storeKey);
  }

  /**
   * Same as Exception; just used to identify exceptions
   * thrown by the outer class
   */
  static class MockStoreKeyConverterException extends Exception {

  }
}
