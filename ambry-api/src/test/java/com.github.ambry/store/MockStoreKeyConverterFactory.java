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
 * A mock factory of {@link StoreKeyConverterFactory}.  Creates MockStoreKeyConverter.
 */
public class MockStoreKeyConverterFactory implements StoreKeyConverterFactory {
  private StoreKeyConverter storeKeyConverter = new MockStoreKeyConverter();
  private Map<StoreKey, StoreKey> conversionMap;
  private Exception exception;

  @Override
  public StoreKeyConverter getStoreKeyConverter() {
    return storeKeyConverter;
  }

  public void setConversionMap(Map<StoreKey, StoreKey> conversionMap) {
    this.conversionMap = conversionMap;
  }

  public void setException(Exception e) {
    this.exception = e;
  }

  /**
   * A mock implementation of {@link StoreKeyConverter}.
   */
  private class MockStoreKeyConverter implements StoreKeyConverter {
    @Override
    public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> input) throws Exception {
      if (exception != null) {
        throw exception;
      }
      Map<StoreKey, StoreKey> output = new HashMap<>();
      if (input != null) {
        input.forEach((storeKey) -> output.put(storeKey, conversionMap.get(storeKey)));
      }
      return output;
    }
  }
}
