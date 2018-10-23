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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * A mock factory of {@link StoreKeyConverterFactory}.  Creates MockStoreKeyConverter.
 */
public class MockStoreKeyConverterFactory implements StoreKeyConverterFactory {
  private Map<StoreKey, StoreKey> conversionMap;
  private Exception exception;
  private boolean returnKeyIfAbsent;

  public MockStoreKeyConverterFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
  }

  @Override
  public MockStoreKeyConverter getStoreKeyConverter() {
    return new MockStoreKeyConverter(new HashMap<>(conversionMap));
  }

  /**
   * Set conversionMap for reference.
   * @param conversionMap initially used by {@link MockStoreKeyConverter} instances
   *                      created by the factory.
   */
  public void setConversionMap(Map<StoreKey, StoreKey> conversionMap) {
    this.conversionMap = conversionMap;
  }

  /**
   * Get the conversionMap used by {@link MockStoreKeyConverter}
   */
  public Map<StoreKey, StoreKey> getConversionMap() {
    return conversionMap;
  }

  /**
   * Set Exception for {@link MockStoreKeyConverter#convert(Collection)}
   * @param e is the exception to be thrown.
   */
  public void setException(Exception e) {
    this.exception = e;
  }

  /**
   * Sets whether produced StoreKeyConverters will return the
   * input key if it is absent from the underlying map. If false,
   * the StoreKeyConverter will return null for missing inputs
   * @param returnInputIfAbsent
   */
  public void setReturnInputIfAbsent(boolean returnInputIfAbsent) {
    returnKeyIfAbsent = returnInputIfAbsent;
  }

  /**
   * A mock implementation of {@link StoreKeyConverter}.
   */
  public class MockStoreKeyConverter implements StoreKeyConverter {

    private Map<StoreKey, StoreKey> lastConverted = new HashMap<>();
    private Map<StoreKey, StoreKey> conversionMap;

    private MockStoreKeyConverter(Map<StoreKey, StoreKey> conversionMap) {
      this.conversionMap = conversionMap;
    }

    /**
     * Set conversionMap for reference.
     * @param conversionMap used by this {@link MockStoreKeyConverter} instance
     */
    public void setConversionMap(Map<StoreKey, StoreKey> conversionMap) {
      this.conversionMap = conversionMap;
    }

    @Override
    public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> input) throws Exception {
      if (exception != null) {
        throw exception;
      }
      Map<StoreKey, StoreKey> output = new HashMap<>();
      if (input != null) {
        for (StoreKey storeKey : input) {
          if (returnKeyIfAbsent && !conversionMap.containsKey(storeKey)) {
            output.put(storeKey, storeKey);
          } else {
            output.put(storeKey, conversionMap.get(storeKey));
          }
        }
      }
      //conversion gets added to cache
      if (lastConverted == null) {
        lastConverted = output;
      } else {
        lastConverted.putAll(output);
      }
      return output;
    }

    @Override
    public StoreKey getConverted(StoreKey storeKey) {
      if (exception != null) {
        throw new IllegalStateException(exception);
      } else if (!lastConverted.containsKey(storeKey)) {
        throw new IllegalStateException(storeKey + " has not been converted");
      }
      return lastConverted.get(storeKey);
    }

    @Override
    public void dropCache() {
      lastConverted = null;
    }
  }
}
