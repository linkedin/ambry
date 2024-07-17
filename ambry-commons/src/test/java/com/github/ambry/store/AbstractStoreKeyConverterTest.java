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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link AbstractStoreKeyConverter}
 */
public class AbstractStoreKeyConverterTest {
  private final StoreKeyConverter storeKeyConverter;

  /**
   * Using {@link com.github.ambry.store.MockStoreKeyConverterFactory.MockStoreKeyConverter}
   * as it implements {@link AbstractStoreKeyConverter}
   */
  public AbstractStoreKeyConverterTest() {
    storeKeyConverter = new MockStoreKeyConverterFactory(null, null).setConversionMap(new HashMap<>())
        .setReturnInputIfAbsent(true)
        .getStoreKeyConverter();
  }

  /**
   * Ensures that {@link #storeKeyConverter} implements {@link AbstractStoreKeyConverter}
   * @throws Exception
   */
  @Test
  public void testClassImplementation() throws Exception {
    assertTrue("storeKeyConverter is not inheriting AbstractStoreKeyConverter",
        storeKeyConverter instanceof AbstractStoreKeyConverter);
  }

  /**
   * Test conversion of keys by  first try getting keys without converting,
   * then converting two keys try getting them ,
   * and then clearing cache and again try getting keys
   * @throws Exception
   */
  @Test
  public void testBasicOperations() throws Exception {
    StoreKey storeKey0 = mock(StoreKey.class);
    StoreKey storeKey1 = mock(StoreKey.class);
    assertNotSame("storeKeys should not be equal", storeKey0, storeKey1);
    List<StoreKey> list = Arrays.asList(storeKey0, storeKey1);

    try {
      storeKeyConverter.getConverted(storeKey0);
      fail("Get succeeded for not converted keys");
    } catch (Exception e) {
      //expected
    }

    Map<StoreKey, StoreKey> convertedMap = storeKeyConverter.convert(list);
    assertEquals("Size of keys in input and output is different", list.size(), convertedMap.size());

    try {
      StoreKey storeKey0Res = storeKeyConverter.getConverted(storeKey0);
      assertNotNull("Conversion of storeKey0 should not be null", storeKey0Res);
    } catch (Exception e) {
      fail("Exception thrown while storeKey0 is present");
    }

    storeKeyConverter.dropCache();
    try {
      storeKeyConverter.getConverted(storeKey0);
      fail("Get succeeded after cache is cleared");
    } catch (Exception e) {
      //expected
    }
  }

  /**
   * Testing caching logic by calling convert two time on a list of keys,
   * then try getting one of keys, then try removing these keys from cache,
   * then try getting these keys and then removing these keys again and try getting again
   * @throws Exception
   */
  @Test
  public void testCachingOperations() throws Exception {
    StoreKey storeKey0 = mock(StoreKey.class);
    StoreKey storeKey1 = mock(StoreKey.class);
    assertNotSame("storeKeys should not be equal", storeKey0, storeKey1);
    List<StoreKey> list = Arrays.asList(storeKey0, storeKey1);

    Map<StoreKey, StoreKey> convertedMap = storeKeyConverter.convert(list);
    assertEquals("Size of keys in input and output is different", list.size(), convertedMap.size());

    convertedMap = storeKeyConverter.convert(list);
    assertEquals("Size of keys in input and output is different", list.size(), convertedMap.size());

    storeKeyConverter.tryDropCache(list);

    try {
      StoreKey storeKey0Res = storeKeyConverter.getConverted(storeKey0);
      assertNotNull("Conversion of storeKey0 should not be null", storeKey0Res);
    } catch (Exception e) {
      fail("Exception thrown while storeKey0 is present");
    }

    storeKeyConverter.tryDropCache(list);
    try {
      storeKeyConverter.getConverted(storeKey0);
      fail("Get succeeded after cache is cleared");
    } catch (Exception e) {
      //expected
    }
  }

  /**
   * Testing overriding cache logic by calling convert on a list of keys
   * then adding a different mapping and trying conversion again and check if mapping is changed
   * @throws Exception
   */
  @Test
  public void testOverrideCacheOperation() throws Exception {
    StoreKey storeKey0 = mock(StoreKey.class);
    StoreKey storeKey1 = mock(StoreKey.class);
    assertNotSame("storeKeys should not be equal", storeKey0, storeKey1);
    List<StoreKey> list = Arrays.asList(storeKey0, storeKey1);

    Map<StoreKey, StoreKey> convertedMap = storeKeyConverter.convert(list);

    MockStoreKeyConverterFactory.MockStoreKeyConverter mockStoreKeyConverter =
        (MockStoreKeyConverterFactory.MockStoreKeyConverter) storeKeyConverter;

    assertEquals("storeKey should be mapped to itself", storeKey0, convertedMap.get(storeKey0));
    assertEquals("storeKey should be mapped to itself", storeKey1, convertedMap.get(storeKey1));
    assertEquals("storeKey should be mapped to itself", storeKey0, storeKeyConverter.getConverted(storeKey0));
    assertEquals("storeKey should be mapped to itself", storeKey1, storeKeyConverter.getConverted(storeKey1));
    ;

    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    conversionMap.put(storeKey0, storeKey1);
    conversionMap.put(storeKey1, storeKey0);
    mockStoreKeyConverter.setConversionMap(conversionMap);

    convertedMap = storeKeyConverter.convert(list);
    assertEquals("storeKey0 should be mapped to storeKey1", storeKey1, convertedMap.get(storeKey0));
    assertEquals("storeKey1 should be mapped to storeKey0", storeKey0, convertedMap.get(storeKey1));
    assertEquals("storeKey0 should be mapped to storeKey1", storeKey1, storeKeyConverter.getConverted(storeKey0));
    assertEquals("storeKey1 should be mapped to storeKey0", storeKey0, storeKeyConverter.getConverted(storeKey1));
  }
}
