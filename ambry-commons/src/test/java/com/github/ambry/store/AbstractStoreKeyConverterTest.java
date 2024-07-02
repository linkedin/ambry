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
package com.github.ambry.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class AbstractStoreKeyConverterTest {
  private final StoreKeyConverter storeKeyConverter;

  public AbstractStoreKeyConverterTest() {
    storeKeyConverter = new MockStoreKeyConverterFactory(null, null).setConversionMap(new HashMap<>())
        .setReturnInputIfAbsent(true)
        .getStoreKeyConverter();
  }

  @Test
  public void testClassImplementation() throws Exception {
    assertTrue("storeKeyConverter is not inheriting AbstractStoreKeyConverter",
        storeKeyConverter instanceof AbstractStoreKeyConverter);
  }

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
    }catch (Exception e){
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

    storeKeyConverter.remove(list);

    try {
      StoreKey storeKey0Res = storeKeyConverter.getConverted(storeKey0);
      assertNotNull("Conversion of storeKey0 should not be null", storeKey0Res);
    }catch (Exception e){
      fail("Exception thrown while storeKey0 is present");
    }

    storeKeyConverter.remove(list);
    try {
      storeKeyConverter.getConverted(storeKey0);
      fail("Get succeeded after cache is cleared");
    } catch (Exception e) {
      //expected
    }
  }
}
