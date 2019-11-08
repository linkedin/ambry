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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link StoreKeyConverterImplNoOp}
 */
public class StoreKeyConverterImplNoOpTest {

  private final StoreKeyConverter storeKeyConverter;

  public StoreKeyConverterImplNoOpTest() {
    storeKeyConverter = new StoreKeyConverterImplNoOp();
  }

  /**
   * Tests conversion of a StoreKey collection with two unique StoreKeys and a duplicate of one of them
   * @throws Exception
   */
  @Test
  public void testBasicOperationWithDuplicate() throws Exception {
    StoreKey storeKey0 = mock(StoreKey.class);
    StoreKey storeKey1 = mock(StoreKey.class);
    StoreKey storeKey2 = storeKey0;
    assertNotSame("storeKeys should not be equal", storeKey0, storeKey1);
    assertSame("storeKey0 and storeKey2 should be equal", storeKey0, storeKey2);
    List<StoreKey> list = Arrays.asList(storeKey0, storeKey1, storeKey2);
    Map<StoreKey, StoreKey> storeKeyMap = storeKeyConverter.convert(list);
    assertEquals("Returned mapping does not have size 2", 2, storeKeyMap.size());
    assertTrue("storeKey0 not in mapping", storeKeyMap.containsKey(storeKey0));
    assertTrue("storeKey1 not in mapping", storeKeyMap.containsKey(storeKey1));
    storeKeyMap.forEach(
        (key, value) -> assertEquals("Returned StoreKey keys should be the same as their values", key, value));
    assertEquals("storeKey0 not equal", storeKey0, storeKeyConverter.getConverted(storeKey0));
    assertEquals("storeKey1 not equal", storeKey1, storeKeyConverter.getConverted(storeKey1));
  }

  /**
   * Tests conversion of an empty Collection
   * @throws Exception
   */
  @Test
  public void testEmptyInput() throws Exception {
    Map<StoreKey, StoreKey> storeKeyMap = storeKeyConverter.convert(new ArrayList<>());
    assertEquals("storeKeyMap should not have inputs", storeKeyMap.size(), 0);
  }

  /**
   * Tests conversion of a null input
   * @throws Exception
   */
  @Test
  public void testNullInput() throws Exception {
    Map<StoreKey, StoreKey> storeKeyMap = storeKeyConverter.convert(null);
    assertEquals("storeKeyMap should not have inputs", storeKeyMap.size(), 0);
  }
}
