/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link ClusterMapUtils}.
 */
public class ClusterMapUtilsTest {
  private final String dc1Name = "dc1";
  private final String dc2Name = "dc2";
  private final String dc3Name = "dc3";
  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Tests the {@link ClusterMapUtils#getDcNameToIdMap(Set)} method with a list of different datacenter names.
   */
  @Test
  public void testGoodDcNamesForGetDcNameToIdMap() {
    Set<String> goodDcNames = new HashSet<>(Arrays.asList(new String[]{dc1Name, dc2Name, dc3Name}));
    Map<String, Short> dcNameToIdMap = getDcNameToIdMap(goodDcNames);
    assertEquals("Wrong map size", goodDcNames.size(), dcNameToIdMap.entrySet().size());
    Set<Short> idSet = new HashSet<>();
    for (Short id : dcNameToIdMap.values()) {
      if (!idSet.add(id)) {
        fail("Datacenter ids collide.");
      }
    }
  }

  /**
   * Tests the case when ids collide when calling {@link ClusterMapUtils#getDcNameToIdMap(Set)}.
   */
  @Test
  public void testDcIdCollision() {
    Set<String> stringSet = new HashSet<>();
    try {
      for (int i = 0; i <= Short.MAX_VALUE; i++) {
        String str = generateRandomString();
        assertTrue("Datacenter id cannot be negative.", hashDcNameToNonNegativeId(str) >= 0);
        if (stringSet.add(str)) {
          System.out.println(
              "Random string: " + str + " - Hash code: " + hashDcNameToNonNegativeId(str) + " - round: " + i);
          getDcNameToIdMap(stringSet);
        }
      }
      fail("should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Generates a random string.
   * @return A randomly generated string.
   */
  private String generateRandomString() {
    return new BigInteger(100, RANDOM).toString(32);
  }
}
