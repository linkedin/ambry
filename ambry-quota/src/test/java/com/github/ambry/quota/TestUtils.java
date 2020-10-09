/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;


public class TestUtils {

  public static Map<String, Map<String, Long>> makeStorageMap(int numAccounts, int numContainerPerAccount,
      long maxValue, long minValue) {
    Random random = new Random();
    Map<String, Map<String, Long>> accountMap = new HashMap<>();

    short accountId = 1;
    for (int i = 0; i < numAccounts; i++) {
      Map<String, Long> containerMap = new HashMap<>();
      accountMap.put(String.valueOf(accountId), containerMap);

      short containerId = 1;
      for (int j = 0; j < numContainerPerAccount; j++) {
        long usage = Math.abs(random.nextLong()) % (maxValue - minValue) + minValue;
        containerMap.put(String.valueOf(containerId), usage);
        containerId++;
      }
      accountId++;
    }
    return accountMap;
  }

  /**
   * Compare two storage maps and fail the test when they are not equal.
   * @param expected The expected storage map.
   * @param obtained The obtained storage map.
   */
  public static void assertContainerMap(Map<String, Map<String, Long>> expected,
      Map<String, Map<String, Long>> obtained) {
    assertEquals(expected.size(), obtained.size());
    for (Map.Entry<String, Map<String, Long>> expectedEntry : expected.entrySet()) {
      String accountId = expectedEntry.getKey();
      Map<String, Long> expectedContainer = expectedEntry.getValue();
      assertTrue("Obtained map does contain account id " + accountId, obtained.containsKey(accountId));
      Map<String, Long> obtainedContainer = obtained.get(accountId);
      assertEquals("Size doesn't match for account id " + accountId, expectedContainer.size(),
          obtainedContainer.size());
      for (Map.Entry<String, Long> expectedContainerEntry : expectedContainer.entrySet()) {
        String containerId = expectedContainerEntry.getKey();
        assertTrue("Obtained map doesn't contain container id " + containerId + " in account id " + accountId,
            obtainedContainer.containsKey(containerId));
        assertEquals("Usage doesn't match for account id " + accountId + " container id " + containerId,
            expectedContainerEntry.getValue().longValue(), obtainedContainer.get(containerId).longValue());
      }
    }
  }
}
