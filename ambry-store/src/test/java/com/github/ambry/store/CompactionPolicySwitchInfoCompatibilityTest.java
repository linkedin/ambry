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
package com.github.ambry.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Forward and backward compatibility tests for {@link CompactionPolicySwitchInfo}.
 */
public class CompactionPolicySwitchInfoCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Forward compatibility: unknown fields in CompactionPolicySwitchInfo JSON should be ignored.
   */
  @Test
  public void testForwardCompatibility() throws Exception {
    String json = "{\"lastCompactAllTime\":1000,\"nextRoundIsCompactAllPolicy\":true,"
        + "\"someNewField\":\"futureValue\",\"anotherNewField\":42}";
    CompactionPolicySwitchInfo deserialized = objectMapper.readValue(json, CompactionPolicySwitchInfo.class);
    assertEquals(1000L, deserialized.getLastCompactAllTime());
    assertTrue(deserialized.isNextRoundCompactAllPolicy());
  }

  /**
   * Backward compatibility: minimal JSON should deserialize with default values.
   */
  @Test
  public void testBackwardCompatibility() throws Exception {
    String json = "{\"lastCompactAllTime\":0,\"nextRoundIsCompactAllPolicy\":false}";
    CompactionPolicySwitchInfo deserialized = objectMapper.readValue(json, CompactionPolicySwitchInfo.class);
    assertEquals(0L, deserialized.getLastCompactAllTime());
    assertFalse(deserialized.isNextRoundCompactAllPolicy());
  }
}
