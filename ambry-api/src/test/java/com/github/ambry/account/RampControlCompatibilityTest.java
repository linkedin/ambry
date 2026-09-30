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
package com.github.ambry.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Forward and backward compatibility tests for {@link RampControl}.
 */
public class RampControlCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Forward compatibility: unknown fields in JSON should be ignored.
   */
  @Test
  public void testForwardCompatibility() throws Exception {
    String json = "{\"secondaryEnabled\":true,\"someNewField\":\"futureValue\",\"anotherNewField\":42}";
    RampControl deserialized = objectMapper.readValue(json, RampControl.class);
    assertTrue(deserialized.isSecondaryEnabled());
  }

  /**
   * Backward compatibility: minimal JSON with only known fields should deserialize correctly.
   */
  @Test
  public void testBackwardCompatibility() throws Exception {
    String json = "{\"secondaryEnabled\":false}";
    RampControl deserialized = objectMapper.readValue(json, RampControl.class);
    assertFalse(deserialized.isSecondaryEnabled());
  }
}
