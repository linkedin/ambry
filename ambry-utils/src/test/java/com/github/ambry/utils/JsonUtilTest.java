/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link JsonUtil}.
 */
public class JsonUtilTest {

  @Test
  public void testNewObjectMapperDisablesFailOnUnknownProperties() {
    ObjectMapper mapper = JsonUtil.newObjectMapper();
    assertFalse("FAIL_ON_UNKNOWN_PROPERTIES should be disabled",
        mapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
  }

  @Test
  public void testNewObjectMapperReturnsDistinctInstances() {
    ObjectMapper first = JsonUtil.newObjectMapper();
    ObjectMapper second = JsonUtil.newObjectMapper();
    assertNotSame("Each call should return a new instance", first, second);
  }

  @Test
  public void testNewObjectMapperToleratesUnknownFields() throws Exception {
    ObjectMapper mapper = JsonUtil.newObjectMapper();
    String json = "{\"known\":\"value\",\"surprise\":123}";
    KnownFieldsPojo pojo = mapper.readValue(json, KnownFieldsPojo.class);
    assertEquals("value", pojo.known);
  }

  /** Minimal POJO used only for the unknown-fields tolerance test. */
  public static class KnownFieldsPojo {
    public String known;
  }
}
