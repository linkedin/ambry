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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Forward and backward compatibility tests for {@link HelixParticipant.MaintenanceRecord}.
 */
public class MaintenanceRecordCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Forward compatibility: unknown fields in MaintenanceRecord JSON should be ignored.
   */
  @Test
  public void testForwardCompatibility() throws Exception {
    String json = "{\"DATE\":\"2026-03-28\",\"TIMESTAMP\":\"1711584000000\","
        + "\"OPERATION_TYPE\":\"ENABLE\",\"REASON\":\"test reason\","
        + "\"TRIGGERED_BY\":\"CONTROLLER\",\"USER\":\"admin\","
        + "\"SOME_NEW_HELIX_FIELD\":\"futureValue\",\"ANOTHER_FIELD\":123}";
    HelixParticipant.MaintenanceRecord record =
        objectMapper.readValue(json, HelixParticipant.MaintenanceRecord.class);
    assertEquals("2026-03-28", record.date);
    assertEquals("1711584000000", record.timestamp);
    assertEquals("ENABLE", record.operationType);
    assertEquals("test reason", record.reason);
    assertEquals("CONTROLLER", record.triggeredBy);
    assertEquals("admin", record.user);
  }

  /**
   * Backward compatibility: MaintenanceRecord with only some fields should deserialize with null defaults.
   */
  @Test
  public void testBackwardCompatibility() throws Exception {
    String json = "{\"DATE\":\"2026-03-28\",\"TIMESTAMP\":\"1711584000000\","
        + "\"OPERATION_TYPE\":\"ENABLE\"}";
    HelixParticipant.MaintenanceRecord record =
        objectMapper.readValue(json, HelixParticipant.MaintenanceRecord.class);
    assertEquals("2026-03-28", record.date);
    assertEquals("ENABLE", record.operationType);
    assertNull(record.reason);
    assertNull(record.triggeredBy);
    assertNull(record.user);
  }
}
