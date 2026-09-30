/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
 * Forward and backward compatibility tests for {@link Dataset} / {@link DatasetBuilder}.
 */
public class DatasetBuilderCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Forward compatibility: unknown fields in Dataset JSON should be ignored during deserialization.
   */
  @Test
  public void testForwardCompatibility() throws Exception {
    String json = "{\"accountName\":\"testAccount\",\"containerName\":\"testContainer\","
        + "\"datasetName\":\"testDataset\",\"versionSchema\":\"TIMESTAMP\","
        + "\"retentionCount\":10,\"someNewField\":\"futureValue\",\"anotherNewField\":true}";
    Dataset deserialized = objectMapper.readValue(json, Dataset.class);
    assertEquals("testAccount", deserialized.getAccountName());
    assertEquals("testContainer", deserialized.getContainerName());
    assertEquals("testDataset", deserialized.getDatasetName());
    assertEquals(Dataset.VersionSchema.TIMESTAMP, deserialized.getVersionSchema());
    assertEquals(Integer.valueOf(10), deserialized.getRetentionCount());
  }

  /**
   * Backward compatibility: Dataset JSON without optional fields should deserialize with null defaults.
   */
  @Test
  public void testBackwardCompatibility() throws Exception {
    String json = "{\"accountName\":\"testAccount\",\"containerName\":\"testContainer\","
        + "\"datasetName\":\"testDataset\",\"versionSchema\":\"MONOTONIC\"}";
    Dataset deserialized = objectMapper.readValue(json, Dataset.class);
    assertEquals("testAccount", deserialized.getAccountName());
    assertEquals(Dataset.VersionSchema.MONOTONIC, deserialized.getVersionSchema());
    assertNull(deserialized.getRetentionPolicy());
    assertNull(deserialized.getRetentionCount());
    assertNull(deserialized.getRetentionTimeInSeconds());
    assertNull(deserialized.getUserTags());
  }
}
