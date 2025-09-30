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

public class MigrationConfigTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testConstructorAndGetters() {
    MigrationConfig.WriteRamp writeRamp = new MigrationConfig.WriteRamp(false, 50.0, 30.0, 20.0, true);
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true);
    MigrationConfig.ListRamp listRamp = new MigrationConfig.ListRamp(false, 70.0, 80.0, true);

    MigrationConfig migrationConfig = new MigrationConfig(true, writeRamp, readRamp, listRamp);

    assertTrue(migrationConfig.isOverrideAccountMigrationConfig());
    assertEquals(writeRamp, migrationConfig.getWriteRamp());
    assertEquals(readRamp, migrationConfig.getReadRamp());
    assertEquals(listRamp, migrationConfig.getListRamp());
  }

  @Test
  public void testJsonSerializationAndDeserialization() throws Exception {
    MigrationConfig.WriteRamp writeRamp = new MigrationConfig.WriteRamp(false, 50.0, 30.0, 20.0, true);
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true);
    MigrationConfig.ListRamp listRamp = new MigrationConfig.ListRamp(false, 70.0, 80.0, true);

    MigrationConfig originalConfig = new MigrationConfig(true, writeRamp, readRamp, listRamp);

    // Serialize to JSON
    String json = objectMapper.writeValueAsString(originalConfig);

    // Deserialize from JSON
    MigrationConfig deserializedConfig = objectMapper.readValue(json, MigrationConfig.class);

    assertEquals(originalConfig.isOverrideAccountMigrationConfig(), deserializedConfig.isOverrideAccountMigrationConfig());
    assertEquals(originalConfig.getWriteRamp(), deserializedConfig.getWriteRamp());
    assertEquals(originalConfig.getReadRamp(), deserializedConfig.getReadRamp());
    assertEquals(originalConfig.getListRamp(), deserializedConfig.getListRamp());
  }

  @Test
  public void testDefaultValues() {
    MigrationConfig migrationConfig = new MigrationConfig(false, null, null, null);

    assertFalse(migrationConfig.isOverrideAccountMigrationConfig());
    assertNull(migrationConfig.getWriteRamp());
    assertNull(migrationConfig.getReadRamp());
    assertNull(migrationConfig.getListRamp());
  }
}