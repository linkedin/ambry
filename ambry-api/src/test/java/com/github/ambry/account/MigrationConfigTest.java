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
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true, 25.0);
    MigrationConfig.ListRamp listRamp = new MigrationConfig.ListRamp(false, 70.0, 80.0, true);
    MigrationConfig.DeleteRamp deleteRamp = new MigrationConfig.DeleteRamp(false, 40.0, 20.0, 10.0, true);

    MigrationConfig migrationConfig = new MigrationConfig(true, writeRamp, readRamp, listRamp, deleteRamp);

    assertTrue(migrationConfig.isOverrideAccountMigrationConfig());
    assertEquals(writeRamp, migrationConfig.getWriteRamp());
    assertEquals(readRamp, migrationConfig.getReadRamp());
    assertEquals(listRamp, migrationConfig.getListRamp());
    assertEquals(deleteRamp, migrationConfig.getDeleteRamp());
    assertEquals(25.0, readRamp.getDualHeadSyncPct(), 0.001);

    // Verify DeleteRamp getters
    assertFalse(deleteRamp.isForceDisableDualDelete());
    assertEquals(40.0, deleteRamp.getDualDeleteAsyncPct(), 0.001);
    assertEquals(20.0, deleteRamp.getDualDeleteSyncPctNonStrict(), 0.001);
    assertEquals(10.0, deleteRamp.getDualDeleteSyncPctStrict(), 0.001);
    assertTrue(deleteRamp.isDeleteOnlyToSecondary());
  }

  @Test
  public void testJsonSerializationAndDeserialization() throws Exception {
    MigrationConfig.WriteRamp writeRamp = new MigrationConfig.WriteRamp(false, 50.0, 30.0, 20.0, true);
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true, 15.0);
    MigrationConfig.ListRamp listRamp = new MigrationConfig.ListRamp(false, 70.0, 80.0, true);
    MigrationConfig.DeleteRamp deleteRamp = new MigrationConfig.DeleteRamp(false, 40.0, 20.0, 10.0, true);

    MigrationConfig originalConfig = new MigrationConfig(true, writeRamp, readRamp, listRamp, deleteRamp);

    // Serialize to JSON
    String json = objectMapper.writeValueAsString(originalConfig);

    // Deserialize from JSON
    MigrationConfig deserializedConfig = objectMapper.readValue(json, MigrationConfig.class);

    assertEquals(originalConfig.isOverrideAccountMigrationConfig(), deserializedConfig.isOverrideAccountMigrationConfig());
    assertEquals(originalConfig.getWriteRamp(), deserializedConfig.getWriteRamp());
    assertEquals(originalConfig.getReadRamp(), deserializedConfig.getReadRamp());
    assertEquals(originalConfig.getListRamp(), deserializedConfig.getListRamp());
    assertEquals(originalConfig.getDeleteRamp(), deserializedConfig.getDeleteRamp());
  }

  @Test
  public void testDefaultValues() {
    MigrationConfig migrationConfig = new MigrationConfig(false, null, null, null, null);

    assertFalse(migrationConfig.isOverrideAccountMigrationConfig());
    assertNull(migrationConfig.getWriteRamp());
    assertNull(migrationConfig.getReadRamp());
    assertNull(migrationConfig.getListRamp());
    assertNull(migrationConfig.getDeleteRamp());
  }

  @Test
  public void testReadRampSixArgConstructorDefaultsDualHeadSyncPct() {
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true);
    assertEquals(0.0, readRamp.getDualHeadSyncPct(), 0.001);
    assertEquals(40.0, readRamp.getShadowReadMetadataPct(), 0.001);
    assertEquals(60.0, readRamp.getShadowReadMd5Pct(), 0.001);
    assertEquals(10.0, readRamp.getShadowReadContentPct(), 0.001);
    assertEquals(5.0, readRamp.getServeReadFromSecondaryPct(), 0.001);
    assertTrue(readRamp.isDisableFallbackToPrimary());
  }

  /**
   * Simulates an old consumer deserializing JSON that contains unknown future fields.
   * Without @JsonIgnoreProperties(ignoreUnknown = true), this throws UnrecognizedPropertyException.
   */
  @Test
  public void testDeserializationIgnoresUnknownFields() throws Exception {
    // ReadRamp JSON with an unknown field "someNewReadField"
    String readRampJson = "{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":10.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":5.0,\"someNewReadField\":99.0}";
    MigrationConfig.ReadRamp readRamp = objectMapper.readValue(readRampJson, MigrationConfig.ReadRamp.class);
    assertEquals(10.0, readRamp.getShadowReadMetadataPct(), 0.001);
    assertEquals(5.0, readRamp.getDualHeadSyncPct(), 0.001);

    // WriteRamp JSON with an unknown field "someNewWriteField"
    String writeRampJson = "{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":50.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false,\"someNewWriteField\":true}";
    MigrationConfig.WriteRamp writeRamp = objectMapper.readValue(writeRampJson, MigrationConfig.WriteRamp.class);
    assertEquals(50.0, writeRamp.getDualWriteAndDeleteAsyncPct(), 0.001);

    // ListRamp JSON with an unknown field "someNewListField"
    String listRampJson = "{\"forceDisableListFromSecondary\":false,\"shadowListPct\":30.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false,\"someNewListField\":\"hello\"}";
    MigrationConfig.ListRamp listRamp = objectMapper.readValue(listRampJson, MigrationConfig.ListRamp.class);
    assertEquals(30.0, listRamp.getShadowListPct(), 0.001);

    // DeleteRamp JSON with an unknown field "someNewDeleteField"
    String deleteRampJson = "{\"forceDisableDualDelete\":false,\"dualDeleteAsyncPct\":45.0,"
        + "\"dualDeleteSyncPctNonStrict\":0.0,\"dualDeleteSyncPctStrict\":0.0,"
        + "\"deleteOnlyToSecondary\":false,\"someNewDeleteField\":true}";
    MigrationConfig.DeleteRamp deleteRamp = objectMapper.readValue(deleteRampJson, MigrationConfig.DeleteRamp.class);
    assertEquals(45.0, deleteRamp.getDualDeleteAsyncPct(), 0.001);
  }

  /**
   * Forward compatibility: MigrationConfig (outer class) with unknown fields should deserialize successfully.
   */
  @Test
  public void testMigrationConfigIgnoresUnknownFields() throws Exception {
    String json = "{\"overrideAccountMigrationConfig\":true,"
        + "\"writeRamp\":{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":10.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":5.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false},"
        + "\"someNewTopLevelField\":\"futureValue\"}";
    MigrationConfig deserialized = objectMapper.readValue(json, MigrationConfig.class);
    assertTrue(deserialized.isOverrideAccountMigrationConfig());
    assertEquals(10.0, deserialized.getWriteRamp().getDualWriteAndDeleteAsyncPct(), 0.001);
    assertEquals(5.0, deserialized.getReadRamp().getShadowReadMetadataPct(), 0.001);
  }

  /**
   * Backward compatibility: MigrationConfig with missing optional ramps should deserialize with nulls.
   */
  @Test
  public void testMigrationConfigBackwardCompatibility() throws Exception {
    String json = "{\"overrideAccountMigrationConfig\":false}";
    MigrationConfig deserialized = objectMapper.readValue(json, MigrationConfig.class);
    assertFalse(deserialized.isOverrideAccountMigrationConfig());
    assertNull(deserialized.getWriteRamp());
    assertNull(deserialized.getReadRamp());
    assertNull(deserialized.getListRamp());
  }

  @Test
  public void testDeserializationWithoutDualHeadSyncPct() throws Exception {
    String json = "{\"overrideAccountMigrationConfig\":false,"
        + "\"writeRamp\":{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":50.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":10.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}}";

    MigrationConfig deserialized = objectMapper.readValue(json, MigrationConfig.class);
    assertEquals(0.0, deserialized.getReadRamp().getDualHeadSyncPct(), 0.001);
    assertEquals(10.0, deserialized.getReadRamp().getShadowReadMetadataPct(), 0.001);
  }

  @Test
  public void testDeserializationWithoutDeleteRamp() throws Exception {
    String json = "{\"overrideAccountMigrationConfig\":false,"
        + "\"writeRamp\":{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":50.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":10.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}}";

    MigrationConfig deserialized = objectMapper.readValue(json, MigrationConfig.class);
    assertNull("deleteRamp should be null when absent from JSON", deserialized.getDeleteRamp());
    assertNotNull(deserialized.getWriteRamp());
    assertNotNull(deserialized.getReadRamp());
    assertNotNull(deserialized.getListRamp());
  }

  @Test
  public void testFourArgConstructorBackwardCompat() {
    MigrationConfig.WriteRamp writeRamp = new MigrationConfig.WriteRamp();
    MigrationConfig.ReadRamp readRamp = new MigrationConfig.ReadRamp();
    MigrationConfig.ListRamp listRamp = new MigrationConfig.ListRamp();

    MigrationConfig config = new MigrationConfig(false, writeRamp, readRamp, listRamp);

    assertNull("deleteRamp should be null when using 4-arg constructor", config.getDeleteRamp());
    assertEquals(writeRamp, config.getWriteRamp());
    assertEquals(readRamp, config.getReadRamp());
    assertEquals(listRamp, config.getListRamp());
  }

  @Test
  public void testDeleteRampDefaultConstructor() {
    MigrationConfig.DeleteRamp deleteRamp = new MigrationConfig.DeleteRamp();

    assertFalse(deleteRamp.isForceDisableDualDelete());
    assertEquals(0.0, deleteRamp.getDualDeleteAsyncPct(), 0.001);
    assertEquals(0.0, deleteRamp.getDualDeleteSyncPctNonStrict(), 0.001);
    assertEquals(0.0, deleteRamp.getDualDeleteSyncPctStrict(), 0.001);
    assertFalse(deleteRamp.isDeleteOnlyToSecondary());
  }

  @Test
  public void testDeleteRampEqualsAndHashCode() {
    MigrationConfig.DeleteRamp ramp1 = new MigrationConfig.DeleteRamp(true, 50.0, 30.0, 20.0, false);
    MigrationConfig.DeleteRamp ramp2 = new MigrationConfig.DeleteRamp(true, 50.0, 30.0, 20.0, false);
    MigrationConfig.DeleteRamp ramp3 = new MigrationConfig.DeleteRamp(false, 50.0, 30.0, 20.0, false);

    assertEquals(ramp1, ramp2);
    assertEquals(ramp1.hashCode(), ramp2.hashCode());
    assertNotEquals(ramp1, ramp3);
  }
}