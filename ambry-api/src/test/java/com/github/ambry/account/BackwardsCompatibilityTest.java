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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static org.junit.Assert.*;


/**
 * Backwards compatibility regression tests for Account and MigrationConfig JSON serialization.
 *
 * These tests use hardcoded "snapshot" JSON strings that represent the wire format as it exists today.
 * They serve two purposes:
 *
 * 1. Old JSON (missing new fields) must always deserialize successfully with safe defaults.
 *    This catches cases where a new required constructor parameter breaks deserialization of
 *    data already stored in ZooKeeper/Helix.
 *
 * 2. The set of serialized field names is checked against an expected set. When a developer adds
 *    a new field, this test will fail, forcing them to:
 *    (a) Update the expected field set in this test
 *    (b) Add a new "deserialize old JSON without new field" test case
 *    (c) Ensure a safe default value is used when the field is absent
 *
 * HOW TO UPDATE WHEN ADDING A NEW FIELD:
 *   1. Add the new field name to the EXPECTED_*_FIELDS set in the relevant test
 *   2. Add a new @Test method that deserializes the OLD snapshot JSON (without your new field)
 *      and asserts the default value is correct
 *   3. Update the snapshot JSON string only if you're changing the semantics of existing fields
 */
public class BackwardsCompatibilityTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  // ==================== MigrationConfig Snapshots ====================

  /**
   * Snapshot of MigrationConfig JSON as of the current schema.
   * This represents data that may already exist in production ZooKeeper/Helix stores.
   * DO NOT modify this string when adding new fields — that's the whole point of the test.
   */
  private static final String MIGRATION_CONFIG_SNAPSHOT_JSON = "{"
      + "\"overrideAccountMigrationConfig\":true,"
      + "\"writeRamp\":{"
      + "\"forceDisableDualWriteAndDelete\":false,"
      + "\"dualWriteAndDeleteAsyncPct\":50.0,"
      + "\"dualWriteAndDeleteSyncPctNonStrict\":30.0,"
      + "\"dualWriteAndDeleteSyncPctStrict\":20.0,"
      + "\"writeAndDeleteOnlyToSecondary\":true"
      + "},"
      + "\"readRamp\":{"
      + "\"forceDisableReadFromSecondary\":false,"
      + "\"shadowReadMetadataPct\":40.0,"
      + "\"shadowReadMd5Pct\":60.0,"
      + "\"shadowReadContentPct\":10.0,"
      + "\"serveReadFromSecondaryPct\":5.0,"
      + "\"disableFallbackToPrimary\":true,"
      + "\"dualHeadSyncPct\":25.0"
      + "},"
      + "\"listRamp\":{"
      + "\"forceDisableListFromSecondary\":false,"
      + "\"shadowListPct\":70.0,"
      + "\"serveListFromSecondaryPct\":80.0,"
      + "\"disableFallbackToPrimary\":true"
      + "}}";

  /**
   * Older snapshot that predates the dualHeadSyncPct field in ReadRamp.
   * Represents data written by older Ambry versions that may still exist in stores.
   */
  private static final String MIGRATION_CONFIG_V1_SNAPSHOT_JSON = "{"
      + "\"overrideAccountMigrationConfig\":false,"
      + "\"writeRamp\":{"
      + "\"forceDisableDualWriteAndDelete\":false,"
      + "\"dualWriteAndDeleteAsyncPct\":50.0,"
      + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,"
      + "\"dualWriteAndDeleteSyncPctStrict\":0.0,"
      + "\"writeAndDeleteOnlyToSecondary\":false"
      + "},"
      + "\"readRamp\":{"
      + "\"forceDisableReadFromSecondary\":false,"
      + "\"shadowReadMetadataPct\":10.0,"
      + "\"shadowReadMd5Pct\":0.0,"
      + "\"shadowReadContentPct\":0.0,"
      + "\"serveReadFromSecondaryPct\":0.0,"
      + "\"disableFallbackToPrimary\":false"
      + "},"
      + "\"listRamp\":{"
      + "\"forceDisableListFromSecondary\":false,"
      + "\"shadowListPct\":0.0,"
      + "\"serveListFromSecondaryPct\":0.0,"
      + "\"disableFallbackToPrimary\":false"
      + "}}";

  // ==================== Account Snapshot ====================

  /**
   * Snapshot of Account JSON (without containers) as of the current schema.
   */
  private static final String ACCOUNT_SNAPSHOT_JSON = "{"
      + "\"accountId\":101,"
      + "\"accountName\":\"TestAccount\","
      + "\"status\":\"ACTIVE\","
      + "\"aclInheritedByContainer\":false,"
      + "\"snapshotVersion\":1,"
      + "\"lastModifiedTime\":1000,"
      + "\"quotaResourceType\":\"ACCOUNT\","
      + "\"version\":1,"
      + "\"containers\":[],"
      + "\"rampControl\":{\"secondaryEnabled\":true},"
      + "\"migrationConfig\":{"
      + "\"overrideAccountMigrationConfig\":false,"
      + "\"writeRamp\":{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":50.0,"
      + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
      + "\"writeAndDeleteOnlyToSecondary\":false},"
      + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":0.0,"
      + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
      + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
      + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
      + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}"
      + "},"
      + "\"migrationConfigs\":null"
      + "}";

  /**
   * Older Account snapshot that predates rampControl, migrationConfig, and migrationConfigs fields.
   */
  private static final String ACCOUNT_V1_SNAPSHOT_JSON = "{"
      + "\"accountId\":101,"
      + "\"accountName\":\"TestAccount\","
      + "\"status\":\"ACTIVE\","
      + "\"aclInheritedByContainer\":false,"
      + "\"snapshotVersion\":1,"
      + "\"lastModifiedTime\":0,"
      + "\"quotaResourceType\":\"ACCOUNT\","
      + "\"version\":1,"
      + "\"containers\":[]"
      + "}";

  // ==================== Expected Field Sets ====================
  // When you add a new field to a class, add it here too. This is the change-detector.

  private static final Set<String> EXPECTED_MIGRATION_CONFIG_FIELDS = Set.of(
      "overrideAccountMigrationConfig", "writeRamp", "readRamp", "listRamp"
  );

  private static final Set<String> EXPECTED_WRITE_RAMP_FIELDS = Set.of(
      "forceDisableDualWriteAndDelete", "dualWriteAndDeleteAsyncPct",
      "dualWriteAndDeleteSyncPctNonStrict", "dualWriteAndDeleteSyncPctStrict",
      "writeAndDeleteOnlyToSecondary"
  );

  private static final Set<String> EXPECTED_READ_RAMP_FIELDS = Set.of(
      "forceDisableReadFromSecondary", "shadowReadMetadataPct", "shadowReadMd5Pct",
      "shadowReadContentPct", "serveReadFromSecondaryPct", "disableFallbackToPrimary",
      "dualHeadSyncPct"
  );

  private static final Set<String> EXPECTED_LIST_RAMP_FIELDS = Set.of(
      "forceDisableListFromSecondary", "shadowListPct", "serveListFromSecondaryPct",
      "disableFallbackToPrimary"
  );

  // Note: "secondaryEnabled" is a computed getter (Account#isSecondaryEnabled()) that Jackson serializes
  // as a top-level property, even though it is derived from rampControl and not independently stored.
  // It must be in this set because it appears in serialized output, but it is NOT part of the stored
  // wire format — old snapshot JSONs intentionally omit it since AccountBuilder ignores it on input.
  private static final Set<String> EXPECTED_ACCOUNT_FIELDS = Set.of(
      "accountId", "accountName", "status", "aclInheritedByContainer", "snapshotVersion",
      "lastModifiedTime", "quotaResourceType", "version", "containers",
      "rampControl", "migrationConfig", "migrationConfigs", "secondaryEnabled"
  );

  private static final Set<String> EXPECTED_RAMP_CONTROL_FIELDS = Set.of(
      "secondaryEnabled"
  );

  // ==================== MigrationConfig Tests ====================

  /**
   * Verify the current MigrationConfig snapshot deserializes correctly with all field values preserved.
   */
  @Test
  public void testMigrationConfigSnapshotDeserialization() throws Exception {
    MigrationConfig config = objectMapper.readValue(MIGRATION_CONFIG_SNAPSHOT_JSON, MigrationConfig.class);

    assertTrue(config.isOverrideAccountMigrationConfig());

    // WriteRamp
    MigrationConfig.WriteRamp writeRamp = config.getWriteRamp();
    assertFalse(writeRamp.isForceDisableDualWriteAndDelete());
    assertEquals(50.0, writeRamp.getDualWriteAndDeleteAsyncPct(), 0.001);
    assertEquals(30.0, writeRamp.getDualWriteAndDeleteSyncPctNonStrict(), 0.001);
    assertEquals(20.0, writeRamp.getDualWriteAndDeleteSyncPctStrict(), 0.001);
    assertTrue(writeRamp.isWriteAndDeleteOnlyToSecondary());

    // ReadRamp
    MigrationConfig.ReadRamp readRamp = config.getReadRamp();
    assertFalse(readRamp.isForceDisableReadFromSecondary());
    assertEquals(40.0, readRamp.getShadowReadMetadataPct(), 0.001);
    assertEquals(60.0, readRamp.getShadowReadMd5Pct(), 0.001);
    assertEquals(10.0, readRamp.getShadowReadContentPct(), 0.001);
    assertEquals(5.0, readRamp.getServeReadFromSecondaryPct(), 0.001);
    assertTrue(readRamp.isDisableFallbackToPrimary());
    assertEquals(25.0, readRamp.getDualHeadSyncPct(), 0.001);

    // ListRamp
    MigrationConfig.ListRamp listRamp = config.getListRamp();
    assertFalse(listRamp.isForceDisableListFromSecondary());
    assertEquals(70.0, listRamp.getShadowListPct(), 0.001);
    assertEquals(80.0, listRamp.getServeListFromSecondaryPct(), 0.001);
    assertTrue(listRamp.isDisableFallbackToPrimary());
  }

  /**
   * Verify the older MigrationConfig snapshot (without dualHeadSyncPct) still deserializes correctly.
   * The missing dualHeadSyncPct field must default to 0.0.
   */
  @Test
  public void testMigrationConfigV1SnapshotDeserialization() throws Exception {
    MigrationConfig config = objectMapper.readValue(MIGRATION_CONFIG_V1_SNAPSHOT_JSON, MigrationConfig.class);

    assertFalse(config.isOverrideAccountMigrationConfig());
    assertEquals(50.0, config.getWriteRamp().getDualWriteAndDeleteAsyncPct(), 0.001);
    assertEquals(10.0, config.getReadRamp().getShadowReadMetadataPct(), 0.001);
    // dualHeadSyncPct was not in V1 — must default to 0.0
    assertEquals(0.0, config.getReadRamp().getDualHeadSyncPct(), 0.001);
  }

  /**
   * FIELD SET REGRESSION: Verify that serialized MigrationConfig contains exactly the expected fields.
   * If a new field is added to any ramp class, this test will fail, alerting the developer to:
   *   1. Update the expected field set
   *   2. Add a snapshot deserialization test for old JSON without the new field
   *   3. Ensure a safe default value when the field is absent
   */
  @Test
  public void testMigrationConfigFieldSetRegression() throws Exception {
    MigrationConfig config = new MigrationConfig(true,
        new MigrationConfig.WriteRamp(false, 50.0, 30.0, 20.0, true),
        new MigrationConfig.ReadRamp(false, 40.0, 60.0, 10.0, 5.0, true, 25.0),
        new MigrationConfig.ListRamp(false, 70.0, 80.0, true));

    String json = objectMapper.writeValueAsString(config);
    JsonNode root = objectMapper.readTree(json);

    Set<String> actualTopFields = fieldNames(root);
    assertEquals("MigrationConfig top-level fields changed. If you added a new field, update "
            + "EXPECTED_MIGRATION_CONFIG_FIELDS and add a snapshot deserialization test for old JSON without it.",
        EXPECTED_MIGRATION_CONFIG_FIELDS, actualTopFields);

    Set<String> actualWriteRampFields = fieldNames(root.get("writeRamp"));
    assertEquals("WriteRamp fields changed. Update EXPECTED_WRITE_RAMP_FIELDS and add a backwards compat test.",
        EXPECTED_WRITE_RAMP_FIELDS, actualWriteRampFields);

    Set<String> actualReadRampFields = fieldNames(root.get("readRamp"));
    assertEquals("ReadRamp fields changed. Update EXPECTED_READ_RAMP_FIELDS and add a backwards compat test.",
        EXPECTED_READ_RAMP_FIELDS, actualReadRampFields);

    Set<String> actualListRampFields = fieldNames(root.get("listRamp"));
    assertEquals("ListRamp fields changed. Update EXPECTED_LIST_RAMP_FIELDS and add a backwards compat test.",
        EXPECTED_LIST_RAMP_FIELDS, actualListRampFields);
  }

  // ==================== Account Tests ====================

  /**
   * Verify the current Account snapshot deserializes correctly with all field values preserved.
   */
  @Test
  public void testAccountSnapshotDeserialization() throws Exception {
    Account account = objectMapper.readValue(ACCOUNT_SNAPSHOT_JSON, Account.class);

    assertEquals(101, account.getId());
    assertEquals("TestAccount", account.getName());
    assertEquals(AccountStatus.ACTIVE, account.getStatus());
    assertFalse(account.isAclInheritedByContainer());
    assertEquals(1, account.getSnapshotVersion());
    assertTrue(account.isSecondaryEnabled());
    assertNotNull(account.getRampControl());
    assertTrue(account.getRampControl().isSecondaryEnabled());
    assertNotNull(account.getMigrationConfig());
    assertEquals(50.0,
        account.getMigrationConfig().getWriteRamp().getDualWriteAndDeleteAsyncPct(), 0.001);
  }

  /**
   * Verify the older Account snapshot (without rampControl, migrationConfig, migrationConfigs)
   * still deserializes correctly. These optional fields must default to null/false.
   */
  @Test
  public void testAccountV1SnapshotDeserialization() throws Exception {
    Account account = objectMapper.readValue(ACCOUNT_V1_SNAPSHOT_JSON, Account.class);

    assertEquals(101, account.getId());
    assertEquals("TestAccount", account.getName());
    assertEquals(AccountStatus.ACTIVE, account.getStatus());
    assertFalse(account.isAclInheritedByContainer());
    // Optional fields absent in V1 must default safely
    assertNull("rampControl should be null when absent from JSON", account.getRampControl());
    assertFalse("isSecondaryEnabled should be false when rampControl is absent",
        account.isSecondaryEnabled());
    assertNull("migrationConfig should be null when absent from JSON",
        account.getMigrationConfig());
    assertNull("migrationConfigs should be null when absent from JSON",
        account.getMigrationConfigs());
  }

  /**
   * FIELD SET REGRESSION: Verify that serialized Account JSON contains exactly the expected top-level fields.
   * If a new field is added to Account, this test will fail.
   */
  @Test
  public void testAccountFieldSetRegression() throws Exception {
    MigrationConfig migrationConfig = new MigrationConfig(false,
        new MigrationConfig.WriteRamp(), new MigrationConfig.ReadRamp(), new MigrationConfig.ListRamp());
    Account account = new AccountBuilder()
        .id((short) 1)
        .name("TestAccount")
        .status(AccountStatus.ACTIVE)
        .rampControl(new RampControl(false))
        .migrationConfig(migrationConfig)
        .migrationConfigs(null)
        .build();

    String json = objectMapper.writeValueAsString(account);
    JsonNode root = objectMapper.readTree(json);

    Set<String> actualFields = fieldNames(root);
    assertEquals("Account top-level fields changed. If you added a new field, update "
            + "EXPECTED_ACCOUNT_FIELDS, add it to AccountBuilder, and add a snapshot deserialization test "
            + "for old JSON without it.",
        EXPECTED_ACCOUNT_FIELDS, actualFields);
  }

  /**
   * FIELD SET REGRESSION for RampControl.
   */
  @Test
  public void testRampControlFieldSetRegression() throws Exception {
    RampControl rampControl = new RampControl(true);
    String json = objectMapper.writeValueAsString(rampControl);
    JsonNode root = objectMapper.readTree(json);

    Set<String> actualFields = fieldNames(root);
    assertEquals("RampControl fields changed. Update EXPECTED_RAMP_CONTROL_FIELDS and add a backwards compat test.",
        EXPECTED_RAMP_CONTROL_FIELDS, actualFields);
  }

  // ==================== Breakage Demos ====================
  // These tests simulate what happens when a newer Ambry version writes JSON with a field
  // that the current code doesn't know about, or when we add a field and forget backwards compat.

  /**
   * DEMO: A newer Ambry version added "dualDeleteSyncPct" to WriteRamp and serialized it to ZooKeeper.
   * Thanks to @JsonIgnoreProperties(ignoreUnknown = true) on MigrationConfig inner classes, older
   * servers can still deserialize this JSON — the unknown field is silently ignored.
   *
   * Without that annotation, this would throw UnrecognizedPropertyException, causing older servers
   * to crash when reading config written by newer servers during a rolling deploy.
   */
  @Test
  public void testWriteRampWithUnknownFutureFieldDeserializesSuccessfully() throws Exception {
    // JSON from a hypothetical future version that added "dualDeleteSyncPct" to WriteRamp
    String futureJson = "{"
        + "\"overrideAccountMigrationConfig\":false,"
        + "\"writeRamp\":{"
        + "\"forceDisableDualWriteAndDelete\":false,"
        + "\"dualWriteAndDeleteAsyncPct\":50.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,"
        + "\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false,"
        + "\"dualDeleteSyncPct\":75.0"
        + "},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":0.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}}";

    // Deserializes successfully — unknown field is silently ignored
    MigrationConfig config = objectMapper.readValue(futureJson, MigrationConfig.class);
    assertEquals(50.0, config.getWriteRamp().getDualWriteAndDeleteAsyncPct(), 0.001);
    assertFalse(config.getWriteRamp().isForceDisableDualWriteAndDelete());
  }

  /**
   * Verify that Account-level migrationConfigs (DC-keyed map) with unknown future fields in the
   * nested MigrationConfig values still deserializes successfully. This simulates a newer server
   * writing per-DC configs with a field the current code doesn't know about.
   */
  @Test
  public void testMigrationConfigsMapWithUnknownFutureFieldDeserializesSuccessfully() throws Exception {
    String accountJson = "{"
        + "\"accountId\":101,"
        + "\"accountName\":\"TestAccount\","
        + "\"status\":\"ACTIVE\","
        + "\"version\":1,"
        + "\"containers\":[],"
        + "\"migrationConfigs\":{"
        + "\"DC-1\":{"
        + "\"overrideAccountMigrationConfig\":false,"
        + "\"writeRamp\":{"
        + "\"forceDisableDualWriteAndDelete\":false,"
        + "\"dualWriteAndDeleteAsyncPct\":50.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,"
        + "\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false,"
        + "\"dualDeleteSyncPct\":75.0"
        + "},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":0.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}"
        + "},"
        + "\"DC-2\":{"
        + "\"overrideAccountMigrationConfig\":true,"
        + "\"writeRamp\":{\"forceDisableDualWriteAndDelete\":false,\"dualWriteAndDeleteAsyncPct\":0.0,"
        + "\"dualWriteAndDeleteSyncPctNonStrict\":0.0,\"dualWriteAndDeleteSyncPctStrict\":0.0,"
        + "\"writeAndDeleteOnlyToSecondary\":false},"
        + "\"readRamp\":{\"forceDisableReadFromSecondary\":false,\"shadowReadMetadataPct\":0.0,"
        + "\"shadowReadMd5Pct\":0.0,\"shadowReadContentPct\":0.0,\"serveReadFromSecondaryPct\":0.0,"
        + "\"disableFallbackToPrimary\":false,\"dualHeadSyncPct\":0.0},"
        + "\"listRamp\":{\"forceDisableListFromSecondary\":false,\"shadowListPct\":0.0,"
        + "\"serveListFromSecondaryPct\":0.0,\"disableFallbackToPrimary\":false}"
        + "}"
        + "}"
        + "}";

    // Deserializes successfully — unknown "dualDeleteSyncPct" in DC-1's writeRamp is silently ignored
    Account account = objectMapper.readValue(accountJson, Account.class);
    assertNotNull("migrationConfigs should not be null", account.getMigrationConfigs());
    assertEquals(2, account.getMigrationConfigs().size());

    MigrationConfig dc1Config = account.getMigrationConfigs().get("DC-1");
    assertNotNull("DC-1 config should be present", dc1Config);
    assertFalse(dc1Config.isOverrideAccountMigrationConfig());
    assertEquals(50.0, dc1Config.getWriteRamp().getDualWriteAndDeleteAsyncPct(), 0.001);

    MigrationConfig dc2Config = account.getMigrationConfigs().get("DC-2");
    assertNotNull("DC-2 config should be present", dc2Config);
    assertTrue(dc2Config.isOverrideAccountMigrationConfig());
  }

  /**
   * DEMO: If someone adds "dualDeleteSyncPct" to WriteRamp but doesn't update the expected field set,
   * the field set regression test catches it. This test simulates that by injecting the extra field
   * into serialized JSON and verifying it does NOT match the expected set.
   */
  @Test
  public void testFieldSetRegressionCatchesNewWriteRampField() throws Exception {
    MigrationConfig config = new MigrationConfig(false,
        new MigrationConfig.WriteRamp(false, 50.0, 0.0, 0.0, false),
        new MigrationConfig.ReadRamp(), new MigrationConfig.ListRamp());

    // Serialize, then inject a new field as if someone added it to the class
    String json = objectMapper.writeValueAsString(config);
    JsonNode root = objectMapper.readTree(json);
    ((com.fasterxml.jackson.databind.node.ObjectNode) root.get("writeRamp"))
        .put("dualDeleteSyncPct", 75.0);

    Set<String> actualWriteRampFields = fieldNames(root.get("writeRamp"));
    assertNotEquals("Regression test should detect the new field — expected set is stale",
        EXPECTED_WRITE_RAMP_FIELDS, actualWriteRampFields);
    assertTrue("New field should be present in actual but missing from expected",
        actualWriteRampFields.contains("dualDeleteSyncPct"));
    assertFalse("Expected set should NOT contain the new field yet",
        EXPECTED_WRITE_RAMP_FIELDS.contains("dualDeleteSyncPct"));
  }

  /**
   * DEMO: Same pattern for Account — a future version adds "replicationPolicy" at the account level.
   * The field set regression catches it.
   */
  @Test
  public void testFieldSetRegressionCatchesNewAccountField() throws Exception {
    Account account = new AccountBuilder()
        .id((short) 1)
        .name("TestAccount")
        .status(AccountStatus.ACTIVE)
        .build();

    String json = objectMapper.writeValueAsString(account);
    JsonNode root = objectMapper.readTree(json);
    ((com.fasterxml.jackson.databind.node.ObjectNode) root).put("replicationPolicy", "CROSS_DC");

    Set<String> actualFields = fieldNames(root);
    assertNotEquals("Regression test should detect the new account field",
        EXPECTED_ACCOUNT_FIELDS, actualFields);
    assertTrue("New field should be present in actual but missing from expected",
        actualFields.contains("replicationPolicy"));
    assertFalse("Expected set should NOT contain the new field yet",
        EXPECTED_ACCOUNT_FIELDS.contains("replicationPolicy"));
  }

  // ==================== Helpers ====================

  private static Set<String> fieldNames(JsonNode node) {
    assertNotNull("JSON node is null — the expected object may have been removed or renamed", node);
    Set<String> names = new TreeSet<>();
    Iterator<String> it = node.fieldNames();
    while (it.hasNext()) {
      names.add(it.next());
    }
    return names;
  }
}
