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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// Migration config applicable to accounts and containers used to migrate from one storage backend to another.
// Determines whether an op is sent to primary storage backend only, secondary storage backend only or both.
public class MigrationConfig {
  // Applicable only to container metadata and used to override account level config.
  public static final String OVERRIDE_ACCOUNT_MIGRATION_CONFIG = "overrideAccountMigrationConfig";
  @JsonProperty(OVERRIDE_ACCOUNT_MIGRATION_CONFIG)
  private final boolean overrideAccountMigrationConfig;

  // WriteRamp is applicable to "put" and "delete".
  public static final String WRITE_RAMP_KEY = "writeRamp";
  @JsonProperty(WRITE_RAMP_KEY)
  private WriteRamp writeRamp;

  // ReadRamp is applicable to "get" and "head".
  public static final String READ_RAMP_KEY = "readRamp";
  @JsonProperty(READ_RAMP_KEY)
  private ReadRamp readRamp;

  public static final String LIST_RAMP_KEY = "listRamp";
  @JsonProperty(LIST_RAMP_KEY)
  private ListRamp listRamp;

  // Write ramp config.
  public static class WriteRamp {
    @JsonProperty("forceDisableDualWriteAndDelete")
    private boolean forceDisableDualWriteAndDelete;

    @JsonProperty("dualWriteAndDeleteAsyncPct")
    private double dualWriteAndDeleteAsyncPct;

    @JsonProperty("dualWriteAndDeleteSyncPctNonStrict")
    private double dualWriteAndDeleteSyncPctNonStrict;

    @JsonProperty("dualWriteAndDeleteSyncPctStrict")
    private double dualWriteAndDeleteSyncPctStrict;

    @JsonProperty("writeAndDeleteOnlyToSecondary")
    private boolean writeAndDeleteOnlyToSecondary;

    @JsonCreator
    public WriteRamp(
        @JsonProperty("forceDisableDualWriteAndDelete") boolean forceDisableDualWriteAndDelete,
        @JsonProperty("dualWriteAndDeleteAsyncPct") double dualWriteAndDeleteAsyncPct,
        @JsonProperty("dualWriteAndDeleteSyncPctNonStrict") double dualWriteAndDeleteSyncPctNonStrict,
        @JsonProperty("dualWriteAndDeleteSyncPctStrict") double dualWriteAndDeleteSyncPctStrict,
        @JsonProperty("writeAndDeleteOnlyToSecondary") boolean writeAndDeleteOnlyToSecondary) {
      this.forceDisableDualWriteAndDelete = forceDisableDualWriteAndDelete;
      this.dualWriteAndDeleteAsyncPct = dualWriteAndDeleteAsyncPct;
      this.dualWriteAndDeleteSyncPctNonStrict = dualWriteAndDeleteSyncPctNonStrict;
      this.dualWriteAndDeleteSyncPctStrict = dualWriteAndDeleteSyncPctStrict;
      this.writeAndDeleteOnlyToSecondary = writeAndDeleteOnlyToSecondary;
    }

    // Default constructor for WriteRamp
    public WriteRamp() {
      this.forceDisableDualWriteAndDelete = false;
      this.dualWriteAndDeleteAsyncPct = 0.0;
      this.dualWriteAndDeleteSyncPctNonStrict = 0.0;
      this.dualWriteAndDeleteSyncPctStrict = 0.0;
      this.writeAndDeleteOnlyToSecondary = false;
    }

    // Getters
    public boolean isForceDisableDualWriteAndDelete() {
      return forceDisableDualWriteAndDelete;
    }

    public double getDualWriteAndDeleteAsyncPct() {
      return dualWriteAndDeleteAsyncPct;
    }

    public double getDualWriteAndDeleteSyncPctNonStrict() {
      return dualWriteAndDeleteSyncPctNonStrict;
    }

    public double getDualWriteAndDeleteSyncPctStrict() {
      return dualWriteAndDeleteSyncPctStrict;
    }

    public boolean isWriteAndDeleteOnlyToSecondary() {
      return writeAndDeleteOnlyToSecondary;
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WriteRamp that = (WriteRamp) o;
      return forceDisableDualWriteAndDelete == that.forceDisableDualWriteAndDelete &&
          Double.compare(that.dualWriteAndDeleteAsyncPct, dualWriteAndDeleteAsyncPct) == 0 &&
          Double.compare(that.dualWriteAndDeleteSyncPctNonStrict, dualWriteAndDeleteSyncPctNonStrict) == 0 &&
          Double.compare(that.dualWriteAndDeleteSyncPctStrict, dualWriteAndDeleteSyncPctStrict) == 0 &&
          writeAndDeleteOnlyToSecondary == that.writeAndDeleteOnlyToSecondary;
    }
  }

  // Read ramp config
  public static class ReadRamp {
    @JsonProperty("forceDisableReadFromSecondary")
    private boolean forceDisableReadFromSecondary;

    @JsonProperty("shadowReadMetadataPct")
    private double shadowReadMetadataPct;

    @JsonProperty("shadowReadMd5Pct")
    private double shadowReadMd5Pct;

    @JsonProperty("shadowReadContentPct")
    private double shadowReadContentPct;

    @JsonProperty("serveReadFromSecondaryPct")
    private double serveReadFromSecondaryPct;

    @JsonProperty("disableFallbackToPrimary")
    private boolean disableFallbackToPrimary;

    @JsonCreator
    public ReadRamp(
        @JsonProperty("forceDisableReadFromSecondary") boolean forceDisableReadFromSecondary,
        @JsonProperty("shadowReadMetadataPct") double shadowReadMetadataPct,
        @JsonProperty("shadowReadMd5Pct") double shadowReadMd5Pct,
        @JsonProperty("shadowReadContentPct") double shadowReadContentPct,
        @JsonProperty("serveReadFromSecondaryPct") double serveReadFromSecondaryPct,
        @JsonProperty("disableFallbackToPrimary") boolean disableFallbackToPrimary) {
      this.forceDisableReadFromSecondary = forceDisableReadFromSecondary;
      this.shadowReadMetadataPct = shadowReadMetadataPct;
      this.shadowReadMd5Pct = shadowReadMd5Pct;
      this.shadowReadContentPct = shadowReadContentPct;
      this.serveReadFromSecondaryPct = serveReadFromSecondaryPct;
      this.disableFallbackToPrimary = disableFallbackToPrimary;
    }

    // Default constructor for ReadRamp
    public ReadRamp() {
      this.forceDisableReadFromSecondary = false;
      this.shadowReadMetadataPct = 0.0;
      this.shadowReadMd5Pct = 0.0;
      this.shadowReadContentPct = 0.0;
      this.serveReadFromSecondaryPct = 0.0;
      this.disableFallbackToPrimary = false;
    }

    // Getters
    public boolean isForceDisableReadFromSecondary() {
      return forceDisableReadFromSecondary;
    }
    public double getShadowReadMetadataPct() {
      return shadowReadMetadataPct;
    }
    public double getShadowReadMd5Pct() {
      return shadowReadMd5Pct;
    }
    public double getShadowReadContentPct() {
      return shadowReadContentPct;
    }
    public double getServeReadFromSecondaryPct() {
      return serveReadFromSecondaryPct;
    }
    public boolean isDisableFallbackToPrimary() {
      return disableFallbackToPrimary;
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadRamp that = (ReadRamp) o;
      return forceDisableReadFromSecondary == that.forceDisableReadFromSecondary &&
          Double.compare(that.shadowReadMetadataPct, shadowReadMetadataPct) == 0 &&
          Double.compare(that.shadowReadMd5Pct, shadowReadMd5Pct) == 0 &&
          Double.compare(that.shadowReadContentPct, shadowReadContentPct) == 0 &&
          Double.compare(that.serveReadFromSecondaryPct, serveReadFromSecondaryPct) == 0 &&
          disableFallbackToPrimary == that.disableFallbackToPrimary;
    }
  }

  // List ramp config.
  public static class ListRamp {
    @JsonProperty("forceDisableListFromSecondary")
    private boolean forceDisableListFromSecondary;

    @JsonProperty("shadowListPct")
    private double shadowListPct;

    @JsonProperty("serveListFromSecondaryPct")
    private double serveListFromSecondaryPct;

    @JsonProperty("disableFallbackToPrimary")
    private boolean disableFallbackToPrimary;

    @JsonCreator
    public ListRamp(
        @JsonProperty("forceDisableListFromSecondary") boolean forceDisableListFromSecondary,
        @JsonProperty("shadowListPct") double shadowListPct,
        @JsonProperty("serveListFromSecondaryPct") double serveListFromSecondaryPct,
        @JsonProperty("disableFallbackToPrimary") boolean disableFallbackToPrimary) {
      this.forceDisableListFromSecondary = forceDisableListFromSecondary;
      this.shadowListPct = shadowListPct;
      this.serveListFromSecondaryPct = serveListFromSecondaryPct;
      this.disableFallbackToPrimary = disableFallbackToPrimary;
    }

    // Default constructor for ListRamp
    public ListRamp() {
      this.forceDisableListFromSecondary = false;
      this.shadowListPct = 0.0;
      this.serveListFromSecondaryPct = 0.0;
      this.disableFallbackToPrimary = false;
    }

    // Getters
    public boolean isForceDisableListFromSecondary() {
      return forceDisableListFromSecondary;
    }
    public double getShadowListPct() {
      return shadowListPct;
    }
    public double getServeListFromSecondaryPct() {
      return serveListFromSecondaryPct;
    }
    public boolean isDisableFallbackToPrimary() {
      return disableFallbackToPrimary;
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ListRamp that = (ListRamp) o;
      return forceDisableListFromSecondary == that.forceDisableListFromSecondary &&
          Double.compare(that.shadowListPct, shadowListPct) == 0 &&
          Double.compare(that.serveListFromSecondaryPct, serveListFromSecondaryPct) == 0 &&
          disableFallbackToPrimary == that.disableFallbackToPrimary;
    }
  }

  // Default migration config.
  public MigrationConfig() {
    this(false, new WriteRamp(), new ReadRamp(), new ListRamp());
  }

  @JsonCreator
  public MigrationConfig(
      @JsonProperty(OVERRIDE_ACCOUNT_MIGRATION_CONFIG) boolean overrideAccountMigrationConfig,
      @JsonProperty(WRITE_RAMP_KEY) WriteRamp writeRamp,
      @JsonProperty(READ_RAMP_KEY) ReadRamp readRamp,
      @JsonProperty(LIST_RAMP_KEY) ListRamp listRamp) {
    this.overrideAccountMigrationConfig = overrideAccountMigrationConfig;
    this.writeRamp = writeRamp;
    this.readRamp = readRamp;
    this.listRamp = listRamp;
  }

  public boolean isOverrideAccountMigrationConfig() {
    return overrideAccountMigrationConfig;
  }

  public WriteRamp getWriteRamp() {
    return writeRamp;
  }

  public ReadRamp getReadRamp() {
    return readRamp;
  }

  public ListRamp getListRamp() {
    return listRamp;
  }
}
