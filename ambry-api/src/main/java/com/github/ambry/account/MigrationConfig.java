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

    @JsonProperty("writeAndDeleteOnlyToCaspian")
    private boolean writeAndDeleteOnlyToCaspian;

    @JsonCreator
    public WriteRamp(
        @JsonProperty("forceDisableDualWriteAndDelete") boolean forceDisableDualWriteAndDelete,
        @JsonProperty("dualWriteAndDeleteAsyncPct") double dualWriteAndDeleteAsyncPct,
        @JsonProperty("dualWriteAndDeleteSyncPctNonStrict") double dualWriteAndDeleteSyncPctNonStrict,
        @JsonProperty("dualWriteAndDeleteSyncPctStrict") double dualWriteAndDeleteSyncPctStrict,
        @JsonProperty("writeAndDeleteOnlyToCaspian") boolean writeAndDeleteOnlyToCaspian) {
      this.forceDisableDualWriteAndDelete = forceDisableDualWriteAndDelete;
      this.dualWriteAndDeleteAsyncPct = dualWriteAndDeleteAsyncPct;
      this.dualWriteAndDeleteSyncPctNonStrict = dualWriteAndDeleteSyncPctNonStrict;
      this.dualWriteAndDeleteSyncPctStrict = dualWriteAndDeleteSyncPctStrict;
      this.writeAndDeleteOnlyToCaspian = writeAndDeleteOnlyToCaspian;
    }
  }

  // Read ramp config
  public static class ReadRamp {
    @JsonProperty("forceDisableReadFromCaspian")
    private boolean forceDisableReadFromCaspian;

    @JsonProperty("shadowReadMetadataPct")
    private double shadowReadMetadataPct;

    @JsonProperty("shadowReadMd5Pct")
    private double shadowReadMd5Pct;

    @JsonProperty("shadowReadContentPct")
    private double shadowReadContentPct;

    @JsonProperty("ambryReadFromCaspianPct")
    private double ambryReadFromCaspianPct;

    @JsonProperty("disableFallbackReadFromAmbry")
    private boolean disableFallbackReadFromAmbry;

    @JsonCreator
    public ReadRamp(
        @JsonProperty("forceDisableReadFromCaspian") boolean forceDisableReadFromCaspian,
        @JsonProperty("shadowReadMetadataPct") double shadowReadMetadataPct,
        @JsonProperty("shadowReadMd5Pct") double shadowReadMd5Pct,
        @JsonProperty("shadowReadContentPct") double shadowReadContentPct,
        @JsonProperty("ambryReadFromCaspianPct") double ambryReadFromCaspianPct,
        @JsonProperty("disableFallbackReadFromAmbry") boolean disableFallbackReadFromAmbry) {
      this.forceDisableReadFromCaspian = forceDisableReadFromCaspian;
      this.shadowReadMetadataPct = shadowReadMetadataPct;
      this.shadowReadMd5Pct = shadowReadMd5Pct;
      this.shadowReadContentPct = shadowReadContentPct;
      this.ambryReadFromCaspianPct = ambryReadFromCaspianPct;
      this.disableFallbackReadFromAmbry = disableFallbackReadFromAmbry;
    }
  }

  // List ramp config.
  public static class ListRamp {
    @JsonProperty("forceDisableListFromCaspian")
    private boolean forceDisableListFromCaspian;

    @JsonProperty("shadowListPct")
    private double shadowListPct;

    @JsonProperty("ambryListFromCaspianPct")
    private double ambryListFromCaspianPct;

    @JsonProperty("disableFallbackListFromAmbry")
    private boolean disableFallbackListFromAmbry;

    @JsonCreator
    public ListRamp(
        @JsonProperty("forceDisableListFromCaspian") boolean forceDisableListFromCaspian,
        @JsonProperty("shadowListPct") double shadowListPct,
        @JsonProperty("ambryListFromCaspianPct") double ambryListFromCaspianPct,
        @JsonProperty("disableFallbackListFromAmbry") boolean disableFallbackListFromAmbry) {
      this.forceDisableListFromCaspian = forceDisableListFromCaspian;
      this.shadowListPct = shadowListPct;
      this.ambryListFromCaspianPct = ambryListFromCaspianPct;
      this.disableFallbackListFromAmbry = disableFallbackListFromAmbry;
    }
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
