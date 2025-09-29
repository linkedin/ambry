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

public class RampControl {
  // Global property to define whether secondary is enabled or not.
  // If secondary is disabled then everything else is ignored.
  public static final String SECONDARY_ENABLED_KEY = "secondaryEnabled";
  @JsonProperty(SECONDARY_ENABLED_KEY)
  private final boolean secondaryEnabled;

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
  public RampControl(@JsonProperty(SECONDARY_ENABLED_KEY) boolean secondaryEnabled,
                     @JsonProperty(WRITE_RAMP_KEY) WriteRamp writeRamp,
                     @JsonProperty(READ_RAMP_KEY) ReadRamp readRamp,
                     @JsonProperty(LIST_RAMP_KEY) ListRamp listRamp) {
    this.secondaryEnabled = secondaryEnabled;
    this.writeRamp = writeRamp;
    this.readRamp = readRamp;
    this.listRamp = listRamp;
  }

  public boolean isSecondaryEnabled() {
    return secondaryEnabled;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RampControl that = (RampControl) o;
    return secondaryEnabled == that.secondaryEnabled;
  }

  @Override
  public int hashCode() {
    return Boolean.hashCode(secondaryEnabled);
  }
}
