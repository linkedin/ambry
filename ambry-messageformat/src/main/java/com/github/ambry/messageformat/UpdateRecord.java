/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

/**
 * In mem representation of an update record header
 */
public class UpdateRecord {

  private final short accountId;
  private final short containerId;
  private final long updateTimeInMs;
  private final SubRecord subRecord;

  /**
   * @param accountId the account that the blob that this update is associated with belongs to
   * @param containerId the id of the container that the blob that this update is associated with belongs to
   * @param updateTimeInMs the time in ms at which the update occurred.
   * @param subRecord the subRecord that this update record represents.
   */
  UpdateRecord(short accountId, short containerId, long updateTimeInMs, SubRecord subRecord) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.updateTimeInMs = updateTimeInMs;
    this.subRecord = subRecord;
  }

  /**
   * @return the type of the update record.
   */
  public SubRecord.Type getType() {
    return subRecord.getType();
  }

  /**
   * @return the id of the account that the blob that this update is associated with belongs to
   */
  public short getAccountId() {
    return accountId;
  }

  /**
   * @return the id of the container that the blob that this update is associated with belongs to
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * @return the time in ms at which the update occurred.
   */
  public long getUpdateTimeInMs() {
    return updateTimeInMs;
  }

  /**
   * @return the delete record if type is {@link SubRecord.Type#DELETE}. {@code null} otherwise.
   */
  public DeleteSubRecord getDeleteSubRecord() {
    if (subRecord.getType().equals(SubRecord.Type.DELETE)) {
      return (DeleteSubRecord) subRecord;
    }
    return null;
  }

  /**
   * @return the ttl update record if type is {@link SubRecord.Type#TTL_UPDATE}. {@code null} otherwise.
   */
  public TtlUpdateSubRecord getTtlUpdateSubRecord() {
    if (subRecord.getType().equals(SubRecord.Type.TTL_UPDATE)) {
      return (TtlUpdateSubRecord) subRecord;
    }
    return null;
  }

  /**
   * @return the undelete record if type is {@link SubRecord.Type#UNDELETE}. {@code null} otherwise.
   */
  public UndeleteSubRecord getUndeleteSubRecord() {
    if (subRecord.getType().equals(SubRecord.Type.UNDELETE)) {
      return (UndeleteSubRecord) subRecord;
    }
    return null;
  }
}
