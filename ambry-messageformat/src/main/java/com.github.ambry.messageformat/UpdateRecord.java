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
  public enum Type {
    DELETE, TTL_UPDATE
  }

  private final Type type;
  private final short accountId;
  private final short containerId;
  private final long updateTimeInMs;
  private final DeleteSubRecord deleteSubRecord;
  private final TtlUpdateSubRecord ttlUpdateSubRecord;

  /**
   * @param accountId the account that the blob that this update is associated with belongs to
   * @param containerId the id of the container that the blob that this update is associated with belongs to
   * @param updateTimeInMs the time in ms at which the update occurred.
   * @param deleteSubRecord the delete record that this update record represents.
   */
  UpdateRecord(short accountId, short containerId, long updateTimeInMs, DeleteSubRecord deleteSubRecord) {
    this(accountId, containerId, updateTimeInMs, Type.DELETE, deleteSubRecord, null);
  }

  /**
   * @param accountId the account that the blob that this update is associated with belongs to
   * @param containerId the id of the container that the blob that this update is associated with belongs to
   * @param updateTimeInMs the time in ms at which the update occurred.
   * @param ttlUpdateSubRecord the ttl update record that this update record represents.
   */
  UpdateRecord(short accountId, short containerId, long updateTimeInMs, TtlUpdateSubRecord ttlUpdateSubRecord) {
    this(accountId, containerId, updateTimeInMs, Type.TTL_UPDATE, null, ttlUpdateSubRecord);
  }

  /**
   * @param accountId the account that the blob that this update is associated with belongs to
   * @param containerId the id of the container that the blob that this update is associated with belongs to
   * @param updateTimeInMs the time in ms at which the update occurred.
   * @param type the type of the update record.
   * @param deleteSubRecord the delete record that this update record represents.
   * @param ttlUpdateSubRecord the ttl update record that this update record represents.
   */
  private UpdateRecord(short accountId, short containerId, long updateTimeInMs, Type type,
      DeleteSubRecord deleteSubRecord, TtlUpdateSubRecord ttlUpdateSubRecord) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.updateTimeInMs = updateTimeInMs;
    this.type = type;
    this.deleteSubRecord = deleteSubRecord;
    this.ttlUpdateSubRecord = ttlUpdateSubRecord;
  }

  /**
   * @return the type of the update record.
   */
  public Type getType() {
    return type;
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
   * @return the delete record if type is {@link Type#DELETE}. {@code null} otherwise.
   */
  public DeleteSubRecord getDeleteSubRecord() {
    return deleteSubRecord;
  }

  /**
   * @return the ttl update record if type is {@link Type#TTL_UPDATE}. {@code null} otherwise.
   */
  public TtlUpdateSubRecord getTtlUpdateSubRecord() {
    return ttlUpdateSubRecord;
  }
}
