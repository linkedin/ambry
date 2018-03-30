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
    DELETE
  }

  private final Type type;
  private final DeleteRecord deleteRecord;
  private final short accountId;
  private final short containerId;
  private final long updateTimeInMs;

  /**
   * @param accountId the account that the blob that this update is associated with belongs to
   * @param containerId the id of the container that the blob that this update is associated with belongs to
   * @param updateTimeInMs the time in ms at which the update occurred.
   * @param deleteRecord the delete record that this update record represents.
   */
  UpdateRecord(short accountId, short containerId, long updateTimeInMs, DeleteRecord deleteRecord) {
    this(accountId, containerId, updateTimeInMs, Type.DELETE, deleteRecord);
  }

  /**
   * @param type the type of the update record.
   * @param deleteRecord the delete record that this update record represents.
   */
  private UpdateRecord(short accountId, short containerId, long updateTimeInMs, Type type, DeleteRecord deleteRecord) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.updateTimeInMs = updateTimeInMs;
    this.type = type;
    this.deleteRecord = deleteRecord;
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
  public DeleteRecord getDeleteRecord() {
    return deleteRecord;
  }
}
