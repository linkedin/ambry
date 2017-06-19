/**
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
package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Utils;


/**
 * Contains the delete record info
 */
public class DeleteRecord {
  private final short version;
  private final short accountId;
  private final short containerId;
  private final long deletionTimeInMs;

  /**
   * Constructs Delete Record in {@link MessageFormatRecord#Delete_Version_V1}
   */
  DeleteRecord() {
    accountId = MessageInfo.ACCOUNT_ID_LEGACY_VALUE;
    containerId = MessageInfo.CONTAINER_ID_LEGACY_VALUE;
    deletionTimeInMs = Utils.Infinite_Time;
    this.version = MessageFormatRecord.Delete_Version_V1;
  }

  /**
   * Constructs Delete Record in {@link MessageFormatRecord#Delete_Version_V2}
   */
  DeleteRecord(short accountId, short containerId, long deletionTimeInMs) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.deletionTimeInMs = deletionTimeInMs;
    this.version = MessageFormatRecord.Delete_Version_V2;
  }

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  public long getDeletionTimeInMs() {
    return deletionTimeInMs;
  }

  public short getVersion() {
    return version;
  }
}
