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

import com.github.ambry.utils.Utils;


/**
 * Contains the delete record info
 */
public class DeleteRecord {

  public static final short ACCOUNTID_CONTAINERID_DEFAULT_VALUE = -1;
  private short version;
  private boolean isDeleted;
  private short accountId = ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
  private short containerId = ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
  private int deletionTimeInSecs = (int) Utils.Infinite_Time;

  DeleteRecord(boolean isDeleted) {
    this.isDeleted = isDeleted;
    this.version = MessageFormatRecord.Delete_Version_V1;
  }

  DeleteRecord(short accountId, short containerId, int deletionTimeInSecs) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.deletionTimeInSecs = deletionTimeInSecs;
    this.version = MessageFormatRecord.Delete_Version_V2;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  public int getDeletionTimeInSecs() {
    return deletionTimeInSecs;
  }

  public short getVersion() {
    return version;
  }
}
