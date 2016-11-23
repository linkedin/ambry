/**
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
package com.github.ambry.store;

import com.github.ambry.commons.BlobId;


/**
 * Holds information about a blob record in the log
 */
class LogBlobRecordInfo {
  final String messageHeader;
  final BlobId blobId;
  final String blobProperty;
  final String userMetadata;
  final String blobDataOutput;
  final String deleteMsg;
  final boolean isDeleted;
  final boolean isExpired;
  final long timeToLiveInSeconds;
  final int totalRecordSize;

  LogBlobRecordInfo(String messageHeader, BlobId blobId, String blobProperty, String userMetadata,
      String blobDataOutput, String deleteMsg, boolean isDeleted, boolean isExpired, long timeToLiveInSeconds,
      int totalRecordSize) {
    this.messageHeader = messageHeader;
    this.blobId = blobId;
    this.blobProperty = blobProperty;
    this.userMetadata = userMetadata;
    this.blobDataOutput = blobDataOutput;
    this.deleteMsg = deleteMsg;
    this.isDeleted = isDeleted;
    this.isExpired = isExpired;
    this.timeToLiveInSeconds = timeToLiveInSeconds;
    this.totalRecordSize = totalRecordSize;
  }
}
