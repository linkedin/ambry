/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.named;

import java.util.Objects;

public class PartiallyReadableBlobRecord {
  private final String accountName;
  private final String containerName;
  private final String blobName;
  private final String chunkId;
  private final long chunkOffset;
  private final long chunkSize;
  private final long lastUpdatedTs;
  private final String status;

  /**
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name within the container.
   * @param chunkId the chunk ID of the blob content in ambry storage.
   * @param chunkOffset the offset of the chunk in the larger composite blob.
   * @param chunkSize the size of the chunk data.
   * @param lastUpdatedTs the timestamp the record was last written to the db.
   * @param status status of the blob uploading process.
   */
  public PartiallyReadableBlobRecord(String accountName, String containerName, String blobName, String chunkId,
      long chunkOffset, long chunkSize, long lastUpdatedTs, String status) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
    this.chunkId = chunkId;
    this.chunkOffset = chunkOffset;
    this.chunkSize = chunkSize;
    this.lastUpdatedTs = lastUpdatedTs;
    this.status = status;
  }

  public String getAccountName() {
    return accountName;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getBlobName() {
    return blobName;
  }

  public String getChunkId() {
    return chunkId;
  }

  public long getChunkOffset() {
    return chunkOffset;
  }

  public long getChunkSize() {
    return chunkSize;
  }

  public long getLastUpdatedTs() {
    return lastUpdatedTs;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartiallyReadableBlobRecord record = (PartiallyReadableBlobRecord) o;
    return lastUpdatedTs == record.getLastUpdatedTs() && chunkOffset == record.getChunkOffset() && chunkSize == record.getChunkSize()
        && Objects.equals(accountName, record.getAccountName()) && Objects.equals(containerName, record.getContainerName())
        && Objects.equals(blobName, record.getBlobName()) && Objects.equals(chunkId, record.getChunkId())
        && Objects.equals(status, record.getStatus());
  }
}