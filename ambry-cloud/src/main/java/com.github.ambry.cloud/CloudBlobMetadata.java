/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.commons.BlobId;


/**
 * Blob metadata document POJO class.
 */
public class CloudBlobMetadata {
  public static final String FIELD_CREATION_TIME = "creationTime";
  public static final String FIELD_UPLOAD_TIME = "uploadTime";
  public static final String FIELD_DELETION_TIME = "deletionTime";
  public static final String FIELD_EXPIRATION_TIME = "expirationTime";
  public static final String FIELD_ACCOUNT_ID = "accountId";
  public static final String FIELD_CONTAINER_ID = "containerId";

  private final String id;
  private final String partitionId;
  private final long creationTime;
  private final long uploadTime;
  private final long size;
  private final int accountId;
  private final int containerId;
  private long expirationTime;
  private long deletionTime;

  /**
   * Constructor from {@link BlobId}.
   * @param blobId The BlobId for metadata record.
   * @param creationTime The blob creation time.
   * @param size The blob size.
   */
  public CloudBlobMetadata(BlobId blobId, long creationTime, long size) {
    this.id = blobId.getID();
    this.partitionId = blobId.getPartition().toPathString();
    this.accountId = blobId.getAccountId();
    this.containerId = blobId.getContainerId();
    this.creationTime = creationTime;
    this.uploadTime = System.currentTimeMillis();
    this.size = size;
  }

  /**
   * @return the blob Id.
   */
  // Note: the field name and getter name must be this way to work with Azure CosmosDB.
  public String getId() {
    return id;
  }

  /**
   * @return the partition Id.
   */
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * @return the blob creation time.
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @return the blob upload time.
   */
  public long getUploadTime() {
    return uploadTime;
  }

  /**
   * @return the blob expiration time.
   */
  public long getExpirationTime() {
    return expirationTime;
  }

  /**
   * Set the blob expiration time.
   * @param expirationTime the expiration time of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
    return this;
  }

  /**
   * @return the blob deletion time.
   */
  public long getDeletionTime() {
    return deletionTime;
  }

  /**
   * Set the blob deletion time.
   * @param deletionTime the deletion time of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setDeletionTime(long deletionTime) {
    this.deletionTime = deletionTime;
    return this;
  }

  /**
   * @return the blob size.
   */
  public long getSize() {
    return size;
  }

  /**
   * @return the account Id.
   */
  public int getAccountId() {
    return accountId;
  }

  /**
   * @return the container Id.
   */
  public int getContainerId() {
    return containerId;
  }
}
