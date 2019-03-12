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
import com.github.ambry.utils.Utils;
import java.util.Objects;


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

  private String id;
  private String partitionId;
  private long creationTime;
  private long uploadTime;
  private long size;
  private int accountId;
  private int containerId;
  private long expirationTime;
  private long deletionTime;

  /**
   * Default constructor (for JSONSerializer).
   */
  public CloudBlobMetadata() {
  }

  /**
   * Constructor from {@link BlobId}.
   * @param blobId The BlobId for metadata record.
   * @param creationTime The blob creation time.
   * @param expirationTime The blob expiration time.
   * @param size The blob size.
   */
  public CloudBlobMetadata(BlobId blobId, long creationTime, long expirationTime, long size) {
    this.id = blobId.getID();
    this.partitionId = blobId.getPartition().toPathString();
    this.accountId = blobId.getAccountId();
    this.containerId = blobId.getContainerId();
    this.creationTime = creationTime;
    this.expirationTime = expirationTime;
    this.uploadTime = System.currentTimeMillis();
    this.deletionTime = Utils.Infinite_Time;
    this.size = size;
  }

  /**
   * @return the blob Id.
   */
  // Note: the field name and getter name must be this way to work with Azure CosmosDB.
  public String getId() {
    return id;
  }

  public CloudBlobMetadata setId(String id) {
    this.id = id;
    return this;
  }

  /**
   * @return the partition Id.
   */
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * Set the partition Id.
   * @param partitionId the partition Id of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setPartitionId(String partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  /**
   * @return the blob creation time.
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Set the creation time.
   * @param creationTime the creation time of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setCreationTime(long creationTime) {
    this.creationTime = creationTime;
    return this;
  }

  /**
   * @return the blob upload time.
   */
  public long getUploadTime() {
    return uploadTime;
  }

  /**
   * Set the upload time.
   * @param uploadTime the upload time of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setUploadTime(long uploadTime) {
    this.uploadTime = uploadTime;
    return this;
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
   * Set the size.
   * @param size the size of the blob in bytes.
   * @return this instance.
   */
  public CloudBlobMetadata setSize(long size) {
    this.size = size;
    return this;
  }

  /**
   * @return the account Id.
   */
  public int getAccountId() {
    return accountId;
  }

  /**
   * Set the account Id.
   * @param accountId the account Id of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setAccountId(int accountId) {
    this.accountId = accountId;
    return this;
  }

  /**
   * @return the container Id.
   */
  public int getContainerId() {
    return containerId;
  }

  /**
   * Set the container Id.
   * @param containerId the container Id of the blob.
   * @return this instance.
   */
  public CloudBlobMetadata setContainerId(int containerId) {
    this.containerId = containerId;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CloudBlobMetadata)) {
      return false;
    }
    CloudBlobMetadata om = (CloudBlobMetadata) o;
    return (Objects.equals(id, om.id) && Objects.equals(partitionId, om.partitionId) && creationTime == om.creationTime
        && uploadTime == om.uploadTime && expirationTime == om.expirationTime && deletionTime == om.deletionTime
        && size == om.size && accountId == om.accountId && containerId == om.containerId);
  }
}
