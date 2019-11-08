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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
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
  public static final String FIELD_ENCRYPTION_ORIGIN = "encryptionOrigin";
  public static final String FIELD_VCR_KMS_CONTEXT = "vcrKmsContext";
  public static final String FIELD_CRYPTO_AGENT_FACTORY = "cryptoAgentFactory";
  public static final String FIELD_CLOUD_BLOB_NAME = "cloudBlobName";

  private String id;
  private String partitionId;
  private long creationTime;
  private long uploadTime;
  private long size;
  private int accountId;
  private int containerId;
  private long expirationTime;
  private long deletionTime;
  private EncryptionOrigin encryptionOrigin;
  private String vcrKmsContext;
  private String cryptoAgentFactory;
  private String cloudBlobName;
  private long encryptedSize;
  // this field is derived from the system generated last Update Time in the cloud db
  // and hence shouldn't be serializable.
  @JsonIgnore
  private long lastUpdateTime;

  /**
   * Possible values of encryption origin for cloud stored blobs.
   * Only considers encryption initiated by Ambry.
   */
  public enum EncryptionOrigin {

    /** Not encrypted by Ambry */
    NONE,
    /** Encrypted by Router */
    ROUTER,
    /** Encrypted by VCR */
    VCR
  }

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
   * @param encryptionOrigin The blob's encryption origin.
   */
  public CloudBlobMetadata(BlobId blobId, long creationTime, long expirationTime, long size,
      EncryptionOrigin encryptionOrigin) {
    this(blobId, creationTime, expirationTime, size, encryptionOrigin, null, null, -1);
  }

  /**
   * Constructor from {@link BlobId}.
   * @param blobId The BlobId for metadata record.
   * @param creationTime The blob creation time.
   * @param expirationTime The blob expiration time.
   * @param size The blob size.
   * @param encryptionOrigin The blob's encryption origin.
   * @param vcrKmsContext The KMS context used to encrypt the blob.  Only used when encryptionOrigin = VCR.
   * @param cryptoAgentFactory The class name of the {@link CloudBlobCryptoAgentFactory} used to encrypt the blob.
   *                         Only used when encryptionOrigin = VCR.
   * @param encryptedSize The size of the uploaded blob if it was encrypted and then uploaded.
   *                      Only used when encryptionOrigin = VCR.
   */
  public CloudBlobMetadata(BlobId blobId, long creationTime, long expirationTime, long size,
      EncryptionOrigin encryptionOrigin, String vcrKmsContext, String cryptoAgentFactory, long encryptedSize) {
    this.id = blobId.getID();
    this.partitionId = blobId.getPartition().toPathString();
    this.accountId = blobId.getAccountId();
    this.containerId = blobId.getContainerId();
    this.creationTime = creationTime;
    this.expirationTime = expirationTime;
    this.uploadTime = System.currentTimeMillis();
    this.deletionTime = Utils.Infinite_Time;
    this.size = size;
    this.encryptionOrigin = encryptionOrigin;
    this.vcrKmsContext = vcrKmsContext;
    this.cryptoAgentFactory = cryptoAgentFactory;
    this.cloudBlobName = blobId.getID();
    this.encryptedSize = encryptedSize;
    this.lastUpdateTime = System.currentTimeMillis();
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

  /**
   * @return the {@link EncryptionOrigin}.
   */
  public EncryptionOrigin getEncryptionOrigin() {
    return encryptionOrigin;
  }

  /**
   * Sets the encryption origin.
   * @param encryptionOrigin the {@link EncryptionOrigin}.
   * @return this instance.
   */
  public CloudBlobMetadata setEncryptionOrigin(EncryptionOrigin encryptionOrigin) {
    this.encryptionOrigin = encryptionOrigin;
    return this;
  }

  /**
   * @return the VCR KMS context.
   */
  public String getVcrKmsContext() {
    return vcrKmsContext;
  }

  /**
   * Sets the VCR KMS context.
   * @param vcrKmsContext the KMS context used for encryption.
   * @return this instance.
   */
  public CloudBlobMetadata setVcrKmsContext(String vcrKmsContext) {
    this.vcrKmsContext = vcrKmsContext;
    return this;
  }

  /**
   * @return the blob's name in cloud.
   */
  public String getCloudBlobName() {
    return cloudBlobName;
  }

  /**
   * Sets blob's name in cloud.
   * @param cloudBlobName the blob's name in cloud.
   * @return this instance.
   */
  public CloudBlobMetadata setCloudBlobName(String cloudBlobName) {
    this.cloudBlobName = cloudBlobName;
    return this;
  }

  /**
   * @return the VCR crypto agent factory class name.
   */
  public String getCryptoAgentFactory() {
    return cryptoAgentFactory;
  }

  /**
   * Sets the VCR crypto agent factory class name.
   * @param cryptoAgentFactory the class name of the {@link CloudBlobCryptoAgentFactory} used for encryption.
   * @return this instance.
   */
  public CloudBlobMetadata setCryptoAgentFactory(String cryptoAgentFactory) {
    this.cryptoAgentFactory = cryptoAgentFactory;
    return this;
  }

  /**
   * @return the encrypted size of the blob if the blob was encrypted and uploaded to cloud, -1 otherwise
   */
  public long getEncryptedSize() {
    return encryptedSize;
  }

  /**
   * Sets the encrypted size of the blob
   * @param encryptedSize
   */
  public CloudBlobMetadata setEncryptedSize(long encryptedSize) {
    this.encryptedSize = encryptedSize;
    return this;
  }

  /**
   * @return the last update time of the blob.
   */
  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  /**
   * Sets the last update time of the blob.
   * @param lastUpdateTime last update time.
   */
  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  /**
   * Utility to cap specified {@link CloudBlobMetadata} list by specified size of its blobs.
   * Always returns at least one metadata object irrespective of size.
   * @param originalList List of {@link CloudBlobMetadata}.
   * @param size total size of metadata's blobs.
   * @return {@link List} of {@link CloudBlobMetadata} capped by size.
   */
  public static List<CloudBlobMetadata> capMetadataListBySize(List<CloudBlobMetadata> originalList, long size) {
    long totalSize = 0;
    List<CloudBlobMetadata> cappedList = new ArrayList<>();
    for (CloudBlobMetadata metadata : originalList) {
      // Cap results at max size
      if (totalSize + metadata.getSize() > size) {
        if (cappedList.isEmpty()) {
          // We must add at least one regardless of size
          cappedList.add(metadata);
        }
        break;
      }
      cappedList.add(metadata);
      totalSize += metadata.getSize();
    }
    return cappedList;
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
