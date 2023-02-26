/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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


/**
 * Represents a metadata record in a {@link NamedBlobDb} implementation.
 */
public class NamedBlobRecord {
  private final String accountName;
  private final String containerName;
  private final String blobName;
  private final String blobId;
  private final long expirationTimeMs;
  private long version;

  /**
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name within the container.
   * @param blobId the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs) {
    this(accountName, containerName, blobName, blobId, expirationTimeMs, 0);
  }

  /**
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name within the container.
   * @param blobId the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version the version of this named blob.
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
    this.blobId = blobId;
    this.expirationTimeMs = expirationTimeMs;
    this.version = version;
  }

  /**
   * @return the account name.
   */
  public String getAccountName() {
    return accountName;
  }

  /**
   * @return the container name.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * @return the blob name within the container.
   */
  public String getBlobName() {
    return blobName;
  }

  /**
   * @return the blob ID for the blob content in ambry storage.
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return the version for the named blob map.
   */
  public long getVersion() {
    return version;
  }

  public void setVersion(long newVersion) {
    version = newVersion;
  }

  /**
   * @return the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  public long getExpirationTimeMs() {
    return expirationTimeMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamedBlobRecord record = (NamedBlobRecord) o;
    return expirationTimeMs == record.expirationTimeMs && Objects.equals(accountName, record.accountName)
        && Objects.equals(containerName, record.containerName) && Objects.equals(blobName, record.blobName)
        && Objects.equals(blobId, record.blobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobId);
  }

  @Override
  public String toString() {
    return "NamedBlobRecord[accountName=" + accountName + ",containerName=" + containerName + ",blobName=" + blobName +
        ",blobId=" + blobId + ",expirationTimeMs=" + expirationTimeMs + ",version=" + version + "]";
  }
}
