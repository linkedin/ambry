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

import com.github.ambry.utils.Utils;
import java.util.Objects;


/**
 * Represents a metadata record in a {@link NamedBlobDb} implementation.
 */
public class NamedBlobRecord {
  public static final long UNINITIALIZED_VERSION = 0L;
  private final String accountName;
  private final String containerName;
  private final String blobName;
  private final long expirationTimeMs;
  private final long version;
  private final String blobId;
  private final long blobSize;
  private long modifiedTimeMs;
  private final boolean isDirectory;
  private final String digest;

  /**
   * A helper method to create a {@link NamedBlobRecord} for Put operation.
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param blobSize         the size of the blob.
   * @return
   */
  public static NamedBlobRecord forPut(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long blobSize) {
    return new NamedBlobRecord(accountName, containerName, blobName, blobId, expirationTimeMs, UNINITIALIZED_VERSION,
        blobSize, 0, false);
  }

  /**
   * A helper method to create a {@link NamedBlobRecord} for Put operation.
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param blobSize         the size of the blob.
   * @param digest           the digest of the blob content, can be null if not set
   * @return
   */
  public static NamedBlobRecord forPut(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long blobSize, String digest) {
    return new NamedBlobRecord(accountName, containerName, blobName, blobId, expirationTimeMs, UNINITIALIZED_VERSION,
        blobSize, 0, false, digest);
  }

  /**
   * A helper method to create a {@link NamedBlobRecord} for ttl update operation.
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param version          the version of this named blob.
   * @return
   */
  public static NamedBlobRecord forUpdate(String accountName, String containerName, String blobName, long version) {
    return new NamedBlobRecord(accountName, containerName, blobName, null, Utils.Infinite_Time, version, 0, 0, false);
  }

  /**
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name within the container.
   * @param blobId the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs) {
    this(accountName, containerName, blobName, blobId, expirationTimeMs, UNINITIALIZED_VERSION);
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
    this(accountName, containerName, blobName, blobId, expirationTimeMs, version, 0);
  }

  /**
   * @param accountName the account name.
   * @param containerName the container name.
   * @param blobName the blob name within the container.
   * @param blobId the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version the version of this named blob.
   * @param digest the digest of this blob
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version, String digest) {
    this(accountName, containerName, blobName, blobId, expirationTimeMs, version, 0, digest);
  }

  /**
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version          the version of this named blob.
   * @param blobSize         the size of the blob.
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version, long blobSize) {
    this(accountName, containerName, blobName, blobId, expirationTimeMs, version, blobSize, 0, false);
  }

  /**
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version          the version of this named blob.
   * @param blobSize         the size of the blob.
   * @param digest           the digest of the blob content, can be null if not set
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version, long blobSize, String digest) {
    this(accountName, containerName, blobName, blobId, expirationTimeMs, version, blobSize, 0, false, digest);
  }

  /**
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version          the version of this named blob.
   * @param blobSize         the size of the blob.
   * @param modifiedTimeMs   the modified time of the blob in milliseconds since epoch
   * @param isDirectory      whether the blob is a directory (virtual folder name separated by '/')
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version, long blobSize, long modifiedTimeMs, boolean isDirectory) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
    this.blobId = blobId;
    this.expirationTimeMs = expirationTimeMs;
    this.version = version;
    this.blobSize = blobSize;
    this.modifiedTimeMs = modifiedTimeMs;
    this.isDirectory = isDirectory;
    this.digest = null;
  }

  /**
   * @param accountName      the account name.
   * @param containerName    the container name.
   * @param blobName         the blob name within the container.
   * @param blobId           the blob ID for the blob content in ambry storage.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param version          the version of this named blob.
   * @param blobSize         the size of the blob.
   * @param modifiedTimeMs   the modified time of the blob in milliseconds since epoch
   * @param isDirectory      whether the blob is a directory (virtual folder name separated by '/')
   * @param digest           the digest of the blob content, can be null if not set
   */
  public NamedBlobRecord(String accountName, String containerName, String blobName, String blobId,
      long expirationTimeMs, long version, long blobSize, long modifiedTimeMs, boolean isDirectory, String digest) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
    this.blobId = blobId;
    this.expirationTimeMs = expirationTimeMs;
    this.version = version;
    this.blobSize = blobSize;
    this.modifiedTimeMs = modifiedTimeMs;
    this.isDirectory = isDirectory;
    this.digest = digest;
  }

  /**
   * @return the digest of the blob content, can be null if not set
   */
  public String getDigest() {
    return digest;
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

  /**
   * @return the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  public long getExpirationTimeMs() {
    return expirationTimeMs;
  }

  /**
   * @return the blob size.
   */
  public long getBlobSize() {
    return blobSize;
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
        && Objects.equals(blobId, record.blobId) && Objects.equals(digest, record.getDigest());
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobId);
  }

  @Override
  public String toString() {
    return "NamedBlobRecord[accountName=" + accountName + ",containerName=" + containerName + ",blobName=" + blobName
        + ",blobId=" + blobId + ",expirationTimeMs=" + expirationTimeMs + ",version=" + version + ",digest=" + digest + "]";
  }

  /**
   * @return the modified timestamp of this blob
   */
  public long getModifiedTimeMs() {
    return modifiedTimeMs;
  }

  /**
   * Set the modified timestamp of this blob. Exposed for testing
   * @param modifiedTimeMs the modified timestamp to set in milliseconds since epoch
   */
  public void setModifiedTimeMs(long modifiedTimeMs) {
    this.modifiedTimeMs = modifiedTimeMs;
  }

  /**
   * @return whether the blob is a directory (virtual folder name separated by '/')
   */
  public boolean isDirectory() {
    return isDirectory;
  }
}
