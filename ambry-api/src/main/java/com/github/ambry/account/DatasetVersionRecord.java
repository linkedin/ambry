/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import java.util.Objects;


/**
 * Represent a metadata record in a {@link Dataset} implementation
 */
public class DatasetVersionRecord {
  private final int accountId;
  private final int containerId;
  private final String datasetName;
  private final String version;
  private final long expirationTimeMs;
  private final Long creationTimeMs;
  private String renameFrom;

  /**
   * Constructor that takes individual arguments.
   *
   * @param accountId        the id of the parent account.
   * @param containerId      the id of the container.
   * @param datasetName      the name of the dataset.
   * @param version          the version of the dataset.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param renameFrom       the original version which renamed from
   */
  public DatasetVersionRecord(int accountId, int containerId, String datasetName, String version,
      long expirationTimeMs, String renameFrom) {
    this(accountId, containerId, datasetName, version, expirationTimeMs, null, renameFrom);
  }

  /**
   * Constructor for retention policy support.
   *
   * @param accountId        the id of the parent account.
   * @param containerId      the id of the container.
   * @param datasetName      the name of the dataset.
   * @param version          the version of the dataset.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   * @param creationTimeMs   the creation time in milliseconds since epoch for dataset version.
   * @param renameFrom       the original version which renamed from
   */
  public DatasetVersionRecord(int accountId, int containerId, String datasetName, String version, long expirationTimeMs,
      Long creationTimeMs, String renameFrom) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.datasetName = datasetName;
    this.version = version;
    this.expirationTimeMs = expirationTimeMs;
    this.creationTimeMs = creationTimeMs;
    this.renameFrom = renameFrom;
  }

  /**
   * @return the account id.
   */
  public int getAccountId() {
    return accountId;
  }

  /**
   * @return the container id.
   */
  public int getContainerId() {
    return containerId;
  }

  /**
   * @return the dataset name.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * @return the version of the dataset.
   */
  public String getVersion() {
    return version;
  }

  /**
   * @return the expiration time in milliseconds of the dataset version.
   */
  public long getExpirationTimeMs() {
    return expirationTimeMs;
  }

  /**
   * @return the creation time of the dataset version.
   */
  public long getCreationTimeMs() {
    return creationTimeMs;
  }

  public String getRenameFrom() {
    return renameFrom;
  }

  public void setRenameFrom(String renameFrom) {
    this.renameFrom = renameFrom;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetVersionRecord record = (DatasetVersionRecord) o;
    return accountId == record.accountId && containerId == record.containerId && Objects.equals(datasetName,
        record.datasetName) && Objects.equals(version, record.version) && expirationTimeMs == record.expirationTimeMs
        && Objects.equals(creationTimeMs, record.creationTimeMs) && Objects.equals(renameFrom, record.renameFrom);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }

  @Override
  public String toString() {
    return "DatasetVersionRecord[accountId=" + accountId + ",containerId=" + containerId + ",datasetName=" + datasetName
        + ",version=" + version + ",expirationTimeMs=" + expirationTimeMs + ",creationTimeMs=" + creationTimeMs + "]";
  }
}

