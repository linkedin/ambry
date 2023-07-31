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
package com.github.ambry.repair;

import java.util.Objects;


/**
 * It is the record of the partially failed request.
 * It can be a TtlUpdate or a Delete request.
 * The request was executed successfully on one replica but failed to execute on other replicas.
 */
public class RepairRequestRecord {
  // Blob Id
  private final String blobId;
  // Partition id of the blob
  private final long partitionId;
  // the source replica on which the request was executed successfully. ODR may replicate the blob from it.
  private final String sourceHostName;
  private final int sourceHostPort;

  // operation type, either TtlUpdate or Delete
  private final OperationType operationType;
  // operation time
  private final long operationTimeMs;
  // life version of the blob
  private final short lifeVersion;
  // if it's the TtlUpdate request, it's the ttl expiration time.
  private final long expirationTimeMs;

  public RepairRequestRecord(String blobId, long partitionId, String sourceHostName, int sourceHostPort,
      OperationType operationType, long operationTimeMs, short lifeVersion, long expirationTimeMs) {
    this.blobId = blobId;
    this.partitionId = partitionId;
    this.sourceHostName = sourceHostName;
    this.sourceHostPort = sourceHostPort;
    this.operationType = operationType;
    this.operationTimeMs = operationTimeMs;
    this.lifeVersion = lifeVersion;
    this.expirationTimeMs = expirationTimeMs;
  }

  /**
   * @return the blob ID
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return the partition ID
   */
  public long getPartitionId() {
    return partitionId;
  }

  /**
   * @return the source replica's host name
   */
  public String getSourceHostName() {
    return sourceHostName;
  }

  /**
   * @return the source replica's port number
   */
  public int getSourceHostPort() {
    return sourceHostPort;
  }

  /**
   * @return the operation type, either TtlUpdate or Delete
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * @return the operation time in milliseconds
   */
  public long getOperationTimeMs() {
    return operationTimeMs;
  }

  /**
   * @return the life version of the blob
   */
  public short getLifeVersion() {
    return lifeVersion;
  }

  /**
   * @return the expiration time in milliseconds of the ttl expiration time.
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
    RepairRequestRecord record = (RepairRequestRecord) o;
    return Objects.equals(blobId, record.blobId) && partitionId == record.partitionId && Objects.equals(sourceHostName,
        record.sourceHostName) && sourceHostPort == record.sourceHostPort && operationType == record.operationType
        && operationTimeMs == record.operationTimeMs && lifeVersion == record.lifeVersion
        && expirationTimeMs == record.expirationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobId, operationType);
  }

  @Override
  public String toString() {
    return "RepairRequestRecord[blobId=" + blobId + ",partitionId=" + partitionId + ",sourceHostName=" + sourceHostName
        + ",sourceHostPort=" + sourceHostPort + ",operationType=" + operationType + ",operationTimeMs="
        + operationTimeMs + ",lifeVersion=" + lifeVersion + ",expirationTimeMs=" + expirationTimeMs + "]";
  }

  public enum OperationType {
    DeleteRequest, TtlUpdateRequest
  }
}
