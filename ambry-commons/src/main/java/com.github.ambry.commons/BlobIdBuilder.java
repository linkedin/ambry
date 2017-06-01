/*
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
package com.github.ambry.commons;

import com.github.ambry.clustermap.PartitionId;


/**
 * A builder class for {@link BlobId}. This class is not thread safe.
 */
public class BlobIdBuilder {
  private Byte flag;
  private Byte datacenterId;
  private Short accountId;
  private Short containerId;
  private PartitionId partitionId;

  /**
   * Constructs a {@code BlobIdBuilder}.
   * @param partitionId The {@link PartitionId} to be set to the blob id.
   */
  public BlobIdBuilder(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Construct a {@code BlobIdBuilder}.
   * @param flag The flag to be set to the blob id.
   * @param datacenterId The id of the datacenter to be set to the blob id.
   * @param accountId The id of the {@link Account} to be set to the blob id.
   * @param containerId The id of the {@link Container} to be set to the blob id.
   * @param partitionId The {@link PartitionId} to be set to the blob id.
   */
  public BlobIdBuilder(Byte flag, Byte datacenterId, Short accountId, Short containerId, PartitionId partitionId) {
    this.flag = flag;
    this.datacenterId = datacenterId;
    this.accountId = accountId;
    this.containerId = containerId;
    this.partitionId = partitionId;
  }

  /**
   * Sets flag to the builder.
   * @param flag The flag for the blobId to set.
   * @return this builder.
   */
  public BlobIdBuilder setFlag(Byte flag) {
    this.flag = flag;
    return this;
  }

  /**
   * Sets datacenterId to the builder.
   * @param datacenterId The id of the datacenter to set.
   * @return this builder.
   */
  public BlobIdBuilder setDatacenterId(Byte datacenterId) {
    this.datacenterId = datacenterId;
    return this;
  }

  /**
   * Sets accountId to the builder.
   * @param accountId The id of the {@link Account} to set.
   * @return this builder.
   */
  public BlobIdBuilder setAccountId(Short accountId) {
    this.accountId = accountId;
    return this;
  }

  /**
   * Sets containerId to the builder.
   * @param containerId The id of the {@link Container} to set.
   * @return this builder.
   */
  public BlobIdBuilder setContainerId(Short containerId) {
    this.containerId = containerId;
    return this;
  }


  /**
   * Sets partitionId to the builder.
   * @param partitionId {@link PartitionId} to set.
   * @return this builder.
   */
  public BlobIdBuilder setPartitionId(PartitionId partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  /**
   * Builds a {@link BlobId}.
   * @return A {@link BlobId}.
   */
  public BlobId build() {
    return new BlobId(flag, datacenterId, accountId, containerId, partitionId);
  }
}
