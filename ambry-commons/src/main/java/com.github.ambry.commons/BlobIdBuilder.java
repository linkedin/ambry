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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.PartitionId;


/**
 * A builder class for {@link BlobId}. This class is not thread safe.
 */
public class BlobIdBuilder {
  protected Byte flag;
  protected Byte datacenterId;
  protected Short accountId;
  protected Short containerId;
  protected PartitionId partitionId;

  /**
   * Constructor.
   * @param partitionId The {@link PartitionId} to be set to the blob id.
   */
  public BlobIdBuilder(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Sets flag value to the builder. There is no effect to call this method when {@link BlobId#CURRENT_VERSION} is set
   * to {@link BlobId#BLOB_ID_V1}, and the flag value of the built {@link BlobId} will always be {@link BlobId#DEFAULT_FLAG}.
   * @param flag The flag for the blobId to set.
   * @return this builder.
   */
  public BlobIdBuilder setFlag(Byte flag) {
    this.flag = flag;
    return this;
  }

  /**
   * Sets datacenterId value to the builder. There is no effect to call this method when {@link BlobId#CURRENT_VERSION}
   * is set to {@link BlobId#BLOB_ID_V1}, and the datacenterId value of the built {@link BlobId} will always be
   * {@link com.github.ambry.clustermap.ClusterMapUtils#LEGACY_DATACENTER_ID}.
   * @param datacenterId The id of the datacenter to set.
   * @return this builder.
   */
  public BlobIdBuilder setDatacenterId(Byte datacenterId) {
    this.datacenterId = datacenterId;
    return this;
  }

  /**
   * Sets accountId value to the builder. There is no effect to call this method when {@link BlobId#CURRENT_VERSION} is
   * set to {@link BlobId#BLOB_ID_V1}, and the accountId value of the built {@link BlobId} will always be
   * {@link Account#LEGACY_ACCOUNT_ID}.
   * @param accountId The id of the {@link Account} to set.
   * @return this builder.
   */
  public BlobIdBuilder setAccountId(Short accountId) {
    this.accountId = accountId;
    return this;
  }

  /**
   * Sets containerId value to the builder. There is no effect to call this method when {@link BlobId#CURRENT_VERSION}
   * is set to {@link BlobId#BLOB_ID_V1}, and the containerId value of the built {@link BlobId} will always be
   * {@link Container#LEGACY_CONTAINER_ID}.
   * @param containerId The id of the {@link Container} to set.
   * @return this builder.
   */
  public BlobIdBuilder setContainerId(Short containerId) {
    this.containerId = containerId;
    return this;
  }

  /**
   * Sets partitionId value to the builder.
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
