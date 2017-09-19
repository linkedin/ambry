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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import java.io.IOException;


/**
 *  A class that makes {@link BlobId#getCurrentVersion()} to return {@link BlobId#BLOB_ID_V1}, which will serialize
 *  a blobId to {@link BlobIdV1}.
 */
public class BlobIdV1 extends BlobId {
  public BlobIdV1(byte flag, byte datacenterId, short accountId, short containerId, PartitionId partitionId) {
    super(flag, datacenterId, accountId, containerId, partitionId);
  }

  /**
   * Re-constructs existing blobId by deserializing from BlobId "string".
   *
   * @param id of Blob as output by BlobId.getID().
   * @param clusterMap of the cluster that the blobId belongs to.
   * @throws IOException
   */
  public BlobIdV1(String id, ClusterMap clusterMap) throws IOException {
    super(id, clusterMap);
  }

  @Override
  protected short getCurrentVersion() {
    return BLOB_ID_V1;
  }
}
