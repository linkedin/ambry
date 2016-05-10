/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BlobIdTest {
  @Test
  public void basicTest() {
    final long id = 99;
    final long replicaCapacityInBytes = 1024 * 1024 * 1024;
    PartitionId partitionId = new Partition(id, PartitionState.READ_WRITE, replicaCapacityInBytes);
    BlobId blobId = new BlobId(partitionId);

    assertEquals(blobId.getPartition(), partitionId);
    System.out.println("Blob Id toString: " + blobId);
    System.out.println("Blob id sizeInBytes: " + blobId.toString().length());
  }
}
