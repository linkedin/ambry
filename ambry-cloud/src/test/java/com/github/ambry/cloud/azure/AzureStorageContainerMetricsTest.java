/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import java.util.Optional;
import org.junit.Test;

import static org.junit.Assert.*;


public class AzureStorageContainerMetricsTest {

  private AzureStorageContainerMetrics partitionMetrics;

  public AzureStorageContainerMetricsTest() {
    partitionMetrics = new AzureStorageContainerMetrics(314L);
  }

  @Test
  public void testAzureStorageContainerMetrics() {
    // no replicas
    assertEquals(Optional.of(0L).get(), partitionMetrics.getPartitionLag());

    // one replica
    partitionMetrics.setPartitionReplicaLag("localhost1", 100);
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // two replicas
    partitionMetrics.setPartitionReplicaLag("localhost1", 100);
    partitionMetrics.setPartitionReplicaLag("localhost2", 1000);
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // two replicas, lag changes
    partitionMetrics.setPartitionReplicaLag("localhost2", 10);
    assertEquals(Optional.of(10L).get(), partitionMetrics.getPartitionLag());

    // one replica removed
    partitionMetrics.removePartitionReplica("localhost2");
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // both replicas removed
    partitionMetrics.removePartitionReplica("localhost1");
    assertEquals(Optional.of(0L).get(), partitionMetrics.getPartitionLag());
  }

}
