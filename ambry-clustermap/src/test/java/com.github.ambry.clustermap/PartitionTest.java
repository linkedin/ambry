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
package com.github.ambry.clustermap;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests {@link Partition} class.
 */
public class PartitionTest {

  @Test
  public void basics() throws JSONException {
    String partitionClass = getRandomString(10);
    int replicaCount = 3;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
    int dcCount = thl.getDatacenterCount();
    JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(thl.getIndependentDisks(replicaCount));
    JSONObject jsonObject =
        TestUtils.getJsonPartition(99, partitionClass, PartitionState.READ_WRITE, replicaCapacityInBytes, jsonReplicas);

    Partition partition = new Partition(thl.getHardwareLayout(), jsonObject);
    assertEquals("Partition class is not as expected", partitionClass, partition.getPartitionClass());
    assertEquals(partition.getReplicaIds().size(), replicaCount * dcCount);
    assertEquals(partition.getReplicaCapacityInBytes(), replicaCapacityInBytes);
    assertEquals(partition.getPartitionState(), PartitionState.READ_WRITE);
  }

  private void failValidation(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    try {
      new Partition(hardwareLayout, jsonObject);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation() throws JSONException {
    int replicaCount = 3;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
    int dcCount = thl.getDatacenterCount();
    JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(thl.getIndependentDisks(replicaCount));
    JSONObject jsonObject;

    // Bad replica capacity in bytes (too small)
    jsonObject =
        TestUtils.getJsonPartition(99, TestUtils.DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE, -1, jsonReplicas);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Bad replica capacity in bytes (too big)
    jsonObject = TestUtils.getJsonPartition(99, TestUtils.DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE,
        11L * 1024 * 1024 * 1024 * 1024, jsonReplicas);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Multiple Replica on same Disk.
    List<Disk> disks = new ArrayList<Disk>(replicaCount);
    Disk randomDisk = thl.getRandomDisk();
    for (int i = 0; i < replicaCount; i++) {
      disks.add(randomDisk);
    }
    JSONArray jsonReplicasSameDisk = TestUtils.getJsonArrayReplicas(disks);
    jsonObject = TestUtils.getJsonPartition(99, TestUtils.DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE,
        replicaCapacityInBytes, jsonReplicasSameDisk);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Multiple Replica on same DataNode.
    JSONArray jsonReplicasSameDataNode = TestUtils.getJsonArrayReplicas(thl.getDependentDisks(replicaCount));
    jsonObject = TestUtils.getJsonPartition(99, TestUtils.DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE,
        replicaCapacityInBytes, jsonReplicasSameDataNode);
    failValidation(thl.getHardwareLayout(), jsonObject);
  }
}
