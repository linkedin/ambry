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

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


// Permits Replica to be constructed with a null Partition
class TestReplica extends Replica {
  public TestReplica(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    super(hardwareLayout, null, jsonObject);
  }

  public TestReplica(TestHardwareLayout hardwareLayout, Disk disk) throws JSONException {
    super(null, disk, hardwareLayout.clusterMapConfig);
  }

  @Override
  public void validatePartition() {
    // Null OK
  }
}

/**
 * Tests {@link Replica} class.
 */
public class ReplicaTest {

  @Test
  public void basics() throws JSONException {
    // Much of Replica depends on Partition. With a null Partition, only nominal testing can be done.
    TestHardwareLayout thl = new TestHardwareLayout("Alpha");
    Disk disk = thl.getRandomDisk();

    TestReplica replicaA = new TestReplica(thl, disk);
    assertEquals(replicaA.getDiskId(), disk);

    TestReplica replicaB = new TestReplica(thl.getHardwareLayout(), getJsonReplica(disk));
    assertEquals(replicaB.getDiskId(), disk);
  }

  @Test
  public void validation() throws JSONException {
    TestHardwareLayout thl = new TestHardwareLayout("Alpha");
    Partition partition =
        new Partition(1, thl.clusterMapConfig.clusterMapDefaultPartitionClass, PartitionState.READ_WRITE,
            100 * 1024 * 1024 * 1024L);
    try {
      // Null Partition
      new Replica(null, thl.getRandomDisk(), thl.clusterMapConfig);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    try {
      // Null clusterMapConfig
      new Replica(partition, thl.getRandomDisk(), null);
      fail("Should have failed during instantiation");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }
}
