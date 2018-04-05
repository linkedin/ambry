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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.json.JSONException;
import org.junit.Test;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


public class PartitionLayoutTest {
  @Test
  public void basics() throws JSONException {
    TestHardwareLayout hardwareLayout = new TestHardwareLayout("Alpha");
    String dc = hardwareLayout.getHardwareLayout().getDatacenters().get(0).getName();
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(hardwareLayout, dc);

    PartitionLayout partitionLayout = testPartitionLayout.getPartitionLayout();

    assertEquals(partitionLayout.getVersion(), TestPartitionLayout.defaultVersion);
    assertEquals(partitionLayout.getClusterName(), "Alpha");
    assertEquals(partitionLayout.getPartitions(null).size(), testPartitionLayout.getPartitionCount());
    assertEquals(partitionLayout.getPartitionCount(), testPartitionLayout.getPartitionCount());
    assertEquals(partitionLayout.getAllocatedRawCapacityInBytes(),
        testPartitionLayout.getAllocatedRawCapacityInBytes());
    assertEquals(partitionLayout.getAllocatedUsableCapacityInBytes(),
        testPartitionLayout.getAllocatedUsableCapacityInBytes());
    assertEquals(partitionLayout.getPartitionInStateCount(PartitionState.READ_WRITE),
        testPartitionLayout.countPartitionsInState(PartitionState.READ_WRITE));
    assertEquals(partitionLayout.getPartitionInStateCount(PartitionState.READ_ONLY),
        testPartitionLayout.countPartitionsInState(PartitionState.READ_ONLY));
  }

  @Test
  public void validation() throws JSONException {
    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");

    try {
      TestPartitionLayout tpl = new TestPartitionLayoutWithDuplicatePartitions(testHardwareLayout);
      fail("Should have failed validation:" + tpl.getPartitionLayout().toString());
    } catch (IllegalStateException e) {
      // Expected.
    }

    try {
      TestPartitionLayout tpl = new TestPartitionLayoutWithDuplicateReplicas(testHardwareLayout);
      fail("Should have failed validation:" + tpl.getPartitionLayout().toString());
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  /**
   * Tests for {@link PartitionLayout#getPartitions(String)} and {@link PartitionLayout#getWritablePartitions(String)}.
   * @throws JSONException
   */
  @Test
  public void getPartitionsTest() throws JSONException {
    String specialPartitionClass = "specialPartitionClass";
    TestHardwareLayout hardwareLayout = new TestHardwareLayout("Alpha");
    String dc = hardwareLayout.getRandomDatacenter().getName();
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(hardwareLayout, dc);
    assertTrue("There should be more than 1 replica per partition in each DC for this test to work",
        testPartitionLayout.replicaCountPerDc > 1);
    PartitionRangeCheckParams defaultRw =
        new PartitionRangeCheckParams(0, testPartitionLayout.partitionCount, DEFAULT_PARTITION_CLASS,
            PartitionState.READ_WRITE);
    // add 15 RW partitions for the special class
    PartitionRangeCheckParams specialRw =
        new PartitionRangeCheckParams(defaultRw.rangeEnd + 1, 15, specialPartitionClass, PartitionState.READ_WRITE);
    testPartitionLayout.addNewPartitions(specialRw.count, specialPartitionClass, PartitionState.READ_WRITE, dc);
    // add 10 RO partitions for the default class
    PartitionRangeCheckParams defaultRo =
        new PartitionRangeCheckParams(specialRw.rangeEnd + 1, 10, DEFAULT_PARTITION_CLASS, PartitionState.READ_ONLY);
    testPartitionLayout.addNewPartitions(defaultRo.count, DEFAULT_PARTITION_CLASS, PartitionState.READ_ONLY, dc);
    // add 5 RO partitions for the special class
    PartitionRangeCheckParams specialRo =
        new PartitionRangeCheckParams(defaultRo.rangeEnd + 1, 5, specialPartitionClass, PartitionState.READ_ONLY);
    testPartitionLayout.addNewPartitions(specialRo.count, specialPartitionClass, PartitionState.READ_ONLY, dc);

    PartitionLayout partitionLayout = testPartitionLayout.getPartitionLayout();
    // "good" cases for getPartitions() and getWritablePartitions() only
    // getPartitions(), class null
    List<PartitionId> returnedPartitions = partitionLayout.getPartitions(null);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, defaultRo, specialRw, specialRo));
    // getWritablePartitions(), class null
    returnedPartitions = partitionLayout.getWritablePartitions(null);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, specialRw));

    // getPartitions(), class default
    returnedPartitions = partitionLayout.getPartitions(DEFAULT_PARTITION_CLASS);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, defaultRo));
    // getWritablePartitions(), class default
    returnedPartitions = partitionLayout.getWritablePartitions(DEFAULT_PARTITION_CLASS);
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(defaultRw));

    // getPartitions(), class special
    returnedPartitions = partitionLayout.getPartitions(specialPartitionClass);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(specialRw, specialRo));
    // getWritablePartitions(), class special
    returnedPartitions = partitionLayout.getWritablePartitions(specialPartitionClass);
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(specialRw));

    // to test the dc affinity, we pick one datanode from "dc" and insert 1 replica for part1 (special class) in "dc"
    // and make sure that it is returned in getPartitions() but not in getWritablePartitions() (because all the other
    // partitions have more than 1 replica in "dc").
    DataNode dataNode = hardwareLayout.getRandomDataNodeFromDc(dc);
    Partition partition =
        partitionLayout.addNewPartition(dataNode.getDisks().subList(0, 1), testPartitionLayout.replicaCapacityInBytes,
            specialPartitionClass);
    PartitionRangeCheckParams extraPartCheckParams =
        new PartitionRangeCheckParams(specialRo.rangeEnd + 1, 1, specialPartitionClass, PartitionState.READ_WRITE);
    // getPartitions(), class special
    returnedPartitions = partitionLayout.getPartitions(specialPartitionClass);
    assertTrue("Added partition should exist in returned partitions", returnedPartitions.contains(partition));
    checkReturnedPartitions(returnedPartitions, Arrays.asList(specialRw, specialRo, extraPartCheckParams));
    // getWritablePartitions(), class special
    returnedPartitions = partitionLayout.getWritablePartitions(specialPartitionClass);
    assertFalse("Added partition should not exist in returned partitions", returnedPartitions.contains(partition));
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(specialRw));
  }
}
