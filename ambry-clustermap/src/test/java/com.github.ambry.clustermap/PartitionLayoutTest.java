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
import org.junit.Test;

import static org.junit.Assert.*;


public class PartitionLayoutTest {
  @Test
  public void basics() throws JSONException {
    TestUtils.TestPartitionLayout testPartitionLayout =
        new TestUtils.TestPartitionLayout(new TestUtils.TestHardwareLayout("Alpha"));

    PartitionLayout partitionLayout = testPartitionLayout.getPartitionLayout();

    assertEquals(partitionLayout.getVersion(), TestUtils.TestPartitionLayout.defaultVersion);
    assertEquals(partitionLayout.getClusterName(), "Alpha");
    assertEquals(partitionLayout.getPartitions().size(), testPartitionLayout.getPartitionCount());
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
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");

    try {
      TestUtils.TestPartitionLayout tpl = new TestUtils.TestPartitionLayoutWithDuplicatePartitions(testHardwareLayout);
      fail("Should have failed validation:" + tpl.getPartitionLayout().toString());
    } catch (IllegalStateException e) {
      // Expected.
    }

    try {
      TestUtils.TestPartitionLayout tpl = new TestUtils.TestPartitionLayoutWithDuplicateReplicas(testHardwareLayout);
      fail("Should have failed validation:" + tpl.getPartitionLayout().toString());
    } catch (IllegalStateException e) {
      // Expected.
    }
  }
}
