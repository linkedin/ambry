package com.github.ambry.clustermap;


import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PartitionLayoutTest {
  @Test
  public void basics() throws JSONException {
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(new TestUtils.TestHardwareLayout("Alpha"));

    PartitionLayout partitionLayout = testPartitionLayout.getPartitionLayout();

    assertEquals(partitionLayout.getClusterName(), "Alpha");
    assertEquals(partitionLayout.getPartitions().size(), testPartitionLayout.getPartitionCount());
    assertEquals(partitionLayout.getCapacityInBytes(), testPartitionLayout.getCapacityInBytes());
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
