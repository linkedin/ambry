package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


// TestDisk permits Disk to be constructed with a null DataNode.
class TestDisk extends Disk {
  public TestDisk(JSONObject jsonObject)
      throws JSONException {
    super(null, jsonObject);
  }

  @Override
  public void validateDataNode() {
    // Null DataNodeId OK for test.
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestDisk testDisk = (TestDisk) o;

    if (!getMountPath().equals(testDisk.getMountPath())) {
      return false;
    }
    if (getRawCapacityInBytes() != testDisk.getRawCapacityInBytes()) {
      return false;
    }
    return getHardState() == testDisk.getHardState();
  }
}

/**
 * Tests {@link Disk} class.
 */
public class DiskTest {
  @Test
  public void basics()
      throws JSONException {
    JSONObject jsonObject = TestUtils.getJsonDisk("/mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L);

    Disk testDisk = new TestDisk(jsonObject);

    assertEquals(testDisk.getMountPath(), "/mnt1");
    assertEquals(testDisk.getHardState(), HardwareState.AVAILABLE);
    assertEquals(testDisk.getRawCapacityInBytes(), 100 * 1024 * 1024 * 1024L);
    assertEquals(testDisk.toJSONObject().toString(), jsonObject.toString());
    assertEquals(testDisk, new TestDisk(testDisk.toJSONObject()));
  }

  public void failValidation(JSONObject jsonObject)
      throws JSONException {
    try {
      new TestDisk(jsonObject);
      fail("Construction of TestDisk should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation()
      throws JSONException {
    try {
      // Null DataNode
      new Disk(null, TestUtils.getJsonDisk("/mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L));
      fail("Construction of Disk should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    // Bad mount path
    failValidation(TestUtils.getJsonDisk("", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L));

    // Bad capacity (too small)
    failValidation(TestUtils.getJsonDisk("/mnt1", HardwareState.UNAVAILABLE, 0));

    // Bad capacity (too big)
    failValidation(TestUtils
        .getJsonDisk("/mnt1", HardwareState.UNAVAILABLE, 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024L));
  }
}
