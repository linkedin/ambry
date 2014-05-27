package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


// TestDataNode permits DataNode to be constructed with a null Datacenter.
class TestDataNode extends DataNode {
  public TestDataNode(JSONObject jsonObject)
      throws JSONException {
    super(null, jsonObject);
  }

  @Override
  public void validateDatacenter() {
    // Null OK
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestDataNode testDataNode = (TestDataNode) o;

    if (!getHostname().equals(testDataNode.getHostname())) {
      return false;
    }
    if (getPort() != testDataNode.getPort()) {
      return false;
    }
    if (getState() != testDataNode.getState()) {
      return false;
    }
    return getRawCapacityInBytes() == testDataNode.getRawCapacityInBytes();
  }
}

/**
 * Tests {@link DataNode} class.
 */
public class DataNodeTest {
  private static final int diskCount = 10;
  private static final long diskCapacityInBytes = 1000 * 1024 * 1024 * 1024L;

  JSONArray getDisks()
      throws JSONException {
    return TestUtils.getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
  }

  @Test
  public void basics()
      throws JSONException {
    JSONObject jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, HardwareState.AVAILABLE, getDisks());

    DataNode dataNode = new TestDataNode(jsonObject);

    assertEquals(dataNode.getHostname(), TestUtils.getLocalHost());
    assertEquals(dataNode.getPort(), 6666);
    assertEquals(dataNode.getState(), HardwareState.AVAILABLE);

    assertEquals(dataNode.getDisks().size(), diskCount);
    assertEquals(dataNode.getRawCapacityInBytes(), diskCount * diskCapacityInBytes);

    assertEquals(dataNode.toJSONObject().toString(), jsonObject.toString());
    assertEquals(dataNode, new TestDataNode(dataNode.toJSONObject()));
  }

  public void failValidation(JSONObject jsonObject)
      throws JSONException {
    try {
      new TestDataNode(jsonObject);
      fail("Construction of TestDataNode should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation()
      throws JSONException {
    JSONObject jsonObject;

    try {
      // Null DataNode
      jsonObject = TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, HardwareState.AVAILABLE, getDisks());
      new DataNode(null, jsonObject);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    // Bad hostname
    jsonObject = TestUtils.getJsonDataNode("", 6666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject);

    // Bad hostname (http://tools.ietf.org/html/rfc6761 defines 'invalid' top level domain)
    jsonObject = TestUtils.getJsonDataNode("hostname.invalid", 6666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject);

    // Bad port (too small)
    jsonObject = TestUtils.getJsonDataNode(TestUtils.getLocalHost(), -1, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject);

    // Bad port (too big)
    jsonObject = TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 100 * 1000, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject);
  }
}
