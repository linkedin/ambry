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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.PortType;
import java.util.Objects;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


// TestDataNode permits DataNode to be constructed with a null Datacenter.
class TestDataNode extends DataNode {
  public TestDataNode(String dataCenterName, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    super(new TestDatacenter(TestUtils.getJsonDatacenter(dataCenterName, (byte) 0, new JSONArray()), clusterMapConfig),
        jsonObject, clusterMapConfig);
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
    if (getRawCapacityInBytes() != testDataNode.getRawCapacityInBytes()) {
      return false;
    }
    return Objects.equals(getRackId(), testDataNode.getRackId());
  }
}

/**
 * Tests {@link DataNode} class.
 */
public class DataNodeTest {
  private static final int diskCount = 10;
  private static final long diskCapacityInBytes = 1000 * 1024 * 1024 * 1024L;
  private Properties props;

  public DataNodeTest() {
    props = new Properties();
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
  }

  JSONArray getDisks() throws JSONException {
    return TestUtils.getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
  }

  @Test
  public void basics() throws JSONException {

    JSONObject jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    DataNode dataNode = new TestDataNode("datacenter", jsonObject, clusterMapConfig);

    assertEquals(dataNode.getHostname(), TestUtils.getLocalHost());
    assertEquals(dataNode.getPort(), 6666);
    assertEquals(dataNode.getState(), HardwareState.AVAILABLE);

    assertEquals(dataNode.getDisks().size(), diskCount);
    assertEquals(dataNode.getRawCapacityInBytes(), diskCount * diskCapacityInBytes);

    assertNull(dataNode.getRackId());
    assertEquals(TestUtils.DEFAULT_XID, dataNode.getXid());

    assertEquals(dataNode.toJSONObject().toString(), jsonObject.toString());
    assertEquals(dataNode, new TestDataNode("datacenter", dataNode.toJSONObject(), clusterMapConfig));

    // Test with defined rackId
    jsonObject = TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 8666, 42, TestUtils.DEFAULT_XID,
        getDisks(), HardwareState.AVAILABLE);
    dataNode = new TestDataNode("datacenter", jsonObject, clusterMapConfig);
    assertEquals("42", dataNode.getRackId());
    assertEquals(TestUtils.DEFAULT_XID, dataNode.getXid());

    assertEquals(dataNode.toJSONObject().toString(), jsonObject.toString());
    assertEquals(dataNode, new TestDataNode("datacenter", dataNode.toJSONObject(), clusterMapConfig));
  }

  private void failValidation(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    try {
      new TestDataNode("datacenter", jsonObject, clusterMapConfig);
      fail("Construction of TestDataNode should have failed validation.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }
  }

  @Test
  public void validation() throws JSONException {
    JSONObject jsonObject;
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    try {
      // Null DataNode
      jsonObject =
          TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());
      new DataNode(null, jsonObject, clusterMapConfig);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    // Bad hostname
    jsonObject = TestUtils.getJsonDataNode("", 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject, clusterMapConfig);

    // Bad hostname (http://tools.ietf.org/html/rfc6761 defines 'invalid' top level domain)
    jsonObject = TestUtils.getJsonDataNode("hostname.invalid", 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject, clusterMapConfig);

    // Port should between 1025 and 65535
    int[][] portsToTest =
        {{-1, 7666, 8666}, {100 * 1000, 7666, 8666}, {6666, -1, 8666}, {6666, 100 * 1000, 8666}, {6666, 7777, -1},
            {6666, 7777, 100 * 1000}};
    for (int[] ports : portsToTest) {
      jsonObject =
          TestUtils.getJsonDataNode(TestUtils.getLocalHost(), ports[0], ports[1], ports[2], HardwareState.AVAILABLE,
              getDisks());
      failValidation(jsonObject, clusterMapConfig);
    }

    // same port number for plain text and ssl port
    jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 6666, 8666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject, clusterMapConfig);

    // same port number for plain text and HTTP2 port
    jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 6666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject, clusterMapConfig);

    // same port number for http2 port and ssl port
    jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 8666, 8666, HardwareState.AVAILABLE, getDisks());
    failValidation(jsonObject, clusterMapConfig);
  }

  @Test
  public void testSoftState() throws JSONException, InterruptedException {
    JSONObject jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());
    Properties props = new Properties();
    props.setProperty("clustermap.fixedtimeout.datanode.retry.backoff.ms", Integer.toString(2000));
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    int threshold = clusterMapConfig.clusterMapFixedTimeoutDatanodeErrorThreshold;
    long retryBackoffMs = clusterMapConfig.clusterMapFixedTimeoutDataNodeRetryBackoffMs;

    DataNode dataNode = new TestDataNode("datacenter", jsonObject, clusterMapConfig);
    for (int i = 0; i < threshold; i++) {
      ensure(dataNode, HardwareState.AVAILABLE);
      dataNode.onNodeTimeout();
    }
    // After threshold number of continuous errors, the resource should be unavailable
    ensure(dataNode, HardwareState.UNAVAILABLE);

    Thread.sleep(retryBackoffMs + 1);
    // If retryBackoffMs has passed, the resource should be available.
    ensure(dataNode, HardwareState.AVAILABLE);

    //A single timeout should make the node unavailable now
    dataNode.onNodeTimeout();
    ensure(dataNode, HardwareState.UNAVAILABLE);

    //A single response should make the node available now
    dataNode.onNodeResponse();
    ensure(dataNode, HardwareState.AVAILABLE);
  }

  /**
   * Validate {@link DataNodeId#getPortToConnectTo()} returns port type corresponding to the
   * SSL enabled datacenter list specified in {@link ClusterMapConfig}.
   * @throws Exception
   */
  @Test
  public void validateGetPort() throws Exception {
    ClusterMapConfig clusterMapConfig;
    Properties props = new Properties();
    props.setProperty("clustermap.ssl.enabled.datacenters", "datacenter1,datacenter2,datacenter3");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    System.out.println(clusterMapConfig.clusterMapSslEnabledDatacenters);
    JSONObject jsonObject =
        TestUtils.getJsonDataNode(TestUtils.getLocalHost(), 6666, 7666, 8666, HardwareState.AVAILABLE, getDisks());

    DataNode dataNode = new TestDataNode("datacenter2", jsonObject, clusterMapConfig);
    assertEquals("The datacenter of the data node is in the ssl enabled datacenter list. SSL port should be returned",
        PortType.SSL, dataNode.getPortToConnectTo().getPortType());

    dataNode = new TestDataNode("datacenter5", jsonObject, clusterMapConfig);
    assertEquals(
        "The datacenter of the data node is not in the ssl enabled datacenter list. Plaintext port should be returned",
        PortType.PLAINTEXT, dataNode.getPortToConnectTo().getPortType());

    jsonObject.remove("sslport");
    dataNode = new TestDataNode("datacenter1", jsonObject, clusterMapConfig);
    try {
      dataNode.getPortToConnectTo();
      fail("Should have thrown Exception because there is no sslPort.");
    } catch (IllegalStateException e) {
      // The datacenter of the data node is in the ssl enabled datacenter list, but the data node does not have an ssl
      // port to connect. Exception should be thrown.
    }
  }

  void ensure(DataNode dataNode, HardwareState state) {
    assertEquals(dataNode.getState(), state);
    for (DiskId disk : dataNode.getDisks()) {
      assertEquals(disk.getState(), state);
    }
  }
}
