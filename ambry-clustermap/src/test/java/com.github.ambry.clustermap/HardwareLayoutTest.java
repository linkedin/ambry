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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests {@link HardwareLayout} class.
 */
public class HardwareLayoutTest {
  private static final int diskCount = 10;
  private static final long diskCapacityInBytes = 1000 * 1024 * 1024 * 1024L;
  private static final int dataNodeCount = 6;
  private static final int datacenterCount = 3;
  private static final int basePort = 6666;
  private static final int baseSslPort = 7666;
  private static final int baseHttp2Port = 8666;
  private Properties props;

  public HardwareLayoutTest() {
    props = new Properties();
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
  }

  private JSONArray getDisks() throws JSONException {
    return TestUtils.getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
  }

  private JSONArray getDuplicateDisks() throws JSONException {
    return TestUtils.getJsonArrayDuplicateDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
  }

  private JSONArray getDataNodes(int basePort, int sslPort, int http2Port, JSONArray disks) throws JSONException {
    return TestUtils.getJsonArrayDataNodes(dataNodeCount, TestUtils.getLocalHost(), basePort, sslPort, http2Port,
        HardwareState.AVAILABLE, disks);
  }

  private JSONArray getDuplicateDataNodes(int basePort, int sslPort, int http2Port, JSONArray disks)
      throws JSONException {
    return TestUtils.getJsonArrayDuplicateDataNodes(dataNodeCount, TestUtils.getLocalHost(), basePort, sslPort,
        http2Port, HardwareState.AVAILABLE, disks);
  }

  private JSONArray getDatacenters() throws JSONException {
    List<String> names = new ArrayList<String>(datacenterCount);
    List<JSONArray> dataNodes = new ArrayList<JSONArray>(datacenterCount);

    int curBasePort = basePort;
    int curSslBasePort = baseSslPort + 1000;
    int curHttp2BasePort = baseHttp2Port + 1000;
    for (int i = 0; i < datacenterCount; i++) {
      names.add(i, "DC" + i);
      dataNodes.add(i, getDataNodes(curBasePort, curSslBasePort, curHttp2BasePort, getDisks()));
      curBasePort += dataNodeCount;
      curSslBasePort += dataNodeCount;
      curHttp2BasePort += dataNodeCount;
    }

    return TestUtils.getJsonArrayDatacenters(names, dataNodes);
  }

  private JSONArray getDatacentersWithDuplicateDisks() throws JSONException {
    List<String> names = new ArrayList<String>(datacenterCount);
    List<JSONArray> dataNodes = new ArrayList<JSONArray>(datacenterCount);

    int curBasePort = basePort;
    int curSslBasePort = baseSslPort + 1000;
    int curHttp2BasePort = baseHttp2Port + 1000;
    for (int i = 0; i < datacenterCount; i++) {
      names.add(i, "DC" + i);
      dataNodes.add(i, getDataNodes(curBasePort, curSslBasePort, curHttp2BasePort, getDuplicateDisks()));
      curBasePort += dataNodeCount;
      curSslBasePort += dataNodeCount;
      curHttp2BasePort += dataNodeCount;
    }

    return TestUtils.getJsonArrayDatacenters(names, dataNodes);
  }

  // All nodes within each datacenter are duplicates. Each datacenter hosts a different repeated node.
  private JSONArray getDatacentersWithDuplicateDataNodes() throws JSONException {
    List<String> names = new ArrayList<String>(datacenterCount);
    List<JSONArray> dataNodes = new ArrayList<JSONArray>(datacenterCount);

    int curBasePort = basePort;
    int curSslBasePort = baseSslPort + 1000;
    int curHttp2BasePort = baseHttp2Port + 1000;
    for (int i = 0; i < datacenterCount; i++) {
      names.add(i, "DC" + i);
      dataNodes.add(i, getDuplicateDataNodes(curBasePort, curSslBasePort, curHttp2BasePort, getDisks()));
      curBasePort += dataNodeCount;
      curSslBasePort += dataNodeCount;
      curHttp2BasePort += dataNodeCount;
    }

    return TestUtils.getJsonArrayDatacenters(names, dataNodes);
  }

  private JSONArray getDuplicateDatacenters() throws JSONException {
    List<String> names = new ArrayList<String>(datacenterCount);
    List<JSONArray> dataNodes = new ArrayList<JSONArray>(datacenterCount);

    int curBasePort = basePort;
    int curSslBasePort = baseSslPort + 1000;
    int curHttp2BasePort = baseHttp2Port + 1000;
    for (int i = 0; i < datacenterCount; i++) {
      names.add(i, "DC");
      dataNodes.add(i, getDataNodes(curBasePort, curSslBasePort, curHttp2BasePort, getDisks()));
      curBasePort += dataNodeCount;
      curSslBasePort += dataNodeCount;
      curHttp2BasePort += dataNodeCount;
    }

    return TestUtils.getJsonArrayDatacenters(names, dataNodes);
  }

  @Test
  public void basics() throws JSONException {
    JSONObject jsonObject = TestUtils.getJsonHardwareLayout("Alpha", getDatacenters());

    HardwareLayout hardwareLayout =
        new HardwareLayout(jsonObject, new ClusterMapConfig(new VerifiableProperties(props)));

    assertEquals(hardwareLayout.getVersion(), TestUtils.defaultHardwareLayoutVersion);
    assertEquals(hardwareLayout.getClusterName(), "Alpha");
    assertEquals(hardwareLayout.getDatacenters().size(), datacenterCount);
    assertEquals(hardwareLayout.getRawCapacityInBytes(),
        datacenterCount * dataNodeCount * diskCount * diskCapacityInBytes);
    assertEquals(hardwareLayout.toJSONObject().toString(), jsonObject.toString());

    assertEquals(hardwareLayout.getDataNodeInHardStateCount(HardwareState.AVAILABLE), datacenterCount * dataNodeCount);
    assertEquals(hardwareLayout.getDataNodeInHardStateCount(HardwareState.UNAVAILABLE), 0);
    assertEquals(hardwareLayout.calculateUnavailableDataNodeCount(), 0);
    assertEquals(hardwareLayout.getDiskInHardStateCount(HardwareState.AVAILABLE),
        datacenterCount * dataNodeCount * diskCount);
    assertEquals(hardwareLayout.getDiskInHardStateCount(HardwareState.UNAVAILABLE), 0);
    assertEquals(hardwareLayout.calculateUnavailableDiskCount(), 0);
  }

  public void failValidation(JSONObject jsonObject) throws JSONException {
    try {
      new HardwareLayout(jsonObject, new ClusterMapConfig(new VerifiableProperties(props)));
      fail("Should have failed validation: " + jsonObject.toString(2));
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation() throws JSONException {
    JSONObject jsonObject;

    // Bad cluster name
    jsonObject = TestUtils.getJsonHardwareLayout("", getDatacenters());
    failValidation(jsonObject);

    // Duplicate disks
    jsonObject = TestUtils.getJsonHardwareLayout("Beta", getDatacentersWithDuplicateDisks());
    failValidation(jsonObject);

    // Duplicate data nodes
    jsonObject = TestUtils.getJsonHardwareLayout("Beta", getDatacentersWithDuplicateDataNodes());
    failValidation(jsonObject);

    // Duplicate datacenters
    jsonObject = TestUtils.getJsonHardwareLayout("Beta", getDuplicateDatacenters());
    failValidation(jsonObject);
  }
}
