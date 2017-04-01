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
import java.util.Properties;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


// TestDisk permits Disk to be constructed with a null DataNode.
class TestDisk extends Disk {
  public TestDisk(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    super(null, jsonObject, clusterMapConfig);
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

  @Override
  public HardwareState getState() {
    return isDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }
}

/**
 * Tests {@link Disk} class.
 */
public class DiskTest {
  private Properties props;

  public DiskTest() {
    props = new Properties();
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
  }

  @Test
  public void basics() throws JSONException {
    JSONObject jsonObject = TestUtils.getJsonDisk("/mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    Disk testDisk = new TestDisk(jsonObject, clusterMapConfig);

    assertEquals(testDisk.getMountPath(), "/mnt1");
    assertEquals(testDisk.getHardState(), HardwareState.AVAILABLE);
    assertEquals(testDisk.getRawCapacityInBytes(), 100 * 1024 * 1024 * 1024L);
    assertEquals(testDisk.toJSONObject().toString(), jsonObject.toString());
    assertEquals(testDisk, new TestDisk(testDisk.toJSONObject(), clusterMapConfig));
  }

  public void failValidation(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    try {
      new TestDisk(jsonObject, clusterMapConfig);
      fail("Construction of TestDisk should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation() throws JSONException {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    try {
      // Null DataNode
      new Disk(null, TestUtils.getJsonDisk("/mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L),
          clusterMapConfig);
      fail("Construction of Disk should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    // Bad mount path (empty)
    failValidation(TestUtils.getJsonDisk("", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L), clusterMapConfig);

    // Bad mount path (relative path)
    failValidation(TestUtils.getJsonDisk("mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L), clusterMapConfig);

    // Bad capacity (too small)
    failValidation(TestUtils.getJsonDisk("/mnt1", HardwareState.UNAVAILABLE, 0), clusterMapConfig);

    // Bad capacity (too big)
    failValidation(TestUtils.getJsonDisk("/mnt1", HardwareState.UNAVAILABLE,
        1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024L), clusterMapConfig);
  }

  @Test
  public void testDiskSoftState() throws JSONException, InterruptedException {
    JSONObject jsonObject = TestUtils.getJsonDisk("/mnt1", HardwareState.AVAILABLE, 100 * 1024 * 1024 * 1024L);
    Properties props = new Properties();
    props.setProperty("clustermap.fixedtimeout.disk.retry.backoff.ms", Integer.toString(2000));
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    int threshold = clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold;
    long retryBackoffMs = clusterMapConfig.clusterMapFixedTimeoutDiskRetryBackoffMs;

    Disk testDisk = new TestDisk(jsonObject, clusterMapConfig);
    for (int i = 0; i < threshold; i++) {
      assertEquals(testDisk.getState(), HardwareState.AVAILABLE);
      testDisk.onDiskError();
    }
    assertEquals(testDisk.getState(), HardwareState.UNAVAILABLE);
    Thread.sleep(retryBackoffMs + 1);
    assertEquals(testDisk.getState(), HardwareState.AVAILABLE);
    //A single error should make it unavailable
    testDisk.onDiskError();
    assertEquals(testDisk.getState(), HardwareState.UNAVAILABLE);
    testDisk.onDiskOk();
    assertEquals(testDisk.getState(), HardwareState.AVAILABLE);
  }
}
