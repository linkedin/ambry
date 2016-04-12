/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


// TestDatacenter permits Datacenter to be constructed with a null HardwareLayout.
class TestDatacenter extends Datacenter {
  public TestDatacenter(JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    super(null, jsonObject, clusterMapConfig);
  }

  @Override
  public void validateHardwareLayout() {
    // Null OK.
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestDatacenter testDatacenter = (TestDatacenter) o;

    if (!getName().equals(testDatacenter.getName())) {
      return false;
    }
    return getRawCapacityInBytes() == testDatacenter.getRawCapacityInBytes();
  }
}

/**
 * Tests {@link Datacenter} class.
 */
public class DatacenterTest {
  private static final int diskCount = 10;
  private static final long diskCapacityInBytes = 1000 * 1024 * 1024 * 1024L;

  private static final int dataNodeCount = 6;

  JSONArray getDisks()
      throws JSONException {
    return TestUtils.getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
  }

  JSONArray getDataNodes()
      throws JSONException {
    return TestUtils.getJsonArrayDataNodes(dataNodeCount, TestUtils.getLocalHost(), 6666, 7666, HardwareState.AVAILABLE,
        getDisks());
  }

  @Test
  public void basics()
      throws JSONException {
    JSONObject jsonObject = TestUtils.getJsonDatacenter("XYZ1", getDataNodes());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));

    Datacenter datacenter = new TestDatacenter(jsonObject, clusterMapConfig);

    assertEquals(datacenter.getName(), "XYZ1");
    assertEquals(datacenter.getDataNodes().size(), dataNodeCount);
    assertEquals(datacenter.getRawCapacityInBytes(), dataNodeCount * diskCount * diskCapacityInBytes);
    assertEquals(datacenter.toJSONObject().toString(), jsonObject.toString());
    assertEquals(datacenter, new TestDatacenter(datacenter.toJSONObject(), clusterMapConfig));
  }

  public void failValidation(JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    try {
      new TestDatacenter(jsonObject, clusterMapConfig);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation()
      throws JSONException {
    JSONObject jsonObject;
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));

    try {
      // Null HardwareLayout
      jsonObject = TestUtils.getJsonDatacenter("XYZ1", getDataNodes());
      new Datacenter(null, jsonObject, clusterMapConfig);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    // Bad datacenter name
    jsonObject = TestUtils.getJsonDatacenter("", getDataNodes());
    failValidation(jsonObject, clusterMapConfig);
  }
}