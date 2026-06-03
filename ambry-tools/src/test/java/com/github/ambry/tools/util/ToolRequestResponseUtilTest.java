/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.util;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ToolRequestResponseUtil#getReplicaFromNode}, focused on the {@code partitionId == null}
 * ("pick any replica on the node") path used by the partition-agnostic admin operations.
 */
public class ToolRequestResponseUtilTest {
  private MockClusterMap clusterMap;

  @Before
  public void setUp() throws Exception {
    clusterMap = new MockClusterMap();
  }

  @After
  public void tearDown() throws Exception {
    if (clusterMap != null) {
      clusterMap.cleanup();
    }
  }

  @Test
  public void testNullPartitionPicksAnyReplicaOnNode() {
    DataNodeId node = clusterMap.getDataNodeIds().get(0);
    ReplicaId replica = ToolRequestResponseUtil.getReplicaFromNode(node, null, clusterMap);
    assertNotNull("A node hosting replicas must resolve a replica when partitionId is null", replica);
    assertEquals(node, replica.getDataNodeId());
  }

  @Test
  public void testNullPartitionOnReplicaLessNodeThrowsClearError() throws Exception {
    // A freshly added node in a new DC hosts no replicas. Resolving "any replica" on it must fail with a clear
    // IllegalStateException, not a bare IndexOutOfBoundsException from a .get(0) on an empty list.
    MockDataNodeId spare = clusterMap.createNewDataNodes(1, "DC-spare").get(0);
    try {
      ToolRequestResponseUtil.getReplicaFromNode(spare, null, clusterMap);
      fail("Expected IllegalStateException for a replica-less node");
    } catch (IllegalStateException expected) {
      assertTrue("error should explain the node hosts no replicas, got: " + expected.getMessage(),
          expected.getMessage().contains("no replicas"));
    }
  }
}
