/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.clustermap.VirtualReplicatorClusterFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test of StaticVcrCluster.
 */
public class StaticVcrClusterTest {
  private MockClusterAgentsFactory mockClusterAgentsFactory;
  private MockClusterMap mockClusterMap;

  @Before
  public void setup() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
  }

  @Test
  public void staticVcrClusterFactoryTest() throws Exception {
    Properties props = new Properties();
    String hostName = "localhostTest";
    int port = 12345;
    List<String> assignedPartitions = Arrays.asList("0", "1");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", hostName);
    props.setProperty("clustermap.port", Integer.toString(port));
    props.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("vcr.assigned.partitions", String.join(",", assignedPartitions));
    VerifiableProperties vProps = new VerifiableProperties(props);
    CloudConfig cloudConfig = new CloudConfig(vProps);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(vProps);
    VirtualReplicatorClusterFactory factory =
        new StaticVcrClusterFactory(cloudConfig, clusterMapConfig, mockClusterMap);
    VirtualReplicatorCluster virtualReplicatorCluster = factory.getVirtualReplicatorCluster();
    assertEquals("CloudDataNode host name doesn't match", hostName,
        virtualReplicatorCluster.getCurrentDataNodeId().getHostname());
    assertEquals("CloudDataNode port doesn't match", port, virtualReplicatorCluster.getCurrentDataNodeId().getPort());
    assertTrue("Partition assignment incorrect", assignedPartitions.equals(
        virtualReplicatorCluster.getAssignedPartitionIds()
            .stream()
            .map(partitionId -> partitionId.toPathString())
            .collect(Collectors.toList())));
    assertEquals("Number of CloudDataNode should be 1", 1, virtualReplicatorCluster.getAllDataNodeIds().size());
    assertEquals("CloudDataNode mismatch", virtualReplicatorCluster.getCurrentDataNodeId(),
        virtualReplicatorCluster.getAllDataNodeIds().get(0));
  }
}
