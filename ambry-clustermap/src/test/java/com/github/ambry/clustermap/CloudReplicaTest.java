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
package com.github.ambry.clustermap;

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.File;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class testing behavior of {@link CloudReplica}.
 */
public class CloudReplicaTest {

  private final ClusterMapConfig clusterMapConfig;
  private final CloudConfig cloudConfig;

  public CloudReplicaTest() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC0,DC1");
    props.setProperty("clustermap.port", "12300");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", "12301");
    props.setProperty("vcr.cluster.name", "VCRCluster");
    cloudConfig = new CloudConfig(new VerifiableProperties(props));
  }

  /** Test the CloudReplica constructor and methods */
  @Test
  public void basicTest() {
    MockPartitionId mockPartitionId = new MockPartitionId();
    CloudDataNode cloudDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    CloudReplica cloudReplica = new CloudReplica(new MockPartitionId(), cloudDataNode);
    assertEquals("Wrong mount path", mockPartitionId.toPathString(), cloudReplica.getMountPath());
    assertEquals("Wrong replica path",
        CloudReplica.Cloud_Replica_Keyword + File.separator + mockPartitionId.toPathString() + File.separator
            + mockPartitionId.toPathString(), cloudReplica.getReplicaPath());
    assertEquals("Wrong dataNodeId", cloudDataNode, cloudReplica.getDataNodeId());
    assertEquals("Wrong partitionId", mockPartitionId, cloudReplica.getPartitionId());
    assertEquals("Wrong peer replicaIds", mockPartitionId.getReplicaIds(), cloudReplica.getPeerReplicaIds());
    String snapshot = cloudReplica.getSnapshot().toString();
    assertTrue("Expected hostname in snapshot", snapshot.contains(cloudReplica.getDataNodeId().getHostname()));
    assertTrue("Expected partitionId in snapshot", snapshot.contains(cloudReplica.getPartitionId().toPathString()));
  }
}
