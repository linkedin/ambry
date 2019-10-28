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

import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.PortType;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class testing behavior of {@link CloudDataNode}.
 */
public class CloudDataNodeTest {

  private final ClusterMapConfig clusterMapConfig;
  private final CloudConfig cloudConfig;

  public CloudDataNodeTest() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC0,DC1");
    props.setProperty("clustermap.port", "12300");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", "12345");
    cloudConfig = new CloudConfig(new VerifiableProperties(props));
  }

  /** Test the CloudDataNode constructor and methods */
  @Test
  public void basicTest() {
    CloudDataNode cloudDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    assertEquals("Wrong hostname", clusterMapConfig.clusterMapHostName, cloudDataNode.getHostname());
    assertEquals("Wrong datacenter name", clusterMapConfig.clusterMapDatacenterName, cloudDataNode.getDatacenterName());
    assertEquals("Wrong port", clusterMapConfig.clusterMapPort.intValue(), cloudDataNode.getPort());
    assertEquals("Wrong ssl port", cloudConfig.vcrSslPort.intValue(), cloudDataNode.getSSLPort());
    assertEquals("Wrong connect to port", PortType.SSL, cloudDataNode.getPortToConnectTo().getPortType());
    assertTrue("Should have SSL port", cloudDataNode.hasSSLPort());
    String snapshot = cloudDataNode.getSnapshot().toString();
    assertTrue("Expected hostname in snapshot", snapshot.contains(clusterMapConfig.clusterMapHostName));
    assertTrue("Expected datacenter name in snapshot", snapshot.contains(clusterMapConfig.clusterMapDatacenterName));
  }
}
