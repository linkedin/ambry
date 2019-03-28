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
package com.github.ambry.server;

import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collections;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;


/**
 * Test of the VCR Server.
 */
public class VCRServerTest {

  private MockClusterAgentsFactory mockClusterAgentsFactory;
  private MockClusterMap mockClusterMap;
  private MockNotificationSystem notificationSystem;

  @Before
  public void setup() {

  }

  /**
   * Bring up the VCR server and then shut it down.
   * @throws Exception
   */
  @Test
  public void testVCRServer() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    notificationSystem = new MockNotificationSystem(mockClusterMap);
    Properties props = new Properties();
    props.setProperty("host.name", mockClusterMap.getDataNodes().get(0).getHostname());
    int port = mockClusterMap.getDataNodes().get(0).getPort();
    props.setProperty("port", Integer.toString(port));
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", Integer.toString(port));
    props.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    props.setProperty("server.scheduler.num.of.threads", "1");
    props.setProperty("num.io.threads", "1");
    props.setProperty("vcr.assigned.partitions", "0,1");
    props.setProperty("ssl.factory", MockSSLFactory.class.getName());
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(new LatchBasedInMemoryCloudDestination(Collections.emptyList()));
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    VCRServer vcrServer =
        new VCRServer(verifiableProperties, mockClusterAgentsFactory, notificationSystem, cloudDestinationFactory, null,
            null);
    vcrServer.startup();
    vcrServer.shutdown();
    mockClusterMap.cleanup();
  }
}
