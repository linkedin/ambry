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
package com.github.ambry.server;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * The multi node single partition test needs to run with clean stores as it checks replication token offset values
 * so it has been put into a separate class with per-test initialization and cleanup
 */
public class ServerPlaintextTokenTest {
  private Properties routerProps;
  private MockNotificationSystem notificationSystem;
  private MockCluster plaintextCluster;

  @Before
  public void initializeTests() throws Exception {
    routerProps = new Properties();
    notificationSystem = new MockNotificationSystem(9);
    plaintextCluster = new MockCluster(notificationSystem, false, SystemTime.getInstance());
    plaintextCluster.startServers();
  }

  @After
  public void cleanup() throws IOException {
    long start = System.currentTimeMillis();
    System.out.println("About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void endToEndReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNodeId = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = Utils.splitString("DC1,DC2,DC3", ",");
    List<DataNodeId> dataNodes = plaintextCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeSinglePartitionTest("DC1", "", dataNodeId.getPort(),
        new Port(dataNodes.get(0).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(1).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(2).getPort(), PortType.PLAINTEXT), plaintextCluster, null, null, notificationSystem,
        routerProps);
  }
}
