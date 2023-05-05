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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.VcrServer;
import com.github.ambry.cloud.VcrTestUtil;
import com.github.ambry.clustermap.DataNodeConfigSourceType;
import com.github.ambry.clustermap.HelixAdminFactory;
import com.github.ambry.clustermap.HelixBootstrapUpgradeUtil;
import com.github.ambry.clustermap.HelixClusterAgentsFactory;
import com.github.ambry.clustermap.HelixVcrUtil;

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;

import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.server.VcrBackupTest.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.mockito.Mockito.*;


public class VcrAutomationTest {
  private static final Logger logger = LoggerFactory.getLogger(VcrAutomationTest.class);

  /**
   * Constructor for {@link VcrAutomationTest}.
   */
  public VcrAutomationTest() {
  }

  @Before
  public void setup() throws Exception {

  }

  @After
  public void cleanup() throws IOException {

  }

  /**
   * Test basic partition add and remove cases.
   */
  @Test
  public void testSimpleVcrAutomation() throws Exception {
    List<ZkInfo> zkInfoList = new ArrayList<>();
    String mainClusterStateModelDef = ClusterMapConfig.AMBRY_STATE_MODEL_DEF;
    DataNodeConfigSourceType dataNodeConfigSourceType = DataNodeConfigSourceType.INSTANCE_CONFIG;
    int zkPort = 2100;
    int numberOfDataNode = 3;
    int partitionCount = 9;
    int newPartitionCount = 13;

    String zkHostName = "localhost";
    String zkConnectString = zkHostName + ":" + zkPort;
    String clusterPrefix = "";
    String clusterName = "MainCluster";
    String vcrClusterName = "VcrCluster";
    String dcName = "DC0";
    TestHardwareLayout testHardwareLayout;
    TestPartitionLayout testPartitionLayout;
    String hardwareLayoutPath;
    String partitionLayoutPath;
    String zkLayoutPath;

    zkInfoList.add(new ZkInfo(TestUtils.getTempDir("tempZk"), "DC0", (byte) 0, zkPort, true));
    String tempDirPath = getTempDir(clusterName + "-");
    hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    zkLayoutPath = tempDirPath + "/zkLayoutPath.json";

    testHardwareLayout = new TestHardwareLayout(clusterName, 1, 10737418240L, numberOfDataNode, 1, 18088, 20, false);
    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, partitionCount, null);
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    Properties props =
        VcrTestUtil.createVcrProperties("DC0", vcrClusterName, zkConnectString, 12300, 12400, 12510, null);
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1100");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dcName);
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.state.model.definition", mainClusterStateModelDef);
    props.setProperty("clustermap.data.node.config.source.type", dataNodeConfigSourceType.name());
    props.setProperty("vcr.helix.updater.partition.id", "1");
    props.setProperty("vcr.helix.update.delay.time.in.seconds", "1");

    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterPrefix,
        dcName, 10, false, false, new HelixAdminFactory(), false, mainClusterStateModelDef,
        HelixBootstrapUpgradeUtil.HelixAdminOperation.BootstrapCluster, dataNodeConfigSourceType, false, 0);

    HelixControllerManager helixControllerManager =
        new HelixControllerManager(zkConnectString, clusterPrefix + clusterName);
    helixControllerManager.syncStart();

    // Main cluster helix setup done.

    HelixVcrUtil.VcrHelixConfig vcrHelixConfig;
    String vcConfigData = CloudConfig.DEFAULT_VCR_HELIX_UPDATE_CONFIG;
    try (InputStream input = new ByteArrayInputStream(vcConfigData.getBytes())) {
      vcrHelixConfig = new ObjectMapper().readValue(input, HelixVcrUtil.VcrHelixConfig.class);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load config from config data: " + vcConfigData);
    }

    HelixVcrUtil.createCluster(zkConnectString, vcrClusterName, vcrHelixConfig);

    HelixVcrUtil.updateResourceAndPartition(zkConnectString, clusterPrefix + clusterName, zkConnectString,
        vcrClusterName, vcrHelixConfig, false);
    Assert.assertTrue("Dest and Src should be same",
        isSrcDestSync(zkConnectString, clusterPrefix + clusterName, zkConnectString, vcrClusterName));

    HelixControllerManager vcrHelixControllerManager = new HelixControllerManager(zkConnectString, vcrClusterName);
    vcrHelixControllerManager.syncStart();

    StrictMatchExternalViewVerifier helixBalanceVerifier =
        new StrictMatchExternalViewVerifier(zkConnectString, vcrClusterName,
            Collections.singleton(VcrTestUtil.helixResource), null);

    // VCR cluster helix setup done.

    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    HelixClusterAgentsFactory helixClusterAgentsFactory = new HelixClusterAgentsFactory(clusterMapConfig, null, null);
    VcrServer vcrServer =
        new VcrServer(verifiableProperties, helixClusterAgentsFactory, null, new CloudDestinationFactory() {
          @Override
          public CloudDestination getCloudDestination() throws IllegalStateException {
            return mock(CloudDestination.class);
          }
        }, null);
    vcrServer.startup();

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    Assert.assertTrue("Partition assignment is not correct.", TestUtils.checkAndSleep(partitionCount,
        () -> vcrServer.getVcrClusterParticipant().getAssignedPartitionIds().size(), 5000));
    // vcr server start up done.

    // Partition add case:
    testPartitionLayout =
        constructInitialPartitionLayoutJSON(testHardwareLayout, partitionCount + newPartitionCount, null);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterPrefix,
        dcName, 10, false, false, new HelixAdminFactory(), false, mainClusterStateModelDef,
        HelixBootstrapUpgradeUtil.HelixAdminOperation.BootstrapCluster, dataNodeConfigSourceType, false, 0);

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    Assert.assertTrue("Partition assignment is not correct.",
        TestUtils.checkAndSleep(partitionCount + newPartitionCount,
            () -> vcrServer.getVcrClusterParticipant().getAssignedPartitionIds().size(), 5000));

    // Partition remove case:

    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, partitionCount, null);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterPrefix,
        dcName, 10, false, true, new HelixAdminFactory(), false, mainClusterStateModelDef,
        HelixBootstrapUpgradeUtil.HelixAdminOperation.BootstrapCluster, dataNodeConfigSourceType, false, 0);

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    Assert.assertTrue("Partition assignment is not correct.", TestUtils.checkAndSleep(partitionCount,
        () -> vcrServer.getVcrClusterParticipant().getAssignedPartitionIds().size(), 5000));

    helixControllerManager.syncStop();
    vcrHelixControllerManager.syncStop();
    zkInfoList.get(0).shutdown();
  }
}
