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

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.HelixAdminFactory;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.HelixControllerManager;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;


/**
 * Utility class for VCR tests.
 */
public class VcrTestUtil {

  /**
   * Create a {@link VcrServer}.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param cloudDestinationFactory the {@link CloudDestinationFactory} to use.
   * @return the created VCR server.
   */
  public static VcrServer createVcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, CloudDestinationFactory cloudDestinationFactory) {
    return new VcrServer(properties, clusterAgentsFactory, notificationSystem, cloudDestinationFactory);
  }

  /**
   * Populate info on ZooKeeper server and start {@link HelixControllerManager}.
   * @param zKConnectString zk connect string to zk server.
   * @param vcrClusterName the vcr cluster name.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the created {@link HelixControllerManager}.
   */
  public static HelixControllerManager populateZkInfoAndStartController(String zKConnectString, String vcrClusterName,
      ClusterMap clusterMap) {
    HelixZkClient zkClient =
        SharedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(zKConnectString));
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterSetup clusterSetup = new ClusterSetup(zkClient);
    clusterSetup.addCluster(vcrClusterName, true);
    HelixAdmin admin = new HelixAdminFactory().getHelixAdmin(zKConnectString);
    // set ALLOW_PARTICIPANT_AUTO_JOIN
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(vcrClusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    // set PersistBestPossibleAssignment
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(vcrClusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    configAccessor.setClusterConfig(vcrClusterName, clusterConfig);

    String resourceName = "1";
    FullAutoModeISBuilder builder = new FullAutoModeISBuilder(resourceName);
    builder.setStateModel(LeaderStandbySMD.name);
    for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
      builder.add(partitionId.toPathString());
    }
    builder.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
    IdealState idealState = builder.build();
    admin.addResource(vcrClusterName, resourceName, idealState);
    admin.rebalance(vcrClusterName, resourceName, 3, "", "");
    HelixControllerManager helixControllerManager = new HelixControllerManager(zKConnectString, vcrClusterName);
    helixControllerManager.syncStart();
    return helixControllerManager;
  }
}
