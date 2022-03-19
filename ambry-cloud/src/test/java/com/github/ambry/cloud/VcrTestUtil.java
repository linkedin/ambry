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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;


/**
 * Utility class for VCR tests.
 */
public class VcrTestUtil {

  public static final String helixResource = "resource1";
  private static final int MIN_ACTIVE_REPLICAS = 0;
  private static final long REBALANCE_DELAY_MS = TimeUnit.SECONDS.toMillis(2);
  private static final int NUM_REPLICAS = 1;

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
    return new VcrServer(properties, clusterAgentsFactory, notificationSystem, cloudDestinationFactory, null);
  }

  /**
   * Populate info on ZooKeeper server and start {@link HelixControllerManager}.
   * @param zkConnectString zk connect string to zk server.
   * @param vcrClusterName the vcr cluster name.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the created {@link HelixControllerManager}.
   */
  public static HelixControllerManager populateZkInfoAndStartController(String zkConnectString, String vcrClusterName,
      ClusterMap clusterMap) {
    return populateZkInfoAndStartController(zkConnectString, vcrClusterName, clusterMap, OnlineOfflineSMD.name);
  }

  /**
   * Populate info on ZooKeeper server and start {@link HelixControllerManager}.
   * @param zkConnectString zk connect string to zk server.
   * @param vcrClusterName the vcr cluster name.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param vcrHelixStateModel the state model to use for helix cluster events.
   * @return the created {@link HelixControllerManager}.
   */
  public static HelixControllerManager populateZkInfoAndStartController(String zkConnectString, String vcrClusterName,
      ClusterMap clusterMap, String vcrHelixStateModel) {
    HelixZkClient zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkConnectString), new HelixZkClient.ZkClientConfig());
    try {
      zkClient.setZkSerializer(new ZNRecordSerializer());
      ClusterSetup clusterSetup = new ClusterSetup(zkClient);
      clusterSetup.addCluster(vcrClusterName, true);
      HelixAdmin admin = new ZKHelixAdmin(zkClient);
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

      FullAutoModeISBuilder builder = new FullAutoModeISBuilder(helixResource);
      builder.setStateModel(vcrHelixStateModel);
      for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
        builder.add(partitionId.toPathString());
      }
      builder.setMinActiveReplica(MIN_ACTIVE_REPLICAS);
      builder.setRebalanceDelay((int) REBALANCE_DELAY_MS);
      builder.setRebalancerClass(DelayedAutoRebalancer.class.getName());
      builder.setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
      IdealState idealState = builder.build();
      admin.addResource(vcrClusterName, helixResource, idealState);
      admin.rebalance(vcrClusterName, helixResource, NUM_REPLICAS, "", "");
      HelixControllerManager helixControllerManager = new HelixControllerManager(zkConnectString, vcrClusterName);
      helixControllerManager.syncStart();
      return helixControllerManager;
    } finally {
      zkClient.close();
    }
  }

  /**
   * Create a {@link Properties} for VCR.
   * @param datacenter the datacenter to use.
   * @param vcrClusterName the vcrClusterName to use.
   * @param zkConnectString the zkConnectString to use.
   * @param clusterMapPort the clusterMapPort to use.
   * @param vcrSslPort the vcrSslPort to use.
   * @param vcrHttp2Port the vcrHttp2Port to use.
   * @param vcrSSLProps the SSL Properties to use if exist. Can be {@code null}.
   * @return the created VCR {@link Properties}.
   */
  public static Properties createVcrProperties(String datacenter, String vcrClusterName, String zkConnectString,
      int clusterMapPort, int vcrSslPort, int vcrHttp2Port, Properties vcrSSLProps) {
    return createVcrProperties(datacenter, vcrClusterName, zkConnectString, clusterMapPort, vcrSslPort, vcrHttp2Port,
        vcrSSLProps, OnlineOfflineHelixVcrStateModelFactory.class.getName(), false);
  }

  /**
   * Create a {@link Properties} for VCR.
   * @param datacenter the datacenter to use.
   * @param vcrClusterName the vcrClusterName to use.
   * @param zkConnectString the zkConnectString to use.
   * @param clusterMapPort the clusterMapPort to use.
   * @param vcrSslPort the vcrSslPort to use.
   * @param vcrHttp2Port the vcrHttp2Port to use.
   * @param vcrSSLProps the SSL Properties to use if exist. Can be {@code null}.
   * @param vcrHelixStateModelFactoryClass the state model factory class.
   * @param enableHttp2Replication enable http2 replication or not.
   * @return the created VCR {@link Properties}.
   */
  public static Properties createVcrProperties(String datacenter, String vcrClusterName, String zkConnectString,
      int clusterMapPort, int vcrSslPort, int vcrHttp2Port, Properties vcrSSLProps,
      String vcrHelixStateModelFactoryClass, boolean enableHttp2Replication) {
    // Start the VCR and CloudBackupManager
    Properties props = new Properties();
    props.setProperty(CloudConfig.CLOUD_IS_VCR, Boolean.TRUE.toString());
    props.setProperty("connectionpool.read.timeout.ms", "15000");
    props.setProperty("server.scheduler.num.of.threads", "1");
    props.setProperty("num.io.threads", "1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "thisIsClusterName");
    props.setProperty("clustermap.datacenter.name", datacenter);
    props.setProperty("vcr.source.datacenters", datacenter);
    props.setProperty("clustermap.port", Integer.toString(clusterMapPort));
    props.setProperty("port", Integer.toString(clusterMapPort));
    props.setProperty("vcr.helix.state.model.factory.class", vcrHelixStateModelFactoryClass);
    props.setProperty("server.security.service.factory", "com.github.ambry.cloud.AmbryVcrSecurityServiceFactory");
    if (vcrSSLProps == null) {
      props.setProperty("clustermap.ssl.enabled.datacenters", "");
    } else {
      props.putAll(vcrSSLProps);
      props.setProperty("clustermap.ssl.enabled.datacenters", datacenter);
      props.setProperty(CloudConfig.VCR_SSL_PORT, Integer.toString(vcrSslPort));
      props.setProperty(CloudConfig.VCR_HTTP2_PORT, Integer.toString(vcrHttp2Port));
    }
    props.setProperty(CloudConfig.VCR_CLUSTER_NAME, vcrClusterName);
    props.setProperty(CloudConfig.VCR_CLUSTER_AGENTS_FACTORY_CLASS, HelixVcrClusterAgentsFactory.class.getName());
    props.setProperty(CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING, zkConnectString);
    props.setProperty(CloudConfig.KMS_SERVICE_KEY_CONTEXT, TestUtils.getRandomKey(32));
    props.setProperty("kms.default.container.key", TestUtils.getRandomKey(16));
    props.setProperty("replication.token.flush.delay.seconds", "100000");
    props.setProperty("replication.token.flush.interval.seconds", "500000");
    props.setProperty("clustermap.enable.http2.replication", Boolean.toString(enableHttp2Replication));
    return props;
  }
}
