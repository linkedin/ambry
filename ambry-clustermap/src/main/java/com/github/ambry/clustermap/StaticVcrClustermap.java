/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to parse and store static vcr clustermap, useful for debugging vcr node issues.
 * {
 *   datacenters: [
 *    {
 *      datacenter: "ei4",
 *      vcr_nodes: [
 *        {
 *          hostname: "..",
 *          plaintext_port: "..",
 *          ssl_port: "..",
 *          http2_port: ".."
 *        },
 *        {
 *          ...
 *        }
 *      ]
 *    }
 *   ]
 * }
 */
public class StaticVcrClustermap implements ClusterMap {
  public static final String HOSTNAME = "hostname";
  public static final String PLAINTEXT_PORT = "plaintext_port";
  public static final String SSL_PORT = "ssl_port";
  public static final String HTTP2_PORT = "http2_port";
  public static final String DATACENTERS = "datacenters";
  public static final String DATACENTER = "datacenter";
  public static final String VCR_NODES = "vcr_nodes";
  private static final Logger logger = LoggerFactory.getLogger(StaticVcrClustermap.class);
  protected List<CloudDataNode> cloudDataNodes;

  public StaticVcrClustermap(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) {
    this.cloudDataNodes = new ArrayList<>();
    JSONArray datacenters = jsonObject.getJSONArray(DATACENTERS);
    for (int i = 0; i < datacenters.length(); ++i) {
      JSONObject datacenter = datacenters.getJSONObject(i);
      String datacenterName = datacenter.getString(DATACENTER);
      JSONArray vcrNodes = datacenter.getJSONArray(VCR_NODES);
      for (int j = 0; j < vcrNodes.length(); ++j) {
        JSONObject vcrNode = vcrNodes.getJSONObject(j);
        CloudDataNode cloudDataNode = new CloudDataNode(vcrNode.getString(HOSTNAME),
            new Port(vcrNode.getInt(PLAINTEXT_PORT), PortType.PLAINTEXT),
            new Port(vcrNode.getInt(SSL_PORT), PortType.SSL),
            new Port(vcrNode.getInt(HTTP2_PORT), PortType.HTTP2),
            datacenterName, clusterMapConfig);
        cloudDataNodes.add(cloudDataNode);
        logger.info("Read vcr node info {}", cloudDataNode);
      }
      logger.info("Done reading static vcr cluster-map");
    }
  }

  /**
   * Returns cloud data nodes (vcr nodes)
   * @return Returns cloud data nodes (vcr nodes)
   */
  public List<CloudDataNode> getCloudDataNodes() {
    return cloudDataNodes;
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    return null;
  }

  @Override
  public PartitionId getPartitionIdByName(String partitionIdStr) {
    return null;
  }

  @Override
  public List<? extends PartitionId> getWritablePartitionIds(String partitionClass) {
    return null;
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    return null;
  }

  @Override
  public List<? extends PartitionId> getAllPartitionIds(String partitionClass) {
    return null;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return false;
  }

  @Override
  public byte getLocalDatacenterId() {
    return 0;
  }

  @Override
  public String getDatacenterName(byte id) {
    return null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return null;
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    return null;
  }

  @Override
  public List<? extends DataNodeId> getDataNodeIds() {
    return null;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return null;
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {

  }

  @Override
  public JSONObject getSnapshot() {
    return null;
  }

  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    return null;
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {

  }

  @Override
  public void close() {

  }
}
