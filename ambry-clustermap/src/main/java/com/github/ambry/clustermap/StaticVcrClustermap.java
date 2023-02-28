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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to parse and store static vcr clustermap, useful for debugging vcr node issues.
 * {
 *   datacenter: [
 *    {
 *      datacenter_name: "ei4",
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
public class StaticVcrClustermap {
  public static final String HOSTNAME = "hostname";
  public static final String PLAINTEXT_PORT = "plaintext_port";
  public static final String SSL_PORT = "ssl_port";
  public static final String HTTP2_PORT = "http2_port";
  public static final String DATACENTER = "datacenter";
  public static final String DATACENTER_NAME = "datacenter_name";
  public static final String VCR_NODES = "vcr_nodes";
  private static final Logger logger = LoggerFactory.getLogger(StaticVcrClustermap.class);
  protected List<CloudDataNode> cloudDataNodes;

  public StaticVcrClustermap(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) {
    this.cloudDataNodes = null;
    JSONArray datacenters = jsonObject.getJSONArray(DATACENTER);
    for (int i = 0; i < datacenters.length(); ++i) {
      String datacenter = jsonObject.getString(DATACENTER_NAME);
      JSONArray vcrNodes = jsonObject.getJSONArray(VCR_NODES);
      for (int j = 0; j < vcrNodes.length(); ++j) {
        JSONObject vcrNode = vcrNodes.getJSONObject(j);
        CloudDataNode cloudDataNode = new CloudDataNode(vcrNode.getString(HOSTNAME),
            new Port(vcrNode.getInt(PLAINTEXT_PORT), PortType.PLAINTEXT),
            new Port(vcrNode.getInt(SSL_PORT), PortType.SSL),
            new Port(vcrNode.getInt(HTTP2_PORT), PortType.HTTP2),
            datacenter, clusterMapConfig);
        cloudDataNodes.add(cloudDataNode);
        logger.info("Read vcr node info {}", cloudDataNode);
      }
      logger.info("Done reading static vcr clustermap");
    }
  }

  /**
   * Returns cloud data nodes (vcr nodes)
   * @return Returns cloud data nodes (vcr nodes)
   */
  public List<CloudDataNode> getCloudDataNodes() {
    return cloudDataNodes;
  }

}
