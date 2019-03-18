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
package com.github.ambry.config;

/**
 * The configs for cloud related configurations.
 */
public class CloudConfig {

  @Config("vcr.cluster.zk.connect.string")
  @Default("")
  public final String vcrClusterZkConnectString;

  /**
   * The name of the associated vcr cluster for this node.
   */
  @Config("vcr.cluster.name")
  public final String vcrClusterName;

  /**
   * The ssl port number associated with this node.
   */
  @Config("vcr.ssl.port")
  @Default("null")
  public final Integer vcrSslPort;

  /**
   * The ssl port number associated with this node.
   */
  @Config("vcr.replica.mount.path.prefix")
  @Default("/tmp/vcr/")
  public final String vcrReplicaMountPathPrefix;

  public CloudConfig(VerifiableProperties verifiableProperties) {
    vcrClusterZkConnectString = verifiableProperties.getString("vcr.cluster.zk.connect.string", "");
    vcrClusterName = verifiableProperties.getString("vcr.cluster.name");
    vcrSslPort = verifiableProperties.getInteger("vcr.ssl.port", null);
    vcrReplicaMountPathPrefix = verifiableProperties.getString("vcr.replica.mount.path.prefix", "/tmp/vcr/");
  }
}
