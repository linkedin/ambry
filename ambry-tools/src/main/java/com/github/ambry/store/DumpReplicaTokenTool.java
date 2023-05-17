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
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping data from replica token file in ambry
 *
 * 1. Put this in a file called ambry/target/dump_replica.props and change the first three to your filesystem paths.
 *
 *    file.to.read=/Users/snalli/projects/ambry/target/replicaTokens
 *    hardware.layout.file.path=/Users/snalli/projects/ambry/target/HardwareLayoutVideo.json
 *    partition.layout.file.path=/Users/snalli/projects/ambry/target/PartitionLayoutVideo.json
 *    clustermap.resolve.hostnames=false
 *    hostname=localhost
 *    clustermap.cluster.name=ambry
 *    clustermap.datacenter.name=prod-lva1
 *    port=15088
 *    clustermap.host.name=localhost
 *    replication.token.factory=com.github.ambry.store.StoreFindTokenFactory
 *    store.key.factory=com.github.ambry.commons.BlobIdFactory
 *
 * 2. Compile ambry open-source.
 *
 *    ./gradlew clean && ./gradlew allJar
 *
 * 3. Switch to target/ folder
 *
 * 4. Run this cmd
 *
 *    java -cp ambry.jar -Dlog4j2.configurationFile=file:../config/log4j2.xml com.github.ambry.store.DumpReplicaTokenTool --propsFile ./dump_replica.props
 */
public class DumpReplicaTokenTool {

  private final ClusterMap clusterMap;
  // Refers to replicatoken file that needs to be dumped
  private final String fileToRead;

  private static final Logger logger = LoggerFactory.getLogger(DumpReplicaTokenTool.class);
  protected ReplicaTokenPersistor.ReplicaTokenSerde replicaTokenSerde;

  public DumpReplicaTokenTool(VerifiableProperties verifiableProperties) throws Exception {
    fileToRead = verifiableProperties.getString("file.to.read");
    String hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    String partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");

    if (!new File(hardwareLayoutFilePath).exists() || !new File(partitionLayoutFilePath).exists()) {
      throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
    }
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    this.clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            hardwareLayoutFilePath, partitionLayoutFilePath)).getClusterMap();

    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    StoreKeyFactory storeKeyFactory = Utils.getObj(verifiableProperties.getString("store.key.factory"), clusterMap);
    try {
      FindTokenHelper findTokenHelper = new FindTokenHelper(storeKeyFactory, replicationConfig);
      this.replicaTokenSerde = new ReplicaTokenPersistor.ReplicaTokenSerde(clusterMap, findTokenHelper);
    } catch (ReflectiveOperationException e) {
      throw new ReplicationException("Failed to get FindTokenHelper " + e);
    }
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    DumpReplicaTokenTool dumpReplicaTokenTool = new DumpReplicaTokenTool(verifiableProperties);
    dumpReplicaTokenTool.dumpReplicaToken();
  }

  /**
   * Dumps replica token file
   * @throws Exception
   */
  private void dumpReplicaToken() throws Exception {
    logger.info("Dumping replica token file {}", fileToRead);
    DataInputStream stream = new DataInputStream(new FileInputStream(fileToRead));
    try {
      List<RemoteReplicaInfo.ReplicaTokenInfo> replicaTokenInfoList = replicaTokenSerde.deserializeTokens(stream);
      for (RemoteReplicaInfo.ReplicaTokenInfo replicaTokenInfo : replicaTokenInfoList) {
        logger.info(replicaTokenInfo.toString() + " " + replicaTokenInfo.getReplicaToken().toString());
      }
    } catch (IOException e) {
      throw new ReplicationException("IO error while reading from replica token file " + fileToRead, e);
    }
  }
}
