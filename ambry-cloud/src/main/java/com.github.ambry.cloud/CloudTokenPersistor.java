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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.FindTokenFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.replication.RemoteReplicaInfo.*;


/**
 * {@link CloudTokenPersistor} persists replication token to a cloud storage.
 */
public class CloudTokenPersistor extends ReplicaTokenPersistor {

  private static final Logger logger = LoggerFactory.getLogger(CloudTokenPersistor.class);
  private final String replicaTokenFileName;
  private final CloudDestination cloudDestination;

  /**
   * Constructor for {@link CloudTokenPersistor}.
   * @param replicaTokenFileName the token's file name.
   * @param partitionGroupedByMountPath A map between mount path and list of partitions under this mount path.
   * @param replicationMetrics metrics including token persist time.
   * @param clusterMap the {@link ClusterMap} to deserialize tokens.
   * @param tokenfactory the {@link FindTokenFactory} to deserialize tokens.
   */
  public CloudTokenPersistor(String replicaTokenFileName, Map<String, List<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenFactory tokenfactory,
      CloudDestination cloudDestination) {
    super(partitionGroupedByMountPath, replicationMetrics, clusterMap, tokenfactory);
    this.replicaTokenFileName = replicaTokenFileName;
    this.cloudDestination = cloudDestination;
  }

  // Note: assuming that passed mountPath is the partitionId path

  @Override
  protected void persist(String mountPath, List<ReplicaTokenInfo> tokenInfoList)
      throws IOException, ReplicationException {
    try {
      ByteArrayOutputStream tokenOutputStream = new ByteArrayOutputStream(4096);
      replicaTokenSerde.serializeTokens(tokenInfoList, tokenOutputStream);

      InputStream inputStream = new ByteArrayInputStream(tokenOutputStream.toByteArray());
      cloudDestination.persistTokens(mountPath, replicaTokenFileName, inputStream);
      logger.debug("Completed writing replica tokens to cloud destination.");
    } catch (CloudStorageException e) {
      throw new ReplicationException("IO error persisting replica tokens at mount path " + mountPath, e);
    }
  }

  @Override
  public List<ReplicaTokenInfo> retrieve(String mountPath) throws ReplicationException {
    try {
      ByteArrayOutputStream tokenOutputStream = new ByteArrayOutputStream(4096);
      boolean tokenExists = cloudDestination.retrieveTokens(mountPath, replicaTokenFileName, tokenOutputStream);
      if (tokenExists) {
        InputStream inputStream = new ByteArrayInputStream(tokenOutputStream.toByteArray());
        return replicaTokenSerde.deserializeTokens(inputStream);
      } else {
        return Collections.emptyList();
      }
    } catch (IOException | CloudStorageException e) {
      throw new ReplicationException("IO error while reading from replica token file at mount path " + mountPath, e);
    }
  }
}
