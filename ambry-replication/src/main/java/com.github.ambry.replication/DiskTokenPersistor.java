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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.FindTokenFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.replication.RemoteReplicaInfo.*;


/**
 * {@link DiskTokenPersistor} persists replication token to disk.
 */
public class DiskTokenPersistor extends ReplicaTokenPersistor {

  private static final Logger logger = LoggerFactory.getLogger(DiskTokenPersistor.class);
  private final String replicaTokenFileName;

  /**
   * Constructor for {@link DiskTokenPersistor}.
   * @param replicaTokenFileName the token's file name.
   * @param partitionGroupedByMountPath A map between mount path and list of partitions under this mount path.
   * @param replicationMetrics metrics including token persist time.
   * @param clusterMap the {@link ClusterMap} to deserialize tokens.
   * @param tokenfactory the {@link FindTokenFactory} to deserialize tokens.
   */
  public DiskTokenPersistor(String replicaTokenFileName, Map<String, List<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenFactory tokenfactory) {
    super(partitionGroupedByMountPath, replicationMetrics, clusterMap, tokenfactory);
    this.replicaTokenFileName = replicaTokenFileName;
  }

  @Override
  protected void persistTokens(String mountPath, List<ReplicaTokenInfo> tokenInfoList) throws IOException {
    File temp = new File(mountPath, replicaTokenFileName + ".tmp");
    File actual = new File(mountPath, replicaTokenFileName);
    try (FileOutputStream fileStream = new FileOutputStream(temp)) {
      replicaTokenSerde.serializeTokens(tokenInfoList, fileStream);

      // swap temp file with the original file
      temp.renameTo(actual);
      logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());
    } catch (IOException e) {
      logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
      throw e;
    }
  }

  @Override
  public List<ReplicaTokenInfo> retrieveTokens(String mountPath) throws IOException, ReplicationException {
    File replicaTokenFile = new File(mountPath, replicaTokenFileName);
    if (replicaTokenFile.exists()) {
      try (FileInputStream fileInputStream = new FileInputStream(replicaTokenFile)) {
        return replicaTokenSerde.deserializeTokens(fileInputStream);
      } catch (IOException e) {
        throw new ReplicationException("IO error while reading from replica token file at mount path " + mountPath, e);
      }
    } else {
      return Collections.emptyList();
    }
  }
}

