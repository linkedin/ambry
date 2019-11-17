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
import com.github.ambry.store.StorageManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.replication.RemoteReplicaInfo.*;


/**
 * {@link DiskTokenPersistor} persists replication token to disk.
 */
public class DiskTokenPersistor extends ReplicaTokenPersistor {

  private static final Logger logger = LoggerFactory.getLogger(DiskTokenPersistor.class);
  private final String replicaTokenFileName;
  private final StorageManager storageManager;

  /**
   * Constructor for {@link DiskTokenPersistor}.
   * @param replicaTokenFileName the token's file name.
   * @param partitionGroupedByMountPath A map between mount path and list of partitions under this mount path.
   * @param replicationMetrics metrics including token persist time.
   * @param clusterMap the {@link ClusterMap} to deserialize tokens.
   * @param tokenHelper the {@link FindTokenHelper} to deserialize tokens.
   * @param storageManager the {@link StorageManager} that manages disks and stores.
   */
  DiskTokenPersistor(String replicaTokenFileName, Map<String, Set<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenHelper tokenHelper,
      StorageManager storageManager) {
    super(partitionGroupedByMountPath, replicationMetrics, clusterMap, tokenHelper);
    this.replicaTokenFileName = replicaTokenFileName;
    this.storageManager = storageManager;
  }

  @Override
  protected void persist(String mountPath, List<ReplicaTokenInfo> tokenInfoList) throws IOException {
    File temp = new File(mountPath, replicaTokenFileName + ".tmp");
    File actual = new File(mountPath, replicaTokenFileName);
    try (FileOutputStream fileStream = new FileOutputStream(temp)) {
      replicaTokenSerde.serializeTokens(tokenInfoList, fileStream);

      // swap temp file with the original file
      temp.renameTo(actual);
      logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());
    } catch (IOException e) {
      logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
      // check disk state in storageManager. If there is a hardware issue, persistor should skip this bad disk next time.
      if (!storageManager.isDiskAvailableAtMountPath(mountPath)) {
        mountPathsToSkip.add(mountPath);
      }
      throw e;
    }
  }

  @Override
  public List<ReplicaTokenInfo> retrieve(String mountPath) throws ReplicationException {
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

