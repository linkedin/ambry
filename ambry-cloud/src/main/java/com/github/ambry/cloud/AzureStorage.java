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

package com.github.ambry.cloud;

import com.azure.storage.blob.BlobContainerClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements methods to write {@link com.github.ambry.clustermap.AmbryPartition} in Azure blob storage.
 */
public class AzureStorage implements Store {

  private final ClusterMap clusterMap;
  private final AzureMetrics azureMetrics;
  private final CloudConfig cloudConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final Object vcrMetrics;
  private final BlobContainerClient blobContainerClient;
  private static final Logger logger = LoggerFactory.getLogger(AzureStorage.class);

  public AzureStorage(VerifiableProperties properties, MetricRegistry metricRegistry, ClusterMap clusterMap,
      BlobContainerClient blobContainerClient) {
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.cloudConfig = new CloudConfig(properties);
    this.clusterMap = clusterMap;
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.vcrMetrics = new VcrMetrics(metricRegistry);
    this.blobContainerClient = blobContainerClient;
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    // TODO
  }

  @Override
  public void delete(List<MessageInfo> messageInfos) throws StoreException {
    // TODO
  }

  @Override
  public short undelete(MessageInfo messageInfo) throws StoreException {
    // TODO
    return 0;
  }

  @Override
  public void updateTtl(List<MessageInfo> messageInfos) {
    // TODO
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> storeKeys) {
    // TODO
    return new HashSet<>();
  }

  @Override
  public MessageInfo findKey(StoreKey storeKey) {
    // TODO
    return null;
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public boolean isEmpty() {
    // TODO
    return false;
  }

  @Override
  public ReplicaState getCurrentState() {
    // TODO
    return null;
  }

  @Override
  public void setCurrentState(ReplicaState state) {
    // TODO
  }

  @Override
  public long getSizeInBytes() {
    // TODO
    return 0;
  }

  @Override
  public long getEndPositionOfLastPut() {
    // TODO
    return 0;
  }

  /////////////////////////////////////////// Unimplemented methods /////////////////////////////////////////////////

  @Override
  public void start() throws StoreException {

  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    return null;
  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) {

  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname, String remoteReplicaPath) throws StoreException {
    return null;
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isDisabled() {
    return false;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) {
    return false;
  }

  @Override
  public boolean isBootstrapInProgress() {
    return false;
  }

  @Override
  public boolean isDecommissionInProgress() {
    return false;
  }

  @Override
  public void completeBootstrap() {

  }

  @Override
  public boolean recoverFromDecommission() {
    return false;
  }

  @Override
  public void shutdown() throws StoreException {

  }
}
