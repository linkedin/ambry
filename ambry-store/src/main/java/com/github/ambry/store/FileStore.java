/*
 * *
 *  * Copyright 2024 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package com.github.ambry.store;

import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.replication.FindToken;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The FileStore which enables file copy for bootstrap in ambry.
 */
public class FileStore implements Store {

  @Override
  public void start() throws StoreException {

  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    return null;
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {

  }

  @Override
  public void delete(List<MessageInfo> infosToDelete) throws StoreException {

  }

  @Override
  public StoreBatchDeleteInfo batchDelete(List<MessageInfo> infosToDelete) throws StoreException {
    return null;
  }

  @Override
  public void purge(List<MessageInfo> infosToPurge) throws StoreException {

  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) throws StoreException {

  }

  @Override
  public short undelete(MessageInfo info) throws StoreException {
    return 0;
  }

  @Override
  public void updateTtl(List<MessageInfo> infosToUpdate) throws StoreException {

  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname,
      String remoteReplicaPath) throws StoreException {
    return null;
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    return null;
  }

  @Override
  public MessageInfo findKey(StoreKey key) throws StoreException {
    return null;
  }

  @Override
  public Map<String, MessageInfo> findAllMessageInfosForKey(StoreKey key) throws StoreException {
    return Store.super.findAllMessageInfosForKey(key);
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    return false;
  }

  @Override
  public long getSizeInBytes() {
    return 0;
  }

  @Override
  public long getEndPositionOfLastPut() throws StoreException {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean isStarted() {
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
  public void setCurrentState(ReplicaState state) {

  }

  @Override
  public ReplicaState getCurrentState() {
    return null;
  }

  @Override
  public boolean recoverFromDecommission() {
    return false;
  }

  @Override
  public boolean isDisabled() {
    return false;
  }

  @Override
  public void shutdown() throws StoreException {

  }

  @Override
  public Long getBlobContentCRC(MessageInfo msg) throws StoreException, IOException {
    return Store.super.getBlobContentCRC(msg);
  }
}
