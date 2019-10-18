/*
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

package com.github.ambry.account;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


/**
 * HolderHelixZkClient is a holder to a {@link ZkClient}. Set the zkClient through {@link HolderHelixZkClient#setZkClient(ZkClient)}
 * when the {@link ZkClient} is ready. When it's not ready, invoking every method would throw a {@link NullPointerException}.
 */
public class HolderHelixZkClient implements HelixZkClient {
  private AtomicReference<ZkClient> zkClient = new AtomicReference<>(null);

  private HelixZkClient getZkClient() {
    HelixZkClient client = zkClient.get();
    if (client == null) {
      throw new NullPointerException("zkClient is not ready yet");
    }
    return client;
  }

  /**
   * Set the {@link ZkClient} for this reference holder. Only works when it was never set before.
   * @param zkClient The {@link ZkClient} to set.
   */
  public void setZkClient(ZkClient zkClient) {
    this.zkClient.compareAndSet(null, zkClient);
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    return getZkClient().subscribeChildChanges(path, listener);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    getZkClient().unsubscribeChildChanges(path, listener);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    getZkClient().subscribeDataChanges(path, listener);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    getZkClient().unsubscribeDataChanges(path, listener);
  }

  @Override
  public void subscribeStateChanges(IZkStateListener listener) {
    getZkClient().subscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeStateChanges(IZkStateListener listener) {
    getZkClient().unsubscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeAll() {
    getZkClient().unsubscribeAll();
  }

  @Override
  public void createPersistent(String path) {
    getZkClient().createPersistent(path);
  }

  @Override
  public void createPersistent(String path, boolean createParents) {
    getZkClient().createPersistent(path, createParents);
  }

  @Override
  public void createPersistent(String path, boolean createParents, List<ACL> acl) {
    getZkClient().createPersistent(path, createParents, acl);
  }

  @Override
  public void createPersistent(String path, Object data) {
    getZkClient().createPersistent(path, data);
  }

  @Override
  public void createPersistent(String path, Object data, List<ACL> acl) {
    getZkClient().createPersistent(path, data, acl);
  }

  @Override
  public String createPersistentSequential(String path, Object data) {
    return getZkClient().createPersistentSequential(path, data);
  }

  @Override
  public String createPersistentSequential(String path, Object data, List<ACL> acl) {
    return getZkClient().createPersistentSequential(path, data, acl);
  }

  @Override
  public void createEphemeral(String path) {
    getZkClient().createEphemeral(path);
  }

  @Override
  public void createEphemeral(String path, List<ACL> acl) {
    getZkClient().createEphemeral(path, acl);
  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    return getZkClient().create(path, data, mode);
  }

  @Override
  public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
    return getZkClient().create(path, datat, acl, mode);
  }

  @Override
  public void createEphemeral(String path, Object data) {
    getZkClient().createEphemeral(path, data);
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl) {
    getZkClient().createEphemeral(path, data, acl);
  }

  @Override
  public String createEphemeralSequential(String path, Object data) {
    return getZkClient().createEphemeralSequential(path, data);
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
    return getZkClient().createEphemeralSequential(path, data, acl);
  }

  @Override
  public List<String> getChildren(String path) {
    return getZkClient().getChildren(path);
  }

  @Override
  public int countChildren(String path) {
    return getZkClient().countChildren(path);
  }

  @Override
  public boolean exists(String path) {
    return getZkClient().exists(path);
  }

  @Override
  public Stat getStat(String path) {
    return getZkClient().getStat(path);
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    return getZkClient().waitUntilExists(path, timeUnit, time);
  }

  @Override
  public void deleteRecursively(String path) {
    getZkClient().deleteRecursively(path);
  }

  @Override
  public boolean delete(String path) {
    return getZkClient().delete(path);
  }

  @Override
  public <T> T readData(String path) {
    return getZkClient().readData(path);
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    return getZkClient().readData(path, returnNullIfPathNotExists);
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    return getZkClient().readData(path, stat);
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    return getZkClient().readData(path, stat, watch);
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    return getZkClient().readDataAndStat(path, stat, returnNullIfPathNotExists);
  }

  @Override
  public void writeData(String path, Object object) {
    getZkClient().writeData(path, object);
  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
    getZkClient().updateDataSerialized(path, updater);
  }

  @Override
  public void writeData(String path, Object datat, int expectedVersion) {
    getZkClient().writeData(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
    return getZkClient().writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
    return getZkClient().writeDataGetStat(path, datat, expectedVersion);
  }

  @Override
  public void asyncCreate(String path, Object datat, CreateMode mode, ZkAsyncCallbacks.CreateCallbackHandler cb) {
    getZkClient().asyncCreate(path, datat, mode, cb);
  }

  @Override
  public void asyncSetData(String path, Object datat, int version, ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    getZkClient().asyncSetData(path, datat, version, cb);
  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    getZkClient().asyncGetData(path, cb);
  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    getZkClient().asyncExists(path, cb);
  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    getZkClient().asyncDelete(path, cb);
  }

  @Override
  public void watchForData(String path) {
    getZkClient().watchForData(path);
  }

  @Override
  public List<String> watchForChilds(String path) {
    return getZkClient().watchForChilds(path);
  }

  @Override
  public long getCreationTime(String path) {
    return getZkClient().getCreationTime(path);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    return getZkClient().multi(ops);
  }

  @Override
  public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    return getZkClient().waitUntilConnected(time, timeUnit);
  }

  @Override
  public String getServers() {
    return getZkClient().getServers();
  }

  @Override
  public long getSessionId() {
    return getZkClient().getSessionId();
  }

  @Override
  public void close() {
    getZkClient().close();
  }

  @Override
  public boolean isClosed() {
    return getZkClient().isClosed();
  }

  @Override
  public byte[] serialize(Object data, String path) {
    return getZkClient().serialize(data, path);
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
    return getZkClient().deserialize(data, path);
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {
    getZkClient().setZkSerializer(zkSerializer);
  }

  @Override
  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    getZkClient().setZkSerializer(zkSerializer);
  }

  @Override
  public PathBasedZkSerializer getZkSerializer() {
    return getZkClient().getZkSerializer();
  }
}

