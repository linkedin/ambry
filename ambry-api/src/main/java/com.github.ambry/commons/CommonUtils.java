/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.commons;

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class CommonUtils {
  /**
   * Create a instance of {@link HelixPropertyStore} based on given {@link HelixPropertyStoreConfig}.
   * @param zkServers the ZooKeeper server address.
   * @param propertyStoreConfig the config for {@link HelixPropertyStore}.
   * @param subscribedPaths a list of paths to which the PropertyStore subscribes.
   * @return the instance of {@link HelixPropertyStore}.
   */
  public static HelixPropertyStore<ZNRecord> createHelixPropertyStore(String zkServers,
      HelixPropertyStoreConfig propertyStoreConfig, List<String> subscribedPaths) {
    if (zkServers == null || zkServers.isEmpty() || propertyStoreConfig == null) {
      throw new IllegalArgumentException("Invalid arguments, cannot create HelixPropertyStore");
    }
    ZkClient zkClient = null;
    try {
      zkClient = new ZkClient(zkServers, propertyStoreConfig.zkClientSessionTimeoutMs,
          propertyStoreConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
    } catch (Exception e) {
    }
    final HelixZkClient helixZkClient = zkClient != null ? zkClient : new HolderHelixZkClient();
    if (zkClient == null) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            try {
              ZkClient client = new ZkClient(zkServers, propertyStoreConfig.zkClientSessionTimeoutMs,
                  propertyStoreConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
              ((HolderHelixZkClient) helixZkClient).setZkClient(client);
              return;
            } catch (Exception e) {
              try {
                Thread.sleep(10 * 60 * 1000);
              } catch (Exception e2) {
              }
            }
          }
        }
      }).start();
    }
    return new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(helixZkClient), propertyStoreConfig.rootPath,
        subscribedPaths);
  }

  private static class NopHelixZkClient implements HelixZkClient {

    @Override
    public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribeChildChanges(String path, IZkChildListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribeDataChanges(String path, IZkDataListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void subscribeStateChanges(IZkStateListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribeStateChanges(IZkStateListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribeAll() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPersistent(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPersistent(String path, boolean createParents) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPersistent(String path, boolean createParents, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPersistent(String path, Object data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPersistent(String path, Object data, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String createPersistentSequential(String path, Object data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String createPersistentSequential(String path, Object data, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createEphemeral(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createEphemeral(String path, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String create(String path, Object data, CreateMode mode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createEphemeral(String path, Object data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createEphemeral(String path, Object data, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String createEphemeralSequential(String path, Object data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getChildren(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int countChildren(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stat getStat(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRecursively(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readData(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readData(String path, boolean returnNullIfPathNotExists) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readData(String path, Stat stat) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readData(String path, Stat stat, boolean watch) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeData(String path, Object object) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeData(String path, Object datat, int expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void asyncCreate(String path, Object datat, CreateMode mode, ZkAsyncCallbacks.CreateCallbackHandler cb) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void asyncSetData(String path, Object datat, int version, ZkAsyncCallbacks.SetDataCallbackHandler cb) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void watchForData(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> watchForChilds(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCreationTime(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getServers() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSessionId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] serialize(Object data, String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T deserialize(byte[] data, String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setZkSerializer(ZkSerializer zkSerializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PathBasedZkSerializer getZkSerializer() {
      throw new UnsupportedOperationException();
    }
  }

  private static class HolderHelixZkClient implements HelixZkClient {
    private AtomicReference<ZkClient> zkClient = new AtomicReference<>(null);
    private static final HelixZkClient nopHelixZkClient = new NopHelixZkClient();

    private HelixZkClient realOrNop() {
      return zkClient.get() == null ? nopHelixZkClient : zkClient.get();
    }

    public void setZkClient(ZkClient zkClient) {
      this.zkClient.compareAndSet(null, zkClient);
    }

    @Override
    public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
      return realOrNop().subscribeChildChanges(path, listener);
    }

    @Override
    public void unsubscribeChildChanges(String path, IZkChildListener listener) {
      realOrNop().unsubscribeChildChanges(path, listener);
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener listener) {
      realOrNop().subscribeDataChanges(path, listener);
    }

    @Override
    public void unsubscribeDataChanges(String path, IZkDataListener listener) {
      realOrNop().unsubscribeDataChanges(path, listener);
    }

    @Override
    public void subscribeStateChanges(IZkStateListener listener) {
      realOrNop().subscribeStateChanges(listener);
    }

    @Override
    public void unsubscribeStateChanges(IZkStateListener listener) {
      realOrNop().unsubscribeStateChanges(listener);
    }

    @Override
    public void unsubscribeAll() {
      realOrNop().unsubscribeAll();
    }

    @Override
    public void createPersistent(String path) {
      realOrNop().createPersistent(path);
    }

    @Override
    public void createPersistent(String path, boolean createParents) {
      realOrNop().createPersistent(path, createParents);
    }

    @Override
    public void createPersistent(String path, boolean createParents, List<ACL> acl) {
      realOrNop().createPersistent(path, createParents, acl);
    }

    @Override
    public void createPersistent(String path, Object data) {
      realOrNop().createPersistent(path, data);
    }

    @Override
    public void createPersistent(String path, Object data, List<ACL> acl) {
      realOrNop().createPersistent(path, data, acl);
    }

    @Override
    public String createPersistentSequential(String path, Object data) {
      return realOrNop().createPersistentSequential(path, data);
    }

    @Override
    public String createPersistentSequential(String path, Object data, List<ACL> acl) {
      return realOrNop().createPersistentSequential(path, data, acl);
    }

    @Override
    public void createEphemeral(String path) {
      realOrNop().createEphemeral(path);
    }

    @Override
    public void createEphemeral(String path, List<ACL> acl) {
      realOrNop().createEphemeral(path, acl);
    }

    @Override
    public String create(String path, Object data, CreateMode mode) {
      return realOrNop().create(path, data, mode);
    }

    @Override
    public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
      return realOrNop().create(path, datat, acl, mode);
    }

    @Override
    public void createEphemeral(String path, Object data) {
      realOrNop().createEphemeral(path, data);
    }

    @Override
    public void createEphemeral(String path, Object data, List<ACL> acl) {
      realOrNop().createEphemeral(path, data, acl);
    }

    @Override
    public String createEphemeralSequential(String path, Object data) {
      return realOrNop().createEphemeralSequential(path, data);
    }

    @Override
    public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
      return realOrNop().createEphemeralSequential(path, data, acl);
    }

    @Override
    public List<String> getChildren(String path) {
      return realOrNop().getChildren(path);
    }

    @Override
    public int countChildren(String path) {
      return realOrNop().countChildren(path);
    }

    @Override
    public boolean exists(String path) {
      return realOrNop().exists(path);
    }

    @Override
    public Stat getStat(String path) {
      return realOrNop().getStat(path);
    }

    @Override
    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
      return realOrNop().waitUntilExists(path, timeUnit, time);
    }

    @Override
    public void deleteRecursively(String path) {
      realOrNop().deleteRecursively(path);
    }

    @Override
    public boolean delete(String path) {
      return realOrNop().delete(path);
    }

    @Override
    public <T> T readData(String path) {
      return realOrNop().readData(path);
    }

    @Override
    public <T> T readData(String path, boolean returnNullIfPathNotExists) {
      return realOrNop().readData(path, returnNullIfPathNotExists);
    }

    @Override
    public <T> T readData(String path, Stat stat) {
      return realOrNop().readData(path, stat);
    }

    @Override
    public <T> T readData(String path, Stat stat, boolean watch) {
      return realOrNop().readData(path, stat, watch);
    }

    @Override
    public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
      return realOrNop().readDataAndStat(path, stat, returnNullIfPathNotExists);
    }

    @Override
    public void writeData(String path, Object object) {
      realOrNop().writeData(path, object);
    }

    @Override
    public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
      realOrNop().updateDataSerialized(path, updater);
    }

    @Override
    public void writeData(String path, Object datat, int expectedVersion) {
      realOrNop().writeData(path, datat, expectedVersion);
    }

    @Override
    public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
      return realOrNop().writeDataReturnStat(path, datat, expectedVersion);
    }

    @Override
    public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
      return realOrNop().writeDataGetStat(path, datat, expectedVersion);
    }

    @Override
    public void asyncCreate(String path, Object datat, CreateMode mode, ZkAsyncCallbacks.CreateCallbackHandler cb) {
      realOrNop().asyncCreate(path, datat, mode, cb);
    }

    @Override
    public void asyncSetData(String path, Object datat, int version, ZkAsyncCallbacks.SetDataCallbackHandler cb) {
      realOrNop().asyncSetData(path, datat, version, cb);
    }

    @Override
    public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
      realOrNop().asyncGetData(path, cb);
    }

    @Override
    public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
      realOrNop().asyncExists(path, cb);
    }

    @Override
    public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
      realOrNop().asyncDelete(path, cb);
    }

    @Override
    public void watchForData(String path) {
      realOrNop().watchForData(path);
    }

    @Override
    public List<String> watchForChilds(String path) {
      return realOrNop().watchForChilds(path);
    }

    @Override
    public long getCreationTime(String path) {
      return realOrNop().getCreationTime(path);
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) {
      return realOrNop().multi(ops);
    }

    @Override
    public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
      return realOrNop().waitUntilConnected(time, timeUnit);
    }

    @Override
    public String getServers() {
      return realOrNop().getServers();
    }

    @Override
    public long getSessionId() {
      return realOrNop().getSessionId();
    }

    @Override
    public void close() {
      realOrNop().close();
    }

    @Override
    public boolean isClosed() {
      return realOrNop().isClosed();
    }

    @Override
    public byte[] serialize(Object data, String path) {
      return realOrNop().serialize(data, path);
    }

    @Override
    public <T> T deserialize(byte[] data, String path) {
      return realOrNop().deserialize(data, path);
    }

    @Override
    public void setZkSerializer(ZkSerializer zkSerializer) {
      realOrNop().setZkSerializer(zkSerializer);
    }

    @Override
    public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
      realOrNop().setZkSerializer(zkSerializer);
    }

    @Override
    public PathBasedZkSerializer getZkSerializer() {
      return realOrNop().getZkSerializer();
    }
  }
}
