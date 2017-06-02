/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.zookeeper.data.Stat;


/**
 * A mock implementation of {@link HelixPropertyStore} and {@link BaseDataAccessor}.
 */
class MockHelixPropertyStore<T> implements HelixPropertyStore<T>, BaseDataAccessor<T> {
  private final Map<String, T> pathToRecords = new HashMap<>();
  private final Map<String, Set<HelixPropertyListener>> pathToListeners = new HashMap<>();

  @Override
  public void start() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void stop() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void subscribe(String parentPath, HelixPropertyListener listener) {
    Set<HelixPropertyListener> listeners = pathToListeners.get(parentPath);
    if (listeners == null) {
      listeners = new HashSet<>();
      pathToListeners.put(parentPath, listeners);
    }
    listeners.add(listener);
  }

  @Override
  public void unsubscribe(String parentPath, HelixPropertyListener listener) {
    Set<HelixPropertyListener> listeners = pathToListeners.get(parentPath);
    if (listeners != null) {
      listeners.remove(listener);
    }
  }

  @Override
  public boolean create(String path, T record, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean set(String path, T record, int options) {
    System.err.println("Setting to store path: " + path + ", record: " + record.toString());
    return setAndNotify(path, record);
  }

  @Override
  public boolean set(String path, T record, int expectVersion, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    T newRecord = updater.update(pathToRecords.get(path));
    System.err.println("Updating to store path: " + path + ", record: " + newRecord.toString());
    return setAndNotify(path, newRecord);
  }

  @Override
  public boolean remove(String path, int options) {
    if (path.equals("/")) {
      notifyListeners("/", HelixPropertyStoreUtils.StoreOperationType.DELETE);
      pathToRecords.clear();
      return true;
    } else {
      throw new IllegalStateException("Not implemented");
    }
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> dataUpdaters, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean[] remove(List<String> paths, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public T get(String path, Stat stat, int options) {
    return pathToRecords.get(path);
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> getChildNames(String parentPath, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean exists(String path, int options) {
    if (path.equals("/")) {
      return true;
    }
    return pathToRecords.containsKey(path);
  }

  @Override
  public boolean[] exists(List<String> paths, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Stat[] getStats(List<String> paths, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Stat getStat(String path, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void reset() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Notifies the {@link HelixPropertyListener}s that have subscribed to the path.
   * @param path The path for the {@link HelixPropertyListener}s to notify.
   * @param operationType The type of the operation that was conducted on the path.
   */
  private void notifyListeners(String path, HelixPropertyStoreUtils.StoreOperationType operationType) {
    Set<HelixPropertyListener> listeners = pathToListeners.get(path);
    if (listeners != null) {
      for (HelixPropertyListener listener : listeners) {
        switch (operationType) {
          case WRITE:
            listener.onDataChange(path);
            break;
          case CREATE:
            listener.onDataCreate(path);
            break;
          case DELETE:
            listener.onDataDelete(path);
            break;

          default:
            throw new IllegalArgumentException("Unrecognized store operation type " + operationType);
        }
      }
    }
  }

  /**
   * Sets a record in store and notify the corresponding listeners.
   * @param path The path to set the record.
   * @param record The record to set.
   * @return {@code true}.
   */
  private boolean setAndNotify(String path, T record) {
    HelixPropertyStoreUtils.StoreOperationType operationType =
        pathToRecords.get(path) == null ? HelixPropertyStoreUtils.StoreOperationType.CREATE
            : HelixPropertyStoreUtils.StoreOperationType.WRITE;
    pathToRecords.put(path, record);
    notifyListeners(path, operationType);
    return true;
  }
}
