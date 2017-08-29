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
package com.github.ambry.commons;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
public class MockHelixPropertyStore<T> implements HelixPropertyStore<T>, BaseDataAccessor<T> {
  private final Map<String, T> pathToRecords = new HashMap<>();
  private final Map<String, Set<HelixPropertyListener>> pathToListeners = new HashMap<>();
  private final AtomicInteger readCount = new AtomicInteger(0);
  private boolean shouldFailSetOperation = false;
  private boolean shouldRemoveRecordBeforeNotify = false;

  /**
   * Constructor for {@code MockHelixPropertyStore}.
   * @param shouldFailSetOperation A binary indicator to specify if the {@link #set(String, Object, int)} operation
   *                               should fail.
   * @param shouldRemoveRecordBeforeNotify A boolean indicator to specify if the record should be removed before
   *                                        notifying listeners.
   */
  MockHelixPropertyStore(boolean shouldFailSetOperation, boolean shouldRemoveRecordBeforeNotify) {
    this.shouldFailSetOperation = shouldFailSetOperation;
    this.shouldRemoveRecordBeforeNotify = shouldRemoveRecordBeforeNotify;
  }

  public MockHelixPropertyStore() {
    shouldFailSetOperation = false;
    shouldRemoveRecordBeforeNotify = false;
  }

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
    if (shouldFailSetOperation) {
      return false;
    }
    return setAndNotify(path, record);
  }

  @Override
  public boolean set(String path, T record, int expectVersion, int options) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    T newRecord = null;
    boolean exceptionDuringUpdater = false;
    try {
      newRecord = updater.update(pathToRecords.get(path));
    } catch (Exception e) {
      exceptionDuringUpdater = true;
    }
    if (exceptionDuringUpdater) {
      return false;
    } else {
      return setAndNotify(path, newRecord);
    }
  }

  @Override
  public boolean remove(String path, int options) {
    if (path.equals("/")) {
      pathToRecords.clear();
      notifyListeners("/", HelixStoreOperator.StoreOperationType.DELETE);
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
    readCount.addAndGet(1);
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
   * Gets count for how many reads have been made to this store.
   * @return The count of reads.
   */
  public int getReadCount() {
    return readCount.get();
  }

  /**
   * Notifies the {@link HelixPropertyListener}s that have subscribed to the path.
   * @param path The path for the {@link HelixPropertyListener}s to notify.
   * @param operationType The type of the operation that was conducted on the path.
   */
  private void notifyListeners(String path, HelixStoreOperator.StoreOperationType operationType) {
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
    HelixStoreOperator.StoreOperationType operationType =
        pathToRecords.get(path) == null ? HelixStoreOperator.StoreOperationType.CREATE
            : HelixStoreOperator.StoreOperationType.WRITE;
    if (!shouldRemoveRecordBeforeNotify) {
      pathToRecords.put(path, record);
    }
    notifyListeners(path, operationType);
    return true;
  }
}
