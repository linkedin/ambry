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

import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;

import static com.github.ambry.commons.TestUtils.*;


/**
 * Utilities for operating on the corresponding part of a {@code ZooKeeper} for a {@link HelixPropertyStore}.
 */
class HelixPropertyStoreUtils {
  private static final long operationTimeoutMs = 20000;

  /**
   * An agent to perform operations on a {@link HelixPropertyStore} on ZooKeeper.
   */
  static class HelixStoreOperator {
    private final HelixPropertyStore<ZNRecord> store;

    /**
     * Constructor.
     * @param storeConfig The configs needed to connect a {@code ZooKeeper} and operate on the specified
     *                    {@link HelixPropertyStore}.
     * @param storeFactory The factory to generate a {@link HelixPropertyStore}.
     */
    HelixStoreOperator(HelixPropertyStoreConfig storeConfig, HelixPropertyStoreFactory storeFactory) {
      ArrayList<String> paths = new ArrayList<>();
      paths.add(storeConfig.rootPath);
      store = storeFactory.getHelixPropertyStore(storeConfig, paths);
    }

    /**
     * Writes a {@link ZNRecord} to a store path. This operation is blocking.
     * @param path The store path to write. This is a relative path under the store root path.
     * @param zNRecord The {@link ZNRecord} to write.
     * @throws Exception
     */
    void write(String path, ZNRecord zNRecord) throws Exception {
      AtomicReference<Exception> exceptionRef = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      StoreOperationListener operationListener =
          new StoreOperationListener(latch, StoreOperationType.WRITE, exceptionRef);
      store.subscribe(path, operationListener);
      store.set(path, zNRecord, AccessOption.PERSISTENT);
      awaitLatchOrTimeout(latch, operationTimeoutMs);
      store.unsubscribe(path, operationListener);
      if (exceptionRef.get() != null) {
        throw exceptionRef.get();
      }
    }

    /**
     * Deletes a store path. This operation is blocking.
     * @param path The store path to delete. This is a relative path under the store root path.
     * @throws InterruptedException
     */
    void delete(String path) throws Exception {
      AtomicReference<Exception> exceptionRef = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      StoreOperationListener operationListener =
          new StoreOperationListener(latch, StoreOperationType.DELETE, exceptionRef);
      store.subscribe(path, operationListener);
      store.remove(path, AccessOption.PERSISTENT);
      awaitLatchOrTimeout(latch, operationTimeoutMs);
      store.unsubscribe(path, operationListener);
      if (exceptionRef.get() != null) {
        throw exceptionRef.get();
      }
    }

    /**
     * Checks if a path exists in the store.
     * @param path The path to check. This is a relative path under the store root path.
     * @return {@code true} if the path exists.
     */
    boolean exist(String path) {
      return store.exists(path, AccessOption.PERSISTENT);
    }
  }

  /**
   * A listener that listens the above operations (write and delete) on a {@link HelixPropertyStore}.
   */
  private static class StoreOperationListener implements HelixPropertyListener {
    private final CountDownLatch latch;
    private final StoreOperationType operationType;
    private final AtomicReference<Exception> exceptionRef;

    /**
     * Constructor.
     * @param latch The {@link CountDownLatch} to count down once the operation of specified type is done.
     * @param operationType The type of the operation to listen.
     */
    StoreOperationListener(CountDownLatch latch, StoreOperationType operationType,
        AtomicReference<Exception> exceptionRef) {
      this.latch = latch;
      this.operationType = operationType;
      this.exceptionRef = exceptionRef;
    }

    @Override
    public void onDataChange(String path) {
      if (!operationType.equals(StoreOperationType.WRITE)) {
        exceptionRef.set(new Exception("Data is changed but wrong operation type is specified. " + operationType));
      }
      latch.countDown();
    }

    @Override
    public void onDataCreate(String path) {
      if (!operationType.equals(StoreOperationType.CREATE) && !operationType.equals(StoreOperationType.WRITE)) {
        exceptionRef.set(new Exception("Data is created but wrong operation type is specified. " + operationType));
      }
      latch.countDown();
    }

    @Override
    public void onDataDelete(String path) {
      if (!operationType.equals(StoreOperationType.DELETE)) {
        exceptionRef.set(new Exception("Data is deleted but wrong operation type is specified. " + operationType));
      }
      latch.countDown();
    }
  }

  /**
   * The type of store operations.
   */
  enum StoreOperationType {
    CREATE, WRITE, DELETE
  }

  /**
   * Delete corresponding {@code ZooKeeper} nodes of a {@link HelixPropertyStore} if exist. The {@link HelixPropertyStore}
   * is specified by {@link HelixPropertyStoreConfig}.
   * @param storeConfig The config that specifies the {@link HelixPropertyStore}.
   * @param storeFactory The factory to get a {@link HelixPropertyStore}.
   * @throws InterruptedException
   */
  static void deleteStoreIfExists(HelixPropertyStoreConfig storeConfig, HelixPropertyStoreFactory storeFactory)
      throws Exception {
    HelixStoreOperator storeOperator = new HelixStoreOperator(storeConfig, storeFactory);
    if (storeOperator.exist("/")) {
      storeOperator.delete("/");
    }
  }

  /**
   * A util method that generates {@link HelixPropertyStoreConfig}.
   * @param zkClientConnectString The connect string to connect to a {@code ZooKeeper}.
   * @param zkClientSessionTimeoutMs Timeout for a zk session.
   * @param zkClientConnectionTimeoutMs Timeout for a zk connection.
   * @param storeRootPath The root path of a store in {@code ZooKeeper}.
   * @return {@link HelixPropertyStoreConfig} defined by the arguments.
   */
  static HelixPropertyStoreConfig getHelixStoreConfig(String zkClientConnectString, int zkClientSessionTimeoutMs,
      int zkClientConnectionTimeoutMs, String storeRootPath) {
    Properties helixConfigProps = new Properties();
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(zkClientConnectionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(zkClientSessionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string",
        zkClientConnectString);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", storeRootPath);
    VerifiableProperties vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    return new HelixPropertyStoreConfig(vHelixConfigProps);
  }
}
