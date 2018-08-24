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
package com.github.ambry.clustermap;

import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.TestUtils.*;


/**
 * An agent to perform operations on a {@link HelixPropertyStore}.
 */
public class HelixStoreOperator {
  private static final Logger logger = LoggerFactory.getLogger(HelixStoreOperator.class);
  private static final long OPERATION_TIMEOUT_MS = 20000;
  private final HelixPropertyStore<ZNRecord> helixStore;

  /**
   * Constructor.
   * @param helixStore A {@link HelixPropertyStore} that will be used by this {@link HelixStoreOperator}.
   */
  public HelixStoreOperator(HelixPropertyStore<ZNRecord> helixStore) {
    this.helixStore = helixStore;
  }

  /**
   * A constructor that gets a {@link HelixStoreOperator} based on the {@link HelixPropertyStoreConfig}.
   * @param zkServers the ZooKeeper server address.
   * @param storeConfig A {@link HelixPropertyStore} used to instantiate a {@link HelixStoreOperator}.
   */
  public HelixStoreOperator(String zkServers, HelixPropertyStoreConfig storeConfig) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("storeConfig cannot be null");
    }
    long startTimeMs = System.currentTimeMillis();
    logger.info("Starting a HelixStoreOperator");
    List<String> subscribedPaths = Collections.singletonList(storeConfig.rootPath);
    HelixPropertyStore<ZNRecord> helixStore =
        CommonUtils.createHelixPropertyStore(zkServers, storeConfig, subscribedPaths);
    logger.info("HelixPropertyStore started with zkClientConnectString={}, zkClientSessionTimeoutMs={}, "
            + "zkClientConnectionTimeoutMs={}, rootPath={}, subscribedPaths={}", zkServers,
        storeConfig.zkClientSessionTimeoutMs, storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath,
        subscribedPaths);
    this.helixStore = helixStore;
    logger.info("HelixStoreOperator started, took {}ms", System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Writes a {@link ZNRecord} to a store path. This operation is blocking.
   * @param path The store path to write. This is a relative path under the store root path.
   * @param zNRecord The {@link ZNRecord} to write.
   * @throws Exception
   */
  public void write(String path, ZNRecord zNRecord) throws Exception {
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    StoreOperationListener operationListener =
        new StoreOperationListener(latch, StoreOperationType.WRITE, exceptionRef);
    helixStore.subscribe(path, operationListener);
    helixStore.set(path, zNRecord, AccessOption.PERSISTENT);
    awaitLatchOrTimeout(latch, OPERATION_TIMEOUT_MS);
    helixStore.unsubscribe(path, operationListener);
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  /**
   * Deletes a store path. This operation is blocking.
   * @param path The store path to delete. This is a relative path under the store root path.
   * @throws InterruptedException
   */
  public void delete(String path) throws Exception {
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    StoreOperationListener operationListener =
        new StoreOperationListener(latch, StoreOperationType.DELETE, exceptionRef);
    helixStore.subscribe(path, operationListener);
    helixStore.remove(path, AccessOption.PERSISTENT);
    awaitLatchOrTimeout(latch, OPERATION_TIMEOUT_MS);
    helixStore.unsubscribe(path, operationListener);
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  /**
   * Checks if a path exists in the store.
   * @param path The path to check. This is a relative path under the store root path.
   * @return {@code true} if the path exists.
   */
  public boolean exist(String path) {
    return helixStore.exists(path, AccessOption.PERSISTENT);
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
  public enum StoreOperationType {
    CREATE, WRITE, DELETE
  }
}
