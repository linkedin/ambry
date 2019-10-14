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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;


/**
 * An extension of {@link StorageManager} to help with tests.
 */
class MockStorageManager extends StorageManager {

  /**
   * The operation received at the store.
   */
  static RequestOrResponseType operationReceived = null;
  /**
   * The {@link MessageWriteSet} received at the store (only for put, delete and ttl update)
   */
  static MessageWriteSet messageWriteSetReceived = null;
  /**
   * The IDs received at the store (only for get)
   */
  static List<? extends StoreKey> idsReceived = null;
  /**
   * The {@link StoreGetOptions} received at the store (only for get)
   */
  static EnumSet<StoreGetOptions> storeGetOptionsReceived;
  /**
   * The {@link FindToken} received at the store (only for findEntriesSince())
   */
  static FindToken tokenReceived = null;
  /**
   * The maxTotalSizeOfEntries received at the store (only for findEntriesSince())
   */
  static Long maxTotalSizeOfEntriesReceived = null;
  /**
   * StoreException to throw when an API is invoked
   */
  static StoreException storeException = null;
  /**
   * RuntimeException to throw when an API is invoked. Will be preferred over {@link #storeException}.
   */
  static RuntimeException runtimeException = null;

  /**
   * An empty {@link Store} implementation.
   */
  private Store store = new Store() {
    boolean started;

    @Override
    public void start() throws StoreException {
      throwExceptionIfRequired();
      started = true;
    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
      operationReceived = RequestOrResponseType.GetRequest;
      idsReceived = ids;
      storeGetOptionsReceived = storeGetOptions;
      throwExceptionIfRequired();
      checkValidityOfIds(ids);
      return new StoreInfo(new MessageReadSet() {
        @Override
        public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) {
          return 0;
        }

        @Override
        public int count() {
          return 0;
        }

        @Override
        public long sizeInBytes(int index) {
          return 0;
        }

        @Override
        public StoreKey getKeyAt(int index) {
          return null;
        }

        @Override
        public void doPrefetch(int index, long relativeOffset, long size) {
        }
      }, Collections.emptyList());
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
      operationReceived = RequestOrResponseType.PutRequest;
      messageWriteSetReceived = messageSetToWrite;
      throwExceptionIfRequired();
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
      operationReceived = RequestOrResponseType.DeleteRequest;
      messageWriteSetReceived = messageSetToDelete;
      throwExceptionIfRequired();
      checkValidityOfIds(
          messageSetToDelete.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()));
    }

    @Override
    public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {
      operationReceived = RequestOrResponseType.TtlUpdateRequest;
      messageWriteSetReceived = messageSetToUpdate;
      throwExceptionIfRequired();
      checkValidityOfIds(
          messageSetToUpdate.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()));
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
      operationReceived = RequestOrResponseType.ReplicaMetadataRequest;
      tokenReceived = token;
      maxTotalSizeOfEntriesReceived = maxTotalSizeOfEntries;
      throwExceptionIfRequired();
      return new FindInfo(Collections.emptyList(),
          findTokenHelper.getFindTokenFactoryFromReplicaType(ReplicaType.DISK_BACKED).getNewFindToken());
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StoreStats getStoreStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKeyDeleted(StoreKey key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isStarted() {
      return started;
    }

    public void shutdown() throws StoreException {
      throwExceptionIfRequired();
      started = false;
    }

    /**
     * Throws a {@link RuntimeException} or {@link StoreException} if so configured
     * @throws StoreException
     */
    private void throwExceptionIfRequired() throws StoreException {
      if (runtimeException != null) {
        throw runtimeException;
      }
      if (storeException != null) {
        throw storeException;
      }
    }

    /**
     * Checks the validity of the {@code ids}
     * @param ids the {@link StoreKey}s to check
     * @throws StoreException if the key is not valid
     */
    private void checkValidityOfIds(Collection<? extends StoreKey> ids) throws StoreException {
      for (StoreKey id : ids) {
        if (!validKeysInStore.contains(id)) {
          throw new StoreException("Not a valid key.", StoreErrorCodes.ID_Not_Found);
        }
      }
    }
  };

  private static final VerifiableProperties VPROPS = new VerifiableProperties(new Properties());
  /**
   * if {@code true}, a {@code null} {@link Store} is returned on a call to {@link StorageManager#getStore(PartitionId, boolean)}. Otherwise
   * {@link #store} is returned.
   */
  boolean returnNullStore = false;
  /**
   * if not null, return this when getStore() method is called.
   */
  Store overrideStoreToReturn = null;
  /**
   * If non-null, the given exception is thrown when {@link #scheduleNextForCompaction(PartitionId)} is called.
   */
  RuntimeException exceptionToThrowOnSchedulingCompaction = null;
  /**
   * If non-null, the given exception is thrown when {@link #controlCompactionForBlobStore(PartitionId, boolean)} is called.
   */
  RuntimeException exceptionToThrowOnControllingCompaction = null;
  /**
   * If non-null, the given exception is thrown when {@link #shutdownBlobStore(PartitionId)} is called.
   */
  RuntimeException exceptionToThrowOnShuttingDownBlobStore = null;
  /**
   * If non-null, the given exception is thrown when {@link #startBlobStore(PartitionId)} is called.
   */
  RuntimeException exceptionToThrowOnStartingBlobStore = null;
  /**
   * The return value for a call to {@link #scheduleNextForCompaction(PartitionId)}.
   */
  boolean returnValueOfSchedulingCompaction = true;
  /**
   * The return value for a call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}.
   */
  boolean returnValueOfControllingCompaction = true;
  /**
   * The return value for a call to {@link #shutdownBlobStore(PartitionId)}.
   */
  boolean returnValueOfShutdownBlobStore = true;
  /**
   * The return value for a call to {@link #startBlobStore(PartitionId)}.
   */
  boolean returnValueOfStartingBlobStore = true;
  /**
   * The return value for a call to {@link #addBlobStore(ReplicaId)}.
   */
  boolean returnValueOfAddBlobStore = true;
  /**
   * The return value for a call to {@link #removeBlobStore(PartitionId)}.
   */
  boolean returnValueOfRemoveBlobStore = true;
  /**
   * The {@link PartitionId} that was provided in the call to {@link #scheduleNextForCompaction(PartitionId)}
   */
  PartitionId compactionScheduledPartitionId = null;
  /**
   * The {@link PartitionId} that was provided in the call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}
   */
  PartitionId compactionControlledPartitionId = null;
  /**
   * The {@link boolean} that was provided in the call to {@link #controlCompactionForBlobStore(PartitionId, boolean)}
   */
  Boolean compactionEnableVal = null;
  /**
   * The {@link PartitionId} that was provided in the call to {@link #shutdownBlobStore(PartitionId)}
   */
  PartitionId shutdownPartitionId = null;
  /**
   * The {@link PartitionId} that was provided in the call to {@link #startBlobStore(PartitionId)}
   */
  PartitionId startedPartitionId = null;
  PartitionId addedPartitionId = null;
  CountDownLatch waitOperationCountdown = new CountDownLatch(0);
  boolean firstCall = true;
  List<PartitionId> unreachablePartitions = new ArrayList<>();

  private Set<StoreKey> validKeysInStore = new HashSet<>();
  private FindTokenHelper findTokenHelper = new FindTokenHelper();
  private Map<PartitionId, Store> storeMap = null;

  MockStorageManager(Set<StoreKey> validKeysInStore, List<? extends ReplicaId> replicas,
      FindTokenHelper findTokenHelper) throws StoreException {
    super(new StoreConfig(VPROPS), new DiskManagerConfig(VPROPS), Utils.newScheduler(1, true), new MetricRegistry(),
        replicas, null, null, null, null, new MockTime());
    this.validKeysInStore = validKeysInStore;
    this.findTokenHelper = findTokenHelper;
  }

  MockStorageManager(Map<PartitionId, Store> map) throws StoreException {
    super(new StoreConfig(VPROPS), new DiskManagerConfig(VPROPS), null, new MetricRegistry(), new ArrayList<>(), null,
        null, null, null, SystemTime.getInstance());
    storeMap = map;
  }

  @Override
  public Store getStore(PartitionId id, boolean skipStateCheck) {
    if (!firstCall) {
      try {
        waitOperationCountdown.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException("CountDown await was interrupted", e);
      }
    }
    firstCall = false;
    Store storeToReturn;
    if (storeMap != null) {
      storeToReturn = storeMap.get(id);
      if (storeToReturn == null) {
        unreachablePartitions.add(id);
      }
    } else if (overrideStoreToReturn != null) {
      storeToReturn = overrideStoreToReturn;
    } else {
      storeToReturn = returnNullStore ? null : store;
    }
    return storeToReturn;
  }

  @Override
  public boolean scheduleNextForCompaction(PartitionId id) {
    if (exceptionToThrowOnSchedulingCompaction != null) {
      throw exceptionToThrowOnSchedulingCompaction;
    }
    compactionScheduledPartitionId = id;
    return returnValueOfSchedulingCompaction;
  }

  @Override
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    if (exceptionToThrowOnControllingCompaction != null) {
      throw exceptionToThrowOnControllingCompaction;
    }
    compactionControlledPartitionId = id;
    compactionEnableVal = enabled;
    return returnValueOfControllingCompaction;
  }

  @Override
  public boolean shutdownBlobStore(PartitionId id) {
    if (exceptionToThrowOnShuttingDownBlobStore != null) {
      throw exceptionToThrowOnShuttingDownBlobStore;
    }
    shutdownPartitionId = id;
    return returnValueOfShutdownBlobStore;
  }

  @Override
  public boolean startBlobStore(PartitionId id) {
    if (exceptionToThrowOnStartingBlobStore != null) {
      throw exceptionToThrowOnStartingBlobStore;
    }
    startedPartitionId = id;
    return returnValueOfStartingBlobStore;
  }

  @Override
  public boolean addBlobStore(ReplicaId id) {
    addedPartitionId = id.getPartitionId();
    return returnValueOfAddBlobStore;
  }

  @Override
  public boolean removeBlobStore(PartitionId id) {
    return returnValueOfRemoveBlobStore;
  }

  /**
   * Resets variables associated with the {@link Store} impl
   */
  void resetStore() {
    operationReceived = null;
    messageWriteSetReceived = null;
    idsReceived = null;
    storeGetOptionsReceived = null;
    tokenReceived = null;
    maxTotalSizeOfEntriesReceived = null;
  }
}
