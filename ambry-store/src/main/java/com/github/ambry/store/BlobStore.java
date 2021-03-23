/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.codahale.metrics.Timer;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.TtlUpdateMessageFormatInputStream;
import com.github.ambry.messageformat.UndeleteMessageFormatInputStream;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.VirtualReplicatorCluster.*;


/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {
  static final String SEPARATOR = "_";
  static final String BOOTSTRAP_FILE_NAME = "bootstrap_in_progress";
  static final String DECOMMISSION_FILE_NAME = "decommission_in_progress";
  private final static String LockFile = ".lock";

  private final String storeId;
  private final String dataDir;
  private final ScheduledExecutorService taskScheduler;
  private final ScheduledExecutorService longLivedTaskScheduler;
  private final DiskIOScheduler diskIOScheduler;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private static final Logger logger = LoggerFactory.getLogger(BlobStore.class);
  private final Object storeWriteLock = new Object();
  private final StoreConfig config;
  private final long capacityInBytes;
  private final StoreKeyFactory factory;
  private final MessageStoreRecovery recovery;
  private final MessageStoreHardDelete hardDelete;
  private final StoreMetrics metrics;
  private final StoreMetrics storeUnderCompactionMetrics;
  private final Time time;
  private final UUID sessionId = UUID.randomUUID();
  private final ReplicaId replicaId;
  private final List<ReplicaStatusDelegate> replicaStatusDelegates;
  private final long thresholdBytesHigh;
  private final long thresholdBytesLow;
  private final long ttlUpdateBufferTimeMs;
  private final RemoteTokenTracker remoteTokenTracker;
  private final AtomicInteger errorCount;
  private final AccountService accountService;

  private Log log;
  private BlobStoreCompactor compactor;
  private BlobStoreStats blobStoreStats;
  private boolean started;
  private FileLock fileLock;
  private volatile ReplicaState currentState;
  private volatile boolean recoverFromDecommission;
  // TODO remove this once ZK migration is complete
  private AtomicBoolean isSealed = new AtomicBoolean(false);
  private AtomicBoolean isDisabled = new AtomicBoolean(false);
  protected PersistentIndex index;

  // THIS IS ONLY FOR TEST.
  volatile protected Callable<Void> operationBeforeSynchronization = null;
  volatile protected Callable<Void> inDeleteBetweenGetEndOffsetAndFindKey = null;

  /**
   * States representing the different scenarios that can occur when a set of messages are to be written to the store.
   * Nomenclature:
   * Absent: a key that is non-existent in the store.
   * Duplicate: a key that exists in the store and has the same CRC (meaning the message is the same).
   * Colliding: a key that exists in the store but has a different CRC (meaning the message is different).
   */
  private enum MessageWriteSetStateInStore {
    ALL_ABSENT, // The messages are all absent in the store.
    COLLIDING, // At least one message in the write set has the same key as another, different message in the store.
    ALL_DUPLICATE, // The messages are all duplicates - every one of them already exist in the store.
    SOME_NOT_ALL_DUPLICATE, // At least one of the message is a duplicate, but not all.
  }

  /**
   * Constructor for BlobStore, used in ambry-server
   * @param replicaId replica associated with BlobStore.  BlobStore id, data directory, and capacity derived from this
   * @param config the settings for store configuration.
   * @param taskScheduler the {@link ScheduledExecutorService} for executing short period background tasks.
   * @param longLivedTaskScheduler the {@link ScheduledExecutorService} for executing long period background tasks.
   * @param diskIOScheduler schedules disk IO operations
   * @param diskSpaceAllocator allocates log segment files.
   * @param metrics the {@link StorageManagerMetrics} instance to use.
   * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
   * @param factory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param replicaStatusDelegates delegates used to communicate BlobStore write status(sealed/unsealed, stopped/started)
   * @param time the {@link Time} instance to use.
   * @param accountService  the {@link AccountService} instance to use.
   */
  public BlobStore(ReplicaId replicaId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      List<ReplicaStatusDelegate> replicaStatusDelegates, Time time, AccountService accountService) {
    this(replicaId, replicaId.getPartitionId().toString(), config, taskScheduler, longLivedTaskScheduler,
        diskIOScheduler, diskSpaceAllocator, metrics, storeUnderCompactionMetrics, replicaId.getReplicaPath(),
        replicaId.getCapacityInBytes(), factory, recovery, hardDelete, replicaStatusDelegates, time, accountService,
        null);
  }

  /**
   * Constructor for BlobStore, used in ambry-tools
   * @param storeId id of the BlobStore
   * @param config the settings for store configuration.
   * @param taskScheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param longLivedTaskScheduler the {@link ScheduledExecutorService} for executing long period background tasks.
   * @param diskIOScheduler schedules disk IO operations.
   * @param diskSpaceAllocator allocates log segment files.
   * @param metrics the {@link StorageManagerMetrics} instance to use.
   * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
   * @param dataDir directory that will be used by the BlobStore for data
   * @param capacityInBytes capacity of the BlobStore
   * @param factory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  BlobStore(String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time) {
    this(null, storeId, config, taskScheduler, longLivedTaskScheduler, diskIOScheduler, diskSpaceAllocator, metrics,
        storeUnderCompactionMetrics, dataDir, capacityInBytes, factory, recovery, hardDelete, null, time, null, null);
  }

  BlobStore(ReplicaId replicaId, String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, List<ReplicaStatusDelegate> replicaStatusDelegates, Time time,
      AccountService accountService, BlobStoreStats blobStoreStats) {
    this.replicaId = replicaId;
    this.storeId = storeId;
    this.dataDir = dataDir;
    this.taskScheduler = taskScheduler;
    this.longLivedTaskScheduler = longLivedTaskScheduler;
    this.diskIOScheduler = diskIOScheduler;
    this.diskSpaceAllocator = diskSpaceAllocator;
    this.metrics = metrics;
    this.storeUnderCompactionMetrics = storeUnderCompactionMetrics;
    this.config = config;
    this.capacityInBytes = capacityInBytes;
    this.factory = factory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.accountService = accountService;
    this.replicaStatusDelegates = config.storeReplicaStatusDelegateEnable ? replicaStatusDelegates : null;
    this.time = time;
    long threshold = config.storeReadOnlyEnableSizeThresholdPercentage;
    long delta = config.storeReadWriteEnableSizeThresholdPercentageDelta;
    this.thresholdBytesHigh = (long) (capacityInBytes * (threshold / 100.0));
    this.thresholdBytesLow = (long) (capacityInBytes * ((threshold - delta) / 100.0));
    this.blobStoreStats = blobStoreStats; // Only in test will this be non-null
    ttlUpdateBufferTimeMs = TimeUnit.SECONDS.toMillis(config.storeTtlUpdateBufferTimeSeconds);
    errorCount = new AtomicInteger(0);
    currentState = ReplicaState.OFFLINE;
    remoteTokenTracker = new RemoteTokenTracker(replicaId);
    logger.debug(
        "The enable state of replicaStatusDelegate is {} on store {}. The high threshold is {} bytes and the low threshold is {} bytes",
        config.storeReplicaStatusDelegateEnable, storeId, this.thresholdBytesHigh, this.thresholdBytesLow);
    // if there is a decommission file in store dir, that means previous decommission didn't complete successfully.
    recoverFromDecommission = isDecommissionInProgress();
  }

  @Override
  public void start() throws StoreException {
    synchronized (storeWriteLock) {
      if (started) {
        throw new StoreException("Store already started", StoreErrorCodes.Store_Already_Started);
      }
      final Timer.Context context = metrics.storeStartTime.time();
      try {
        // Check if the data dir exist. If it does not exist, create it
        File dataFile = new File(dataDir);
        if (!dataFile.exists()) {
          logger.info("Store : {} data directory not found. creating it", dataDir);
          boolean created = dataFile.mkdir();
          if (!created) {
            throw new StoreException("Failed to create directory for data dir " + dataDir,
                StoreErrorCodes.Initialization_Error);
          }
        }
        if (!dataFile.isDirectory() || !dataFile.canRead()) {
          throw new StoreException(dataFile.getAbsolutePath() + " is either not a directory or is not readable",
              StoreErrorCodes.Initialization_Error);
        }

        // lock the directory
        fileLock = new FileLock(new File(dataDir, LockFile));
        if (!fileLock.tryLock()) {
          throw new StoreException(
              "Failed to acquire lock on file " + dataDir + ". Another process or thread is using this directory.",
              StoreErrorCodes.Initialization_Error);
        }

        StoreDescriptor storeDescriptor = new StoreDescriptor(dataDir, config);
        log = new Log(dataDir, capacityInBytes, diskSpaceAllocator, config, metrics);
        compactor = new BlobStoreCompactor(dataDir, storeId, factory, config, metrics, storeUnderCompactionMetrics,
            diskIOScheduler, diskSpaceAllocator, log, time, sessionId, storeDescriptor.getIncarnationId(),
            accountService, remoteTokenTracker);
        index = new PersistentIndex(dataDir, storeId, taskScheduler, log, config, factory, recovery, hardDelete,
            diskIOScheduler, metrics, time, sessionId, storeDescriptor.getIncarnationId());
        compactor.initialize(index);
        metrics.initializeIndexGauges(storeId, index, capacityInBytes);
        long logSegmentForecastOffsetMs = TimeUnit.HOURS.toMillis(config.storeDeletedMessageRetentionHours);
        long bucketSpanInMs = TimeUnit.MINUTES.toMillis(config.storeStatsBucketSpanInMinutes);
        long queueProcessingPeriodInMs =
            TimeUnit.MINUTES.toMillis(config.storeStatsRecentEntryProcessingIntervalInMinutes);
        if (blobStoreStats == null) {
          blobStoreStats = new BlobStoreStats(storeId, index, config.storeStatsBucketCount, bucketSpanInMs,
              logSegmentForecastOffsetMs, queueProcessingPeriodInMs, config.storeStatsWaitTimeoutInSecs,
              config.storeEnableBucketForLogSegmentReports, time, longLivedTaskScheduler, taskScheduler,
              diskIOScheduler, metrics);
        }
        checkCapacityAndUpdateReplicaStatusDelegate();
        logger.trace("The store {} is successfully started", storeId);
        onSuccess();
        isDisabled.set(false);
        started = true;
        resolveStoreInitialState();
        if (replicaId != null) {
          replicaId.markDiskUp();
        }
        enableReplicaIfNeeded();
      } catch (Exception e) {
        metrics.storeStartFailure.inc();
        throw new StoreException("Error while starting store for dir " + dataDir, e,
            StoreErrorCodes.Initialization_Error);
      } finally {
        context.stop();
      }
    }
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    checkStarted();
    // allows concurrent gets
    final Timer.Context context = metrics.getResponse.time();
    if (ids.size() > 1 && ids.size() != new HashSet<>(ids).size()) {
      metrics.duplicateKeysInBatch.inc();
      throw new IllegalArgumentException("The list of IDs provided contains duplicates");
    }
    try {
      List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>(ids.size());
      Map<StoreKey, MessageInfo> indexMessages = new HashMap<StoreKey, MessageInfo>(ids.size());
      for (StoreKey key : ids) {
        BlobReadOptions readInfo = index.getBlobReadInfo(key, storeGetOptions);
        readOptions.add(readInfo);
        indexMessages.put(key, readInfo.getMessageInfo());
        // validate accountId and containerId
        if (!key.isAccountContainerMatch(readInfo.getMessageInfo().getAccountId(),
            readInfo.getMessageInfo().getContainerId())) {
          if (config.storeValidateAuthorization) {
            throw new StoreException(
                "GET authorization failure. Key: " + key.getID() + " Actually accountId: " + readInfo.getMessageInfo()
                    .getAccountId() + " Actually containerId: " + readInfo.getMessageInfo().getContainerId(),
                StoreErrorCodes.Authorization_Failure);
          } else {
            logger.warn("GET authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                key.getID(), readInfo.getMessageInfo().getAccountId(), readInfo.getMessageInfo().getContainerId());
            metrics.getAuthorizationFailureCount.inc();
          }
        }
      }

      MessageReadSet readSet = new StoreMessageReadSet(readOptions);
      // We ensure that the metadata list is ordered with the order of the message read set view that the
      // log provides. This ensures ordering of all messages across the log and metadata from the index.
      List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(readSet.count());
      for (int i = 0; i < readSet.count(); i++) {
        messageInfoList.add(indexMessages.get(readSet.getKeyAt(i)));
      }
      onSuccess();
      return new StoreInfo(readSet, messageInfoList);
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown exception while trying to fetch blobs from store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  /**
   * Checks the state of the messages in the given {@link MessageWriteSet} in the given {@link FileSpan}.
   * @param messageSetToWrite Non-empty set of messages to write to the store.
   * @param fileSpan The fileSpan on which the check for existence of the messages have to be made.
   * @return {@link MessageWriteSetStateInStore} representing the outcome of the state check.
   * @throws StoreException relays those encountered from {@link PersistentIndex#findKey(StoreKey, FileSpan, EnumSet)}.
   */
  private MessageWriteSetStateInStore checkWriteSetStateInStore(MessageWriteSet messageSetToWrite, FileSpan fileSpan)
      throws StoreException {
    int existingIdenticalEntries = 0;
    for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
      if (index.findKey(info.getStoreKey(), fileSpan,
          EnumSet.of(PersistentIndex.IndexEntryType.PUT, PersistentIndex.IndexEntryType.DELETE)) != null) {
        if (index.wasRecentlySeen(info)) {
          existingIdenticalEntries++;
          metrics.identicalPutAttemptCount.inc();
        } else {
          logger.error("COLLISION: For key {} in WriteSet with crc {}, index already has the key with journal crc {}",
              info.getStoreKey(), info.getCrc(), index.journal.getCrcOfKey(info.getStoreKey()));
          return MessageWriteSetStateInStore.COLLIDING;
        }
      }
    }
    if (existingIdenticalEntries == messageSetToWrite.getMessageSetInfo().size()) {
      return MessageWriteSetStateInStore.ALL_DUPLICATE;
    } else if (existingIdenticalEntries > 0) {
      return MessageWriteSetStateInStore.SOME_NOT_ALL_DUPLICATE;
    } else {
      return MessageWriteSetStateInStore.ALL_ABSENT;
    }
  }

  /**
   * Check disabled state of current store(replica), if it is on disabled list, remove it and re-enable the replica.
   * Current store might be shut down and disabled due to disk I/O error and now it is able to restart after disk is
   * fixed/replaced. This method is called at the end of startup to re-enable this store if it's still in disabled state.
   */
  private void enableReplicaIfNeeded() {
    if (config.storeSetLocalPartitionStateEnabled && replicaStatusDelegates != null) {
      for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
        replicaStatusDelegate.enableReplica(replicaId);
      }
    }
  }

  /**
   * Checks the used capacity of the store against the configured percentage thresholds to see if the store
   * should be read-only or read-write
   */
  private void checkCapacityAndUpdateReplicaStatusDelegate() {
    if (replicaStatusDelegates != null) {
      logger.debug("The current used capacity is {} bytes on store {}", index.getLogUsedCapacity(),
          replicaId.getPartitionId());
      // In two zk clusters case, "replicaId.isSealed()" might be true in first cluster but is still unsealed in second
      // cluster, as server only spectates the first cluster. To guarantee both clusters have consistent seal/unseal
      // state, we bypass "isSealed()" check if there are more than one replicaStatusDelegates.
      if (index.getLogUsedCapacity() > thresholdBytesHigh && (!replicaId.isSealed() || (
          replicaStatusDelegates.size() > 1 && !isSealed.getAndSet(true)))) {
        for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
          if (!replicaStatusDelegate.seal(replicaId)) {
            metrics.sealSetError.inc();
            logger.warn("Could not set the partition as read-only status on {}", replicaId);
            isSealed.set(false);
          } else {
            metrics.sealDoneCount.inc();
            logger.info(
                "Store is successfully sealed for partition : {} because current used capacity : {} bytes exceeds ReadOnly threshold : {} bytes",
                replicaId.getPartitionId(), index.getLogUsedCapacity(), thresholdBytesHigh);
          }
        }
      } else if (index.getLogUsedCapacity() <= thresholdBytesLow && (replicaId.isSealed() || (
          replicaStatusDelegates.size() > 1 && isSealed.getAndSet(false)))) {
        for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
          if (!replicaStatusDelegate.unseal(replicaId)) {
            metrics.unsealSetError.inc();
            logger.warn("Could not set the partition as read-write status on {}", replicaId);
            isSealed.set(true);
          } else {
            metrics.unsealDoneCount.inc();
            logger.info(
                "Store is successfully unsealed for partition : {} because current used capacity : {} bytes is below ReadWrite threshold : {} bytes",
                replicaId.getPartitionId(), index.getLogUsedCapacity(), thresholdBytesLow);
          }
        }
      }
      // During startup, we also need to reconcile the replica state from both ZK clusters.
      if (!started && replicaStatusDelegates.size() > 1 && thresholdBytesLow < index.getLogUsedCapacity()
          && index.getLogUsedCapacity() <= thresholdBytesHigh) {
        // reconcile the state by reading sealing state from both clusters
        boolean sealed = false;
        String partitionName = replicaId.getPartitionId().toPathString();
        for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
          Set<String> sealedReplicas = new HashSet<>(replicaStatusDelegate.getSealedReplicas());
          sealed |= sealedReplicas.contains(partitionName);
        }
        for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
          boolean success = sealed ? replicaStatusDelegate.seal(replicaId) : replicaStatusDelegate.unseal(replicaId);
          if (success) {
            logger.info("Succeeded in reconciling replica state to {} state", sealed ? "sealed" : "unsealed");
            isSealed.set(sealed);
          } else {
            logger.error("Failed on reconciling replica state to {} state", sealed ? "sealed" : "unsealed");
          }
        }
      }
      //else: maintain current replicaId status if percentFilled between threshold - delta and threshold
    } else {
      logger.debug("The ReplicaStatusDelegate is not instantiated");
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    checkStarted();
    if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
      throw new IllegalArgumentException("Message write set cannot be empty");
    }
    checkDuplicates(messageSetToWrite.getMessageSetInfo());
    final Timer.Context context = metrics.putResponse.time();
    try {
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      MessageWriteSetStateInStore state =
          checkWriteSetStateInStore(messageSetToWrite, new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
      if (state == MessageWriteSetStateInStore.ALL_ABSENT) {
        maybeCallBeforeSynchronization();
        synchronized (storeWriteLock) {
          // Validate that log end offset was not changed. If changed, check once again for existing
          // keys in store
          Offset currentIndexEndOffset = index.getCurrentEndOffset();
          if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
            FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
            state = checkWriteSetStateInStore(messageSetToWrite, fileSpan);
          }

          if (state == MessageWriteSetStateInStore.ALL_ABSENT) {
            Offset endOffsetOfLastMessage = log.getEndOffset();
            messageSetToWrite.writeTo(log);
            logger.trace("Store : {} message set written to log", dataDir);

            List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
            ArrayList<IndexEntry> indexEntries = new ArrayList<>(messageInfo.size());
            for (MessageInfo info : messageInfo) {
              FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
              // Put from frontend should always use 0 as lifeVersion. (0 is the starting life version number for any data).
              // Put from replication or recovery should use liferVersion as it's.
              short lifeVersion = IndexValue.hasLifeVersion(info.getLifeVersion()) ? info.getLifeVersion() : (short) 0;
              IndexValue value =
                  new IndexValue(info.getSize(), fileSpan.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE,
                      info.getExpirationTimeInMs(), info.getOperationTimeMs(), info.getAccountId(),
                      info.getContainerId(), lifeVersion);
              IndexEntry entry = new IndexEntry(info.getStoreKey(), value, info.getCrc());
              indexEntries.add(entry);
              endOffsetOfLastMessage = fileSpan.getEndOffset();
            }
            FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(), endOffsetOfLastMessage);
            index.addToIndex(indexEntries, fileSpan);
            for (IndexEntry newEntry : indexEntries) {
              blobStoreStats.handleNewPutEntry(newEntry.getValue());
            }
            logger.trace("Store : {} message set written to index ", dataDir);
            checkCapacityAndUpdateReplicaStatusDelegate();
          }
        }
      }
      switch (state) {
        case COLLIDING:
          throw new StoreException(
              "For at least one message in the write set, another blob with same key exists in store",
              StoreErrorCodes.Already_Exist);
        case SOME_NOT_ALL_DUPLICATE:
          throw new StoreException(
              "At least one message but not all in the write set is identical to an existing entry",
              StoreErrorCodes.Already_Exist);
        case ALL_DUPLICATE:
          logger.trace("All entries to put already exist in the store, marking operation as successful");
          break;
        case ALL_ABSENT:
          logger.trace("All entries were absent, and were written to the store successfully");
          break;
      }
      onSuccess();
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to put blobs to store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  @Override
  public void delete(List<MessageInfo> infosToDelete) throws StoreException {
    checkStarted();
    checkDuplicates(infosToDelete);
    final Timer.Context context = metrics.deleteResponse.time();
    try {
      List<IndexValue> indexValuesPriorToDelete = new ArrayList<>();
      List<IndexValue> originalPuts = new ArrayList<>();
      List<Short> lifeVersions = new ArrayList<>();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      maybeCallInDeleteBetweenGetEndOffsetAndFindKey();
      for (MessageInfo info : infosToDelete) {
        IndexValue value =
            index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
        if (value == null) {
          throw new StoreException("Cannot delete id " + info.getStoreKey() + " because it is not present in the index",
              StoreErrorCodes.ID_Not_Found);
        }
        if (!info.getStoreKey().isAccountContainerMatch(value.getAccountId(), value.getContainerId())) {
          if (config.storeValidateAuthorization) {
            throw new StoreException("DELETE authorization failure. Key: " + info.getStoreKey() + "Actually accountId: "
                + value.getAccountId() + "Actually containerId: " + value.getContainerId(),
                StoreErrorCodes.Authorization_Failure);
          } else {
            logger.warn("DELETE authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                info.getStoreKey(), value.getAccountId(), value.getContainerId());
            metrics.deleteAuthorizationFailureCount.inc();
          }
        }
        short revisedLifeVersion = info.getLifeVersion();
        if (info.getLifeVersion() == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
          // This is a delete request from frontend
          if (value.isDelete()) {
            throw new StoreException(
                "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                StoreErrorCodes.ID_Deleted);
          }
          revisedLifeVersion = value.getLifeVersion();
        } else {
          // This is a delete request from replication
          if (value.isDelete() && value.getLifeVersion() == info.getLifeVersion()) {
            throw new StoreException(
                "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index with lifeVersion "
                    + value.getLifeVersion() + ".", StoreErrorCodes.ID_Deleted);
          }
          if (value.getLifeVersion() > info.getLifeVersion()) {
            throw new StoreException(
                "Cannot delete id " + info.getStoreKey() + " since it has a higher lifeVersion than the message info: "
                    + value.getLifeVersion() + ">" + info.getLifeVersion(), StoreErrorCodes.Life_Version_Conflict);
          }
        }
        indexValuesPriorToDelete.add(value);
        lifeVersions.add(revisedLifeVersion);
        if (!value.isDelete() && !value.isUndelete()) {
          originalPuts.add(value);
        } else {
          originalPuts.add(index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), value.getOffset()),
              EnumSet.of(PersistentIndex.IndexEntryType.PUT)));
        }
      }
      maybeCallBeforeSynchronization();
      synchronized (storeWriteLock) {
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          int i = 0;
          for (MessageInfo info : infosToDelete) {
            IndexValue value =
                index.findKey(info.getStoreKey(), fileSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
            if (value != null) {
              // There are several possible cases that can exist here. Delete has to follow either PUT, TTL_UPDATE or UNDELETE.
              // let EOBC be end offset before check, and [RECORD] means RECORD is optional
              // 1. PUT [TTL_UPDATE DELETE UNDELETE] EOBC DELETE
              // 2. PUT EOBC TTL_UPDATE
              // 3. PUT EOBC DELETE UNDELETE: this is really extreme case
              // From these cases, we can have value being DELETE, TTL_UPDATE AND UNDELETE, we have to deal with them accordingly.
              if (value.getLifeVersion() == lifeVersions.get(i)) {
                if (value.isDelete()) {
                  throw new StoreException(
                      "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                      StoreErrorCodes.ID_Deleted);
                }
                // value being ttl update is fine, we can just append DELETE to it.
              } else {
                // For the extreme case, we log it out and throw an exception.
                logger.warn("Concurrent operation for id " + info.getStoreKey() + " in store " + dataDir
                    + ". Newly added value " + value);
                throw new StoreException(
                    "Cannot delete id " + info.getStoreKey() + " since there are concurrent operation while delete",
                    StoreErrorCodes.Life_Version_Conflict);
              }
              indexValuesPriorToDelete.set(i, value);
            }
            i++;
          }
        }
        List<InputStream> inputStreams = new ArrayList<>(infosToDelete.size());
        List<MessageInfo> updatedInfos = new ArrayList<>(infosToDelete.size());
        int i = 0;
        for (MessageInfo info : infosToDelete) {
          MessageFormatInputStream stream =
              new DeleteMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
                  info.getOperationTimeMs(), lifeVersions.get(i));
          // Don't change the lifeVersion here, there are other logic in markAsDeleted that relies on this lifeVersion.
          updatedInfos.add(
              new MessageInfo(info.getStoreKey(), stream.getSize(), info.getAccountId(), info.getContainerId(),
                  info.getOperationTimeMs(), info.getLifeVersion()));
          inputStreams.add(stream);
          i++;
        }
        Offset endOffsetOfLastMessage = log.getEndOffset();
        MessageFormatWriteSet writeSet =
            new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), updatedInfos,
                false);
        writeSet.writeTo(log);
        logger.trace("Store : {} delete mark written to log", dataDir);
        int correspondingPutIndex = 0;
        for (MessageInfo info : updatedInfos) {
          FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
          IndexValue deleteIndexValue =
              index.markAsDeleted(info.getStoreKey(), fileSpan, null, info.getOperationTimeMs(), info.getLifeVersion());
          endOffsetOfLastMessage = fileSpan.getEndOffset();
          blobStoreStats.handleNewDeleteEntry(info.getStoreKey(), deleteIndexValue,
              originalPuts.get(correspondingPutIndex), indexValuesPriorToDelete.get(correspondingPutIndex));
          correspondingPutIndex++;
        }
        logger.trace("Store : {} delete has been marked in the index ", dataDir);
      }
      onSuccess();
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  @Override
  public void updateTtl(List<MessageInfo> infosToUpdate) throws StoreException {
    checkStarted();
    checkDuplicates(infosToUpdate);
    final Timer.Context context = metrics.ttlUpdateResponse.time();
    try {
      List<IndexValue> indexValuesToUpdate = new ArrayList<>();
      List<Short> lifeVersions = new ArrayList<>();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      for (MessageInfo info : infosToUpdate) {
        if (info.getExpirationTimeInMs() != Utils.Infinite_Time) {
          throw new StoreException("BlobStore only supports removing the expiration time",
              StoreErrorCodes.Update_Not_Allowed);
        }
        IndexValue value =
            index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
        if (value == null) {
          throw new StoreException("Cannot update TTL of " + info.getStoreKey() + " since it's not in the index",
              StoreErrorCodes.ID_Not_Found);
        } else if (!info.getStoreKey().isAccountContainerMatch(value.getAccountId(), value.getContainerId())) {
          if (config.storeValidateAuthorization) {
            throw new StoreException(
                "UPDATE authorization failure. Key: " + info.getStoreKey() + " AccountId in store: "
                    + value.getAccountId() + " ContainerId in store: " + value.getContainerId(),
                StoreErrorCodes.Authorization_Failure);
          } else {
            logger.warn("UPDATE authorization failure. Key: {} AccountId in store: {} ContainerId in store: {}",
                info.getStoreKey(), value.getAccountId(), value.getContainerId());
            metrics.ttlUpdateAuthorizationFailureCount.inc();
          }
        } else if (value.isDelete()) {
          throw new StoreException(
              "Cannot update TTL of " + info.getStoreKey() + " since it is already deleted in the index.",
              StoreErrorCodes.ID_Deleted);
        } else if (value.isTtlUpdate()) {
          throw new StoreException("TTL of " + info.getStoreKey() + " is already updated in the index.",
              StoreErrorCodes.Already_Updated);
        } else if (!IndexValue.hasLifeVersion(info.getLifeVersion()) && value.getExpiresAtMs() != Utils.Infinite_Time
            && value.getExpiresAtMs() < info.getOperationTimeMs() + ttlUpdateBufferTimeMs) {
          // When the request is from frontend, make sure it's not too close to expiry date.
          // When the request is from replication, we don't care about the operation time.
          throw new StoreException(
              "TTL of " + info.getStoreKey() + " cannot be updated because it is too close to expiry. Op time (ms): "
                  + info.getOperationTimeMs() + ". ExpiresAtMs: " + value.getExpiresAtMs(),
              StoreErrorCodes.Update_Not_Allowed);
        }
        indexValuesToUpdate.add(value);
        lifeVersions.add(value.getLifeVersion());
      }
      maybeCallBeforeSynchronization();
      synchronized (storeWriteLock) {
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          for (MessageInfo info : infosToUpdate) {
            IndexValue value =
                index.findKey(info.getStoreKey(), fileSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
            if (value != null) {
              if (value.isDelete()) {
                throw new StoreException(
                    "Cannot update TTL of " + info.getStoreKey() + " since it is already deleted in the index.",
                    StoreErrorCodes.ID_Deleted);
              } else if (value.isTtlUpdate()) {
                throw new StoreException("TTL of " + info.getStoreKey() + " is already updated in the index.",
                    StoreErrorCodes.Already_Updated);
              }
            }
          }
        }
        List<InputStream> inputStreams = new ArrayList<>(infosToUpdate.size());
        List<MessageInfo> updatedInfos = new ArrayList<>(infosToUpdate.size());
        int i = 0;
        for (MessageInfo info : infosToUpdate) {
          MessageFormatInputStream stream =
              new TtlUpdateMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
                  info.getExpirationTimeInMs(), info.getOperationTimeMs(), lifeVersions.get(i));
          // we only need change the stream size.
          updatedInfos.add(
              new MessageInfo(info.getStoreKey(), stream.getSize(), info.getAccountId(), info.getContainerId(),
                  info.getOperationTimeMs(), info.getLifeVersion()));
          inputStreams.add(stream);
          i++;
        }
        Offset endOffsetOfLastMessage = log.getEndOffset();
        MessageFormatWriteSet writeSet =
            new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), updatedInfos,
                false);
        writeSet.writeTo(log);
        logger.trace("Store : {} ttl update mark written to log", dataDir);
        int correspondingPutIndex = 0;
        for (MessageInfo info : updatedInfos) {
          FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
          // Ttl update should aways use the same lifeVersion as it's previous value of the same key, that's why we are
          // using LIFE_VERSION_FROM_FRONTEND here no matter the lifeVersion from the message info.
          IndexValue ttlUpdateValue =
              index.markAsPermanent(info.getStoreKey(), fileSpan, null, info.getOperationTimeMs(),
                  MessageInfo.LIFE_VERSION_FROM_FRONTEND);
          endOffsetOfLastMessage = fileSpan.getEndOffset();
          blobStoreStats.handleNewTtlUpdateEntry(ttlUpdateValue, indexValuesToUpdate.get(correspondingPutIndex++));
        }
        logger.trace("Store : {} ttl update has been marked in the index ", dataDir);
      }
      onSuccess();
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to update ttl of blobs from store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  @Override
  public short undelete(MessageInfo info) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.undeleteResponse.time();
    // The lifeVersion from message info is -1 when the undelete method is invoked by frontend request, we have to
    // get the legit lifeVersion before we can write undelete record to log segment.
    short revisedLifeVersion = info.getLifeVersion();
    try {
      StoreKey id = info.getStoreKey();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      short lifeVersionFromMessageInfo = info.getLifeVersion();

      List<IndexValue> values =
          index.findAllIndexValuesForKey(id, new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
      // Check if the undelete record is valid.
      index.validateSanityForUndelete(id, values, lifeVersionFromMessageInfo);
      IndexValue latestValue = values.get(0);
      IndexValue originalPut = values.get(values.size() - 1);
      if (!IndexValue.hasLifeVersion(revisedLifeVersion)) {
        revisedLifeVersion = (short) (latestValue.getLifeVersion() + 1);
      }
      MessageFormatInputStream stream =
          new UndeleteMessageFormatInputStream(id, info.getAccountId(), info.getContainerId(),
              info.getOperationTimeMs(), revisedLifeVersion);
      // Update info to add stream size;
      info =
          new MessageInfo(id, stream.getSize(), info.getAccountId(), info.getContainerId(), info.getOperationTimeMs(),
              revisedLifeVersion);
      ArrayList<MessageInfo> infoList = new ArrayList<>();
      infoList.add(info);
      MessageFormatWriteSet writeSet = new MessageFormatWriteSet(stream, infoList, false);
      if (!info.getStoreKey().isAccountContainerMatch(latestValue.getAccountId(), latestValue.getContainerId())) {
        if (config.storeValidateAuthorization) {
          throw new StoreException(
              "UNDELETE authorization failure. Key: " + info.getStoreKey() + " Actually accountId: "
                  + latestValue.getAccountId() + "Actually containerId: " + latestValue.getContainerId(),
              StoreErrorCodes.Authorization_Failure);
        } else {
          logger.warn("UNDELETE authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
              info.getStoreKey(), latestValue.getAccountId(), latestValue.getContainerId());
          metrics.undeleteAuthorizationFailureCount.inc();
        }
      }
      maybeCallBeforeSynchronization();
      synchronized (storeWriteLock) {
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          IndexValue value = index.findKey(info.getStoreKey(), fileSpan,
              EnumSet.of(PersistentIndex.IndexEntryType.DELETE, PersistentIndex.IndexEntryType.UNDELETE));
          if (value != null) {
            if (value.isUndelete() && value.getLifeVersion() == revisedLifeVersion) {
              // Might get an concurrent undelete from both replication and frontend.
              throw new IdUndeletedStoreException(
                  "Can't undelete id " + info.getStoreKey() + " in " + dataDir + " since concurrent operations",
                  value.getLifeVersion());
            } else {
              logger.warn("Revised lifeVersion is " + revisedLifeVersion + " last value is " + value);
              throw new StoreException(
                  "Cannot undelete id " + info.getStoreKey() + " since concurrent operation occurs",
                  StoreErrorCodes.Life_Version_Conflict);
            }
          }
        }
        Offset endOffsetOfLastMessage = log.getEndOffset();
        writeSet.writeTo(log);
        logger.trace("Store : {} undelete mark written to log", dataDir);
        FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
        // we still use lifeVersion from message info here so that we can re-verify the sanity of undelete request in persistent index.
        IndexValue newUndelete = index.markAsUndeleted(info.getStoreKey(), fileSpan, null, info.getOperationTimeMs(),
            lifeVersionFromMessageInfo);
        blobStoreStats.handleNewUndeleteEntry(info.getStoreKey(), newUndelete, originalPut, latestValue);
      }
      onSuccess();
      return revisedLifeVersion;
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to undelete blobs from store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  /**
   * Call {@link #operationBeforeSynchronization} if it's not null. This is for testing only.
   */
  private void maybeCallBeforeSynchronization() throws Exception {
    Callable<Void> callable = operationBeforeSynchronization;
    if (callable != null) {
      callable.call();
    }
  }

  /**
   * Call {@link #inDeleteBetweenGetEndOffsetAndFindKey} if it's not null. This is for testing only.
   */
  private void maybeCallInDeleteBetweenGetEndOffsetAndFindKey() throws Exception {
    Callable<Void> callable = inDeleteBetweenGetEndOffsetAndFindKey;
    if (callable != null) {
      callable.call();
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname,
      String remoteReplicaPath) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findEntriesSinceResponse.time();
    try {
      if (hostname != null && !hostname.startsWith(Cloud_Replica_Keyword)) {
        // only tokens from disk-backed replicas are tracked
        remoteTokenTracker.updateTokenFromPeerReplica(token, hostname, remoteReplicaPath);
      }
      FindInfo findInfo = index.findEntriesSince(token, maxTotalSizeOfEntries);
      onSuccess();
      return findInfo;
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } finally {
      context.stop();
    }
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findMissingKeysResponse.time();
    try {
      Set<StoreKey> missingKeys = index.findMissingKeys(keys);
      onSuccess();
      return missingKeys;
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } finally {
      context.stop();
    }
  }

  @Override
  public MessageInfo findKey(StoreKey key) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findKeyResponse.time();
    try {
      IndexValue value = index.findKey(key);
      if (value == null) {
        throw new StoreException("Key " + key + " not found in store. Cannot check if it is deleted",
            StoreErrorCodes.ID_Not_Found);
      }
      return new MessageInfo(key, value.getSize(), value.isDelete(), value.isTtlUpdate(), value.isUndelete(),
          value.getExpiresAtMs(), null, value.getAccountId(), value.getContainerId(), value.getOperationTimeInMs(),
          value.getLifeVersion());
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } finally {
      context.stop();
    }
  }

  @Override
  public StoreStats getStoreStats() {
    return blobStoreStats;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.isKeyDeletedResponse.time();
    try {
      IndexValue value = index.findKey(key);
      if (value == null) {
        throw new StoreException("Key " + key + " not found in store. Cannot check if it is deleted",
            StoreErrorCodes.ID_Not_Found);
      }
      return value.isDelete();
    } finally {
      context.stop();
    }
  }

  @Override
  public long getSizeInBytes() {
    return index.getLogUsedCapacity();
  }

  /**
   * @return The number of byte been written to log
   */
  public long getLogEndOffsetInBytes() {
    return log.getEndOffset().getOffset();
  }

  @Override
  public boolean isEmpty() {
    return index.isEmpty();
  }

  @Override
  public boolean isBootstrapInProgress() {
    return (new File(dataDir, BOOTSTRAP_FILE_NAME)).exists();
  }

  @Override
  public boolean isDecommissionInProgress() {
    // note that, the decommission file will be removed by calling deleteStoreFiles() when replica is being dropped. We
    // don't need to explicitly delete it. The file is also used for failure recovery to resume decommission process.
    return (new File(dataDir, DECOMMISSION_FILE_NAME)).exists();
  }

  @Override
  public void completeBootstrap() {
    File bootstrapFile = new File(dataDir, BOOTSTRAP_FILE_NAME);
    try {
      // the method will check if file exists or not
      Utils.deleteFileOrDirectory(bootstrapFile);
    } catch (IOException e) {
      // if deletion fails, we log here without throwing exception. Next time when server restarts, the store should
      // complete BOOTSTRAP -> STANDBY quickly and attempt to delete this again.
      logger.error("Failed to delete {}", bootstrapFile.getName(), e);
    }
  }

  @Override
  public void setCurrentState(ReplicaState state) {
    currentState = state;
  }

  @Override
  public ReplicaState getCurrentState() {
    return currentState;
  }

  @Override
  public boolean recoverFromDecommission() {
    return recoverFromDecommission;
  }

  @Override
  public boolean isDisabled() {
    return isDisabled.get();
  }

  /**
   * Fetch {@link CompactionDetails} based on the {@link CompactionPolicy} for this {@link BlobStore} containing
   * information about log segments to be compacted
   * @param compactionPolicy the {@link CompactionPolicy} that needs to be used to determine the {@link CompactionDetails}
   * @return the {@link CompactionDetails} containing information about log segments to be compacted. Could be
   * {@code null} if there isn't anything to compact
   * @throws StoreException on any issues while reading entries from index
   */
  CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
    return compactionPolicy.getCompactionDetails(capacityInBytes, index.getLogUsedCapacity(), log.getSegmentCapacity(),
        LogSegment.HEADER_SIZE, index.getLogSegmentsNotInJournal(), blobStoreStats, dataDir);
  }

  /**
   * Delete files of this store.
   * This is the last step to remove store from this node. Return swap segments (if any) to reserve pool and delete all
   * files/dirs associated with this store. This method is invoked by transition in AmbryStateModel (OFFLINE -> DROPPED)
   */
  public void deleteStoreFiles() throws StoreException, IOException {
    // Step 0: ensure the store has been shut down
    if (started) {
      throw new IllegalStateException("Store is still started. Deleting store files is not allowed.");
    }
    // Step 1: return occupied swap segments (if any) to reserve pool
    String[] swapSegmentsInUse = compactor.getSwapSegmentsInUse();
    for (String fileName : swapSegmentsInUse) {
      logger.info("Returning swap segment {} to reserve pool", fileName);
      File swapSegmentTempFile = new File(dataDir, fileName);
      diskSpaceAllocator.free(swapSegmentTempFile, config.storeSegmentSizeInBytes, storeId, true);
    }
    // Step 2: if segmented, delete remaining store segments in reserve pool
    if (log.isLogSegmented()) {
      logger.info("Deleting remaining segments associated with store {} in reserve pool", storeId);
      diskSpaceAllocator.deleteAllSegmentsForStoreIds(
          Collections.singletonList(replicaId.getPartitionId().toPathString()));
    }
    // Step 3: delete all files in current store directory
    logger.info("Deleting store {} directory", storeId);
    File storeDir = new File(dataDir);
    try {
      Utils.deleteFileOrDirectory(storeDir);
    } catch (Exception e) {
      throw new IOException("Couldn't delete store directory " + dataDir, e);
    }
    logger.info("All files of store {} are deleted", storeId);
  }

  /**
   * @return return absolute end position of last PUT in current store when this method is invoked.
   * @throws StoreException
   */
  @Override
  public long getEndPositionOfLastPut() throws StoreException {
    return index.getAbsoluteEndPositionOfLastPut();
  }

  /**
   * @return a {@link ReplicaStatusDelegate}(s) associated with this store
   */
  public List<ReplicaStatusDelegate> getReplicaStatusDelegates() {
    return replicaStatusDelegates;
  }

  @Override
  public void shutdown() throws StoreException {
    shutdown(false);
  }

  /**
   * Shuts down the store.
   * @param skipDiskFlush {@code true} should skip any disk flush operations during shutdown. {@code false} otherwise.
   * @throws StoreException
   */
  private void shutdown(boolean skipDiskFlush) throws StoreException {
    long startTimeInMs = time.milliseconds();
    synchronized (storeWriteLock) {
      checkStarted();
      try {
        logger.info("Store : {} shutting down", dataDir);
        blobStoreStats.close();
        compactor.close(30);
        index.close(skipDiskFlush);
        log.close(skipDiskFlush);
        metrics.deregisterMetrics(storeId);
        currentState = ReplicaState.OFFLINE;
        started = false;
      } catch (Exception e) {
        logger.error("Store : {} shutdown of store failed for directory ", dataDir, e);
      } finally {
        try {
          fileLock.destroy();
        } catch (IOException e) {
          logger.error("Store : {} IO Exception while trying to close the file lock", dataDir, e);
        }
        metrics.storeShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
      }
    }
  }

  /**
   * On an exception/error, if error count exceeds threshold, properly shutdown store.
   */
  private void onError() throws StoreException {
    int count = errorCount.incrementAndGet();
    if (count == config.storeIoErrorCountToTriggerShutdown) {
      logger.error("Shutting down BlobStore {} because IO error count exceeds threshold", storeId);
      shutdown(true);
      // Explicitly disable replica to trigger Helix state transition: LEADER -> STANDBY -> INACTIVE -> OFFLINE
      if (config.storeSetLocalPartitionStateEnabled && !isDisabled.getAndSet(true)
          && replicaStatusDelegates != null) {
        try {
          replicaStatusDelegates.forEach(delegate -> delegate.disableReplica(replicaId));
        } catch (Exception e) {
          logger.error("Failed to disable replica {} due to exception ", replicaId, e);
        }
      }
      metrics.storeIoErrorTriggeredShutdownCount.inc();
    }
  }

  /**
   * If store restarted successfully or at least one operation succeeded, reset the error count.
   */
  private void onSuccess() {
    errorCount.getAndSet(0);
  }

  /**
   * @return errorCount of store.
   */
  AtomicInteger getErrorCount() {
    return errorCount;
  }

  /**
   * Set isDisabled boolean value in this class.
   * @param disabled whether the store is disabled or not.
   */
  public void setDisableState(boolean disabled) {
    isDisabled.set(disabled);
  }

  /**
   * @return the {@link DiskSpaceRequirements} for this store to provide to
   * {@link DiskSpaceAllocator#initializePool(Collection)}. This will be {@code null} if this store uses a non-segmented
   * log. This is because it does not require any additional/swap segments.
   * @throws StoreException
   */
  DiskSpaceRequirements getDiskSpaceRequirements() throws StoreException {
    checkStarted();
    DiskSpaceRequirements requirements =
        log.isLogSegmented() ? new DiskSpaceRequirements(replicaId.getPartitionId().toPathString(),
            log.getSegmentCapacity(), log.getRemainingUnallocatedSegments(), compactor.getSwapSegmentsInUse().length)
            : null;
    logger.info("Store {} has disk space requirements: {}", storeId, requirements);
    return requirements;
  }

  /**
   * @return {@code true} if this store has been started successfully.
   */
  @Override
  public boolean isStarted() {
    return started;
  }

  /**
   * Compacts the store data based on {@code details}.
   * @param details the {@link CompactionDetails} describing what needs to be compacted.
   * @param bundleReadBuffer the preAllocated buffer for bundle read in compaction copy phase.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
   * in the segments to compact are in the journal.
   * @throws IOException if there is any error creating the {@link CompactionLog}.
   * @throws StoreException if there are any errors during the compaction.
   */
  void compact(CompactionDetails details, byte[] bundleReadBuffer) throws IOException, StoreException {
    checkStarted();
    remoteTokenTracker.refreshPeerReplicaTokens();
    compactor.compact(details, bundleReadBuffer);
    checkCapacityAndUpdateReplicaStatusDelegate();
    logger.info("One cycle of compaction is completed on the store {}", storeId);
  }

  /**
   * Resumes a compaction if one is in progress.
   * @throws StoreException if there are any errors during the compaction.
   * @param bundleReadBuffer the preAllocated buffer for bundle read in compaction copy phase.
   */
  void maybeResumeCompaction(byte[] bundleReadBuffer) throws StoreException {
    checkStarted();
    if (CompactionLog.isCompactionInProgress(dataDir, storeId)) {
      logger.info("Resuming compaction of {}", this);
      remoteTokenTracker.refreshPeerReplicaTokens();
      compactor.resumeCompaction(bundleReadBuffer);
      checkCapacityAndUpdateReplicaStatusDelegate();
    }
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }

  /**
   * Detects duplicates in {@code writeSet}
   * @param infos the list of {@link MessageInfo} to detect duplicates in
   */
  private void checkDuplicates(List<MessageInfo> infos) {
    if (infos.size() > 1) {
      Set<StoreKey> seenKeys = new HashSet<>();
      for (MessageInfo info : infos) {
        if (!seenKeys.add(info.getStoreKey())) {
          metrics.duplicateKeysInBatch.inc();
          throw new IllegalArgumentException("WriteSet contains duplicates. Duplicate detected: " + info.getStoreKey());
        }
      }
    }
  }

  /**
   * Resolve initial state of current store.
   */
  private void resolveStoreInitialState() {
    // Determine if HelixParticipant is adopted.
    if (replicaStatusDelegates != null && !replicaStatusDelegates.isEmpty() && replicaStatusDelegates.get(0)
        .supportsStateChanges()) {
      // If store is managed by Helix, Helix controller will update its state after participation.
      currentState = ReplicaState.OFFLINE;
    } else {
      // This is to be compatible with static clustermap. If static cluster manager is adopted, all replicas are supposed to be STANDBY
      // and router relies on failure detector to track liveness of replicas(stores).
      currentState = ReplicaState.STANDBY;
    }
  }

  @Override
  public String toString() {
    return "StoreId: " + storeId + ". DataDir: " + dataDir + ". Capacity: " + capacityInBytes;
  }
}
