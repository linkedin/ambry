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
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The blob store that controls the log and index
 */
class BlobStore implements Store {
  static final String SEPARATOR = "_";
  private final static String LockFile = ".lock";

  private final String storeId;
  private final String dataDir;
  private final ScheduledExecutorService taskScheduler;
  private final ScheduledExecutorService longLivedTaskScheduler;
  private final DiskIOScheduler diskIOScheduler;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final Logger logger = LoggerFactory.getLogger(getClass());
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
  private final WriteStatusDelegate writeStatusDelegate;
  private final long thresholdBytesHigh;
  private final long thresholdBytesLow;

  private Log log;
  private BlobStoreCompactor compactor;
  private PersistentIndex index;
  private BlobStoreStats blobStoreStats;
  private boolean started;
  private FileLock fileLock;

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
   * @param writeStatusDelegate delegate used to communicate BlobStore write status (sealed or unsealed)
   * @param time the {@link Time} instance to use.
   */
  BlobStore(ReplicaId replicaId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      WriteStatusDelegate writeStatusDelegate, Time time) {
    this(replicaId, replicaId.getPartitionId().toString(), config, taskScheduler, longLivedTaskScheduler,
        diskIOScheduler, diskSpaceAllocator, metrics, storeUnderCompactionMetrics, replicaId.getReplicaPath(),
        replicaId.getCapacityInBytes(), factory, recovery, hardDelete, writeStatusDelegate, time);
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
        storeUnderCompactionMetrics, dataDir, capacityInBytes, factory, recovery, hardDelete, null, time);
  }

  private BlobStore(ReplicaId replicaId, String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, WriteStatusDelegate writeStatusDelegate, Time time) {
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
    this.writeStatusDelegate = config.storeWriteStatusDelegateEnable ? writeStatusDelegate : null;
    this.time = time;
    long threshold = config.storeReadOnlyEnableSizeThresholdPercentage;
    long delta = config.storeReadWriteEnableSizeThresholdPercentageDelta;
    this.thresholdBytesHigh = (long) (capacityInBytes * (threshold / 100.0));
    this.thresholdBytesLow = (long) (capacityInBytes * ((threshold - delta) / 100.0));
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

        StoreDescriptor storeDescriptor = new StoreDescriptor(dataDir);
        log = new Log(dataDir, capacityInBytes, config.storeSegmentSizeInBytes, diskSpaceAllocator, metrics);
        compactor = new BlobStoreCompactor(dataDir, storeId, factory, config, metrics, storeUnderCompactionMetrics,
            diskIOScheduler, diskSpaceAllocator, log, time, sessionId, storeDescriptor.getIncarnationId());
        index = new PersistentIndex(dataDir, storeId, taskScheduler, log, config, factory, recovery, hardDelete,
            diskIOScheduler, metrics, time, sessionId, storeDescriptor.getIncarnationId());
        compactor.initialize(index);
        metrics.initializeIndexGauges(storeId, index, capacityInBytes);
        long logSegmentForecastOffsetMs = TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays);
        long bucketSpanInMs = TimeUnit.MINUTES.toMillis(config.storeStatsBucketSpanInMinutes);
        long queueProcessingPeriodInMs =
            TimeUnit.MINUTES.toMillis(config.storeStatsRecentEntryProcessingIntervalInMinutes);
        blobStoreStats =
            new BlobStoreStats(storeId, index, config.storeStatsBucketCount, bucketSpanInMs, logSegmentForecastOffsetMs,
                queueProcessingPeriodInMs, config.storeStatsWaitTimeoutInSecs, time, longLivedTaskScheduler,
                taskScheduler, diskIOScheduler, metrics);
        checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
        started = true;
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
    try {
      List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>(ids.size());
      Map<StoreKey, MessageInfo> indexMessages = new HashMap<StoreKey, MessageInfo>(ids.size());
      for (StoreKey key : ids) {
        BlobReadOptions readInfo = index.getBlobReadInfo(key, storeGetOptions);
        readOptions.add(readInfo);
        indexMessages.put(key, readInfo.getMessageInfo());
        // validate accountId and containerId
        if (!validateAuthorization(readInfo.getMessageInfo().getAccountId(), readInfo.getMessageInfo().getContainerId(),
            key.getAccountId(), key.getContainerId())) {
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
      return new StoreInfo(readSet, messageInfoList);
    } catch (StoreException e) {
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
   * @throws StoreException relays those encountered from {@link PersistentIndex#findKey(StoreKey, FileSpan)}.
   */
  private MessageWriteSetStateInStore checkWriteSetStateInStore(MessageWriteSet messageSetToWrite, FileSpan fileSpan)
      throws StoreException {
    int existingIdenticalEntries = 0;
    for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
      if (index.findKey(info.getStoreKey(), fileSpan) != null) {
        if (index.wasRecentlySeen(info)) {
          existingIdenticalEntries++;
          metrics.identicalPutAttemptCount.inc();
        } else {
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
   * Checks the used capacity of the store against the configured percentage thresholds to see if the store
   * should be read-only or read-write
   * @param totalCapacity total capacity of the store in bytes
   * @param usedCapacity total used capacity of the store in bytes
   */
  private void checkCapacityAndUpdateWriteStatusDelegate(long totalCapacity, long usedCapacity) {
    if (writeStatusDelegate != null) {
      if (index.getLogUsedCapacity() > thresholdBytesHigh && !replicaId.isSealed()) {
        if (!writeStatusDelegate.seal(replicaId)) {
          metrics.sealSetError.inc();
        }
      } else if (index.getLogUsedCapacity() <= thresholdBytesLow && replicaId.isSealed()) {
        if (!writeStatusDelegate.unseal(replicaId)) {
          metrics.unsealSetError.inc();
        }
      }
      //else: maintain current replicaId status if percentFilled between threshold - delta and threshold
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.putResponse.time();
    try {
      if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
        throw new IllegalArgumentException("Message write set cannot be empty");
      }
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      MessageWriteSetStateInStore state = checkWriteSetStateInStore(messageSetToWrite, null);
      if (state == MessageWriteSetStateInStore.ALL_ABSENT) {
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
              IndexValue value = new IndexValue(info.getSize(), fileSpan.getStartOffset(), info.getExpirationTimeInMs(),
                  info.getOperationTimeMs(), info.getAccountId(), info.getContainerId());
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
            checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
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
    } catch (StoreException e) {
      throw e;
    } catch (IOException e) {
      throw new StoreException("IO error while trying to put blobs to store " + dataDir, e, StoreErrorCodes.IOError);
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to put blobs to store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.deleteResponse.time();
    try {
      List<IndexValue> indexValuesToDelete = new ArrayList<>();
      List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      for (MessageInfo info : infoList) {
        IndexValue value = index.findKey(info.getStoreKey());
        if (value == null) {
          throw new StoreException("Cannot delete id " + info.getStoreKey() + " since it is not present in the index.",
              StoreErrorCodes.ID_Not_Found);
        } else if (!validateAuthorization(value.getAccountId(), value.getContainerId(), info.getAccountId(),
            info.getContainerId())) {
          if (config.storeValidateAuthorization) {
            throw new StoreException("DELETE authorization failure. Key: " + info.getStoreKey() + "Actually accountId: "
                + value.getAccountId() + "Actually containerId: " + value.getContainerId(),
                StoreErrorCodes.Authorization_Failure);
          } else {
            logger.warn("DELETE authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                info.getStoreKey(), value.getAccountId(), value.getContainerId());
            metrics.deleteAuthorizationFailureCount.inc();
          }
        } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
          throw new StoreException(
              "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
              StoreErrorCodes.ID_Deleted);
        }
        indexValuesToDelete.add(value);
      }
      synchronized (storeWriteLock) {
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          for (MessageInfo info : infoList) {
            IndexValue value = index.findKey(info.getStoreKey(), fileSpan);
            if (value != null && value.isFlagSet(IndexValue.Flags.Delete_Index)) {
              throw new StoreException(
                  "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                  StoreErrorCodes.ID_Deleted);
            }
          }
        }
        Offset endOffsetOfLastMessage = log.getEndOffset();
        messageSetToDelete.writeTo(log);
        logger.trace("Store : {} delete mark written to log", dataDir);
        int correspondingPutIndex = 0;
        for (MessageInfo info : infoList) {
          FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
          IndexValue deleteIndexValue = index.markAsDeleted(info.getStoreKey(), fileSpan, info.getOperationTimeMs());
          endOffsetOfLastMessage = fileSpan.getEndOffset();
          blobStoreStats.handleNewDeleteEntry(deleteIndexValue, indexValuesToDelete.get(correspondingPutIndex++));
        }
        logger.trace("Store : {} delete has been marked in the index ", dataDir);
      }
    } catch (StoreException e) {
      throw e;
    } catch (IOException e) {
      throw new StoreException("IO error while trying to delete blobs from store " + dataDir, e,
          StoreErrorCodes.IOError);
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    } finally {
      context.stop();
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findEntriesSinceResponse.time();
    try {
      return index.findEntriesSince(token, maxTotalSizeOfEntries);
    } finally {
      context.stop();
    }
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findMissingKeysResponse.time();
    try {
      return index.findMissingKeys(keys);
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
      return value.isFlagSet(IndexValue.Flags.Delete_Index);
    } finally {
      context.stop();
    }
  }

  @Override
  public long getSizeInBytes() {
    return index.getLogUsedCapacity();
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
        LogSegment.HEADER_SIZE, index.getLogSegmentsNotInJournal(), blobStoreStats);
  }

  @Override
  public void shutdown() throws StoreException {
    long startTimeInMs = time.milliseconds();
    synchronized (storeWriteLock) {
      checkStarted();
      try {
        logger.info("Store : " + dataDir + " shutting down");
        blobStoreStats.close();
        compactor.close(30);
        index.close();
        log.close();
        started = false;
      } catch (Exception e) {
        logger.error("Store : " + dataDir + " shutdown of store failed for directory ", e);
      } finally {
        try {
          fileLock.destroy();
        } catch (IOException e) {
          logger.error("Store : " + dataDir + " IO Exception while trying to close the file lock", e);
        }
        metrics.storeShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
      }
    }
  }

  /**
   * @return the {@link DiskSpaceRequirements} for this store to provide to
   * {@link DiskSpaceAllocator#initializePool(Collection)}. This will be {@code null} if this store uses a non-segmented
   * log. This is because it does not require any additional/swap segments.
   * @throws StoreException
   */
  DiskSpaceRequirements getDiskSpaceRequirements() throws StoreException {
    checkStarted();
    DiskSpaceRequirements requirements = log.isLogSegmented() ? new DiskSpaceRequirements(log.getSegmentCapacity(),
        log.getRemainingUnallocatedSegments(), compactor.getSwapSegmentsInUse()) : null;
    logger.debug("Store {} has disk space requirements: {}", storeId, requirements);
    return requirements;
  }

  /**
   * @return {@code true} if this store has been started successfully.
   */
  boolean isStarted() {
    return started;
  }

  /**
   * Compacts the store data based on {@code details}.
   * @param details the {@link CompactionDetails} describing what needs to be compacted.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
   * in the segments to compact are in the journal.
   * @throws IOException if there is any error creating the {@link CompactionLog}.
   * @throws StoreException if there are any errors during the compaction.
   */
  void compact(CompactionDetails details) throws IOException, StoreException {
    checkStarted();
    compactor.compact(details);
    checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
  }

  /**
   * Resumes a compaction if one is in progress.
   * @throws StoreException if there are any errors during the compaction.
   */
  void maybeResumeCompaction() throws StoreException {
    checkStarted();
    if (CompactionLog.isCompactionInProgress(dataDir, storeId)) {
      logger.info("Resuming compaction of {}", this);
      compactor.resumeCompaction();
      checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
    }
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }

  @Override
  public String toString() {
    return "StoreId: " + storeId + ". DataDir: " + dataDir + ". Capacity: " + capacityInBytes;
  }

  /**
   * Check if accountId/containerId from store and request are same.
   * If either one of accountId and containerId in store is unknown, the validation is skipped.
   */
  private boolean validateAuthorization(short storeAccountId, short storeContainerId, short requestAccountId,
      short requestContainerId) {
    if (storeAccountId != Account.UNKNOWN_ACCOUNT_ID && storeContainerId != Container.UNKNOWN_CONTAINER_ID) {
      return (storeAccountId == requestAccountId) && (storeContainerId == requestContainerId);
    }
    return true;
  }
}
