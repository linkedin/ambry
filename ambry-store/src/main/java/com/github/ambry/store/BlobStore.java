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
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.TtlUpdateMessageFormatInputStream;
import com.github.ambry.messageformat.UndeleteMessageFormatInputStream;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.VcrClusterParticipant.*;


/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {
  private static final Logger logger = LoggerFactory.getLogger(BlobStore.class);
  static final String SEPARATOR = "_";
  static final String DECOMMISSION_FILE_NAME = "decommission_in_progress";
  final static String LockFile = ".lock";

  private final String storeId;
  private final String dataDir;
  private final DiskMetrics diskMetrics;
  private final ScheduledExecutorService taskScheduler;
  private final ScheduledExecutorService longLivedTaskScheduler;
  private final DiskManager diskManager;
  private final DiskIOScheduler diskIOScheduler;
  private final DiskSpaceAllocator diskSpaceAllocator;
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
  private final long sealThresholdBytesHigh;
  private final long sealThresholdBytesLow;
  private final long partialSealThresholdBytesHigh;
  private final long partialSealThresholdBytesLow;
  private final long ttlUpdateBufferTimeMs;
  private final RemoteTokenTracker remoteTokenTracker;
  private final AtomicInteger errorCount;
  private final AccountService accountService;
  private final ScheduledExecutorService indexPersistScheduler;
  private Log log;
  private BlobStoreCompactor compactor;
  private BlobStoreStats blobStoreStats;
  private boolean started;
  private boolean initialized;
  private final FileStore fileStore;
  private FileLock fileLock;
  private volatile ReplicaState currentState;
  private volatile ReplicaState previousState;

  private StoreDescriptor storeDescriptor;
  private volatile boolean recoverFromDecommission;
  // TODO remove this once ZK migration is complete
  private AtomicReference<ReplicaSealStatus> replicaSealStatus = new AtomicReference<>(ReplicaSealStatus.NOT_SEALED);
  private AtomicBoolean isDisabled = new AtomicBoolean(false);
  // how many BlobStores are stale
  static final AtomicInteger staleBlobCount = new AtomicInteger(0);
  protected PersistentIndex index;

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
   *
   * @param replicaId                   replica associated with BlobStore.  BlobStore id, data directory, and capacity
   *                                    derived from this
   * @param config                      the settings for store configuration.
   * @param taskScheduler               the {@link ScheduledExecutorService} for executing short period background
   *                                    tasks.
   * @param longLivedTaskScheduler      the {@link ScheduledExecutorService} for executing long period background
   *                                    tasks.
   * @param diskManager                 the {@link DiskManager} object
   * @param diskIOScheduler             schedules disk IO operations
   * @param diskSpaceAllocator          allocates log segment files.
   * @param metrics                     the {@link StorageManagerMetrics} instance to use.
   * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
   * @param factory                     the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery                    the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete                  the {@link MessageStoreHardDelete} instance to use.
   * @param replicaStatusDelegates      delegates used to communicate BlobStore write status(sealed/unsealed,
   *                                    stopped/started)
   * @param time                        the {@link Time} instance to use.
   * @param accountService              the {@link AccountService} instance to use.
   * @param diskMetrics                 the {@link DiskMetrics} for the disk of this {@link BlobStore}
   * @param indexPersistScheduler       a dedicated {@link ScheduledExecutorService} for persisting index segments.
   */
  public BlobStore(ReplicaId replicaId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskManager diskManager, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      List<ReplicaStatusDelegate> replicaStatusDelegates, Time time, AccountService accountService,
      DiskMetrics diskMetrics, ScheduledExecutorService indexPersistScheduler) throws StoreException {
    this(replicaId, replicaId.getPartitionId().toString(), config, taskScheduler, longLivedTaskScheduler, diskManager,
        diskIOScheduler, diskSpaceAllocator, metrics, storeUnderCompactionMetrics, replicaId.getReplicaPath(),
        replicaId.getCapacityInBytes(), factory, recovery, hardDelete, replicaStatusDelegates, time, accountService,
        null, diskMetrics, indexPersistScheduler);
  }

  /**
   * Constructor for BlobStore, used in ambry-tools
   *
   * @param storeId                     id of the BlobStore
   * @param config                      the settings for store configuration.
   * @param taskScheduler               the {@link ScheduledExecutorService} for executing background tasks.
   * @param longLivedTaskScheduler      the {@link ScheduledExecutorService} for executing long period background
   *                                    tasks.
   * @param diskManager                 the {@link DiskManager} object
   * @param diskIOScheduler             schedules disk IO operations.
   * @param diskSpaceAllocator          allocates log segment files.
   * @param metrics                     the {@link StorageManagerMetrics} instance to use.
   * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
   * @param dataDir                     directory that will be used by the BlobStore for data
   * @param capacityInBytes             capacity of the BlobStore
   * @param factory                     the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery                    the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete                  the {@link MessageStoreHardDelete} instance to use.
   * @param time                        the {@link Time} instance to use.
   * @param indexPersistScheduler       a dedicated {@link ScheduledExecutorService} for persisting index segments.
   */
  // TODO [TOMBSTONE]: deprecate this constructor. ReplicaId cannot be null. Do we still need the StoreCopier.class
  BlobStore(String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskManager diskManager, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time, ScheduledExecutorService indexPersistScheduler)
      throws StoreException {
    this(null, storeId, config, taskScheduler, longLivedTaskScheduler, diskManager, diskIOScheduler, diskSpaceAllocator,
        metrics, storeUnderCompactionMetrics, dataDir, capacityInBytes, factory, recovery, hardDelete, null, time, null,
        null, null, indexPersistScheduler);
  }

  BlobStore(ReplicaId replicaId, String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
      ScheduledExecutorService longLivedTaskScheduler, DiskManager diskManager, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
      String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, List<ReplicaStatusDelegate> replicaStatusDelegates, Time time,
      AccountService accountService, BlobStoreStats blobStoreStats, DiskMetrics diskMetrics,
      ScheduledExecutorService indexPersistScheduler) throws StoreException {
    this.replicaId = replicaId;
    this.storeId = storeId;
    this.dataDir = dataDir;
    this.diskMetrics = diskMetrics;
    this.taskScheduler = taskScheduler;
    this.longLivedTaskScheduler = longLivedTaskScheduler;
    this.diskManager = diskManager;
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
    this.indexPersistScheduler = indexPersistScheduler;
    this.replicaStatusDelegates = config.storeReplicaStatusDelegateEnable ? replicaStatusDelegates : null;
    this.time = time;
    this.sealThresholdBytesHigh =
        (long) (capacityInBytes * (config.storeReadOnlyEnableSizeThresholdPercentage / 100.0));
    this.sealThresholdBytesLow = (long) (capacityInBytes * ((config.storeReadOnlyEnableSizeThresholdPercentage
        - config.storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDelta) / 100.0));
    this.partialSealThresholdBytesHigh =
        (long) (capacityInBytes * (config.storePartialWriteEnableSizeThresholdPercentage / 100.0));
    this.partialSealThresholdBytesLow = (long) (capacityInBytes * (
        (config.storePartialWriteEnableSizeThresholdPercentage
            - config.storePartialWriteToReadWriteEnableSizeThresholdPercentageDelta) / 100.0));
    this.blobStoreStats = blobStoreStats; // Only in test will this be non-null
    ttlUpdateBufferTimeMs = TimeUnit.SECONDS.toMillis(config.storeTtlUpdateBufferTimeSeconds);
    errorCount = new AtomicInteger(0);
    currentState = ReplicaState.OFFLINE;
    previousState = ReplicaState.OFFLINE;
    remoteTokenTracker = new RemoteTokenTracker(replicaId, taskScheduler, factory, time);
    logger.debug(
        "The enable state of replicaStatusDelegate is {} on store {}. The high threshold for seal is {} bytes and the"
            + "low threshold for seal is {} bytes. The high threshold for partial seal is {} bytes and low threshold of"
            + "partial seal is {} bytes", config.storeReplicaStatusDelegateEnable, storeId, this.sealThresholdBytesHigh,
        this.sealThresholdBytesLow, this.partialSealThresholdBytesHigh, this.partialSealThresholdBytesLow);
    // if there is a decommission file in store dir, that means previous decommission didn't complete successfully.
    recoverFromDecommission = isDecommissionInProgress();
    fileStore = new FileStore(dataDir);
  }

  @Override
  public void initialize() throws StoreException {
    synchronized (storeWriteLock) {
      if (started) {
        throw new StoreException("Store already started", StoreErrorCodes.StoreAlreadyStarted);
      }
      if(initialized) {
        throw new StoreException("Store already initialized", StoreErrorCodes.StoreAlreadyInitialized);
      }
      try {
        // Check if the data dir exist. If it does not exist, create it
        File dataFile = new File(dataDir);
        if (!dataFile.exists()) {
          logger.info("Store : {} data directory not found. creating it", dataDir);
          boolean created = dataFile.mkdir();
          if (!created) {
            throw new StoreException("Failed to create directory for data dir " + dataDir,
                StoreErrorCodes.InitializationError);
          }
        }
        if (!dataFile.isDirectory() || !dataFile.canRead()) {
          throw new StoreException(dataFile.getAbsolutePath() + " is either not a directory or is not readable",
              StoreErrorCodes.InitializationError);
        }
        // check the file system before we do any file write.
        checkIfStoreIsStale();

        // lock the directory
        fileLock = new FileLock(new File(dataDir, LockFile));
        if (!fileLock.tryLock()) {
          throw new StoreException(
              "Failed to acquire lock on file " + dataDir + ". Another process or thread is using this directory.",
              StoreErrorCodes.InitializationError);
        }

        storeDescriptor = new StoreDescriptor(dataDir, config);
        fileStore.start();
        initialized = true;
      } catch (Exception e) {
        if (fileLock != null) {
          // Release the file lock
          try {
            fileLock.unlock();
          } catch (Exception lockException) {
            logger.error("Failed to unlock file lock for dir " + dataDir, lockException);
          }
        }
        String err = String.format("Error while starting store for dir %s due to %s", dataDir, e.getMessage());
        throw new StoreException(err, e, StoreErrorCodes.InitializationError);
      }
    }
  }

  @Override
  public void load() throws StoreException {
    synchronized (storeWriteLock) {
      if (started) {
        throw new StoreException("Store already started", StoreErrorCodes.StoreAlreadyStarted);
      }
      if (!initialized) {
        throw new StoreException("Store not initialized", StoreErrorCodes.StoreNotInitialized);
      }
      try {
        log = new Log(dataDir, capacityInBytes, diskSpaceAllocator, config, metrics, diskMetrics);
        compactor = new BlobStoreCompactor(dataDir, storeId, factory, config, metrics, storeUnderCompactionMetrics,
            diskIOScheduler, diskSpaceAllocator, log, time, sessionId, storeDescriptor.getIncarnationId(),
            accountService, remoteTokenTracker, diskMetrics);
        index = new PersistentIndex(dataDir, storeId, indexPersistScheduler, log, config, factory, recovery, hardDelete,
            diskIOScheduler, metrics, time, sessionId, storeDescriptor.getIncarnationId());
        compactor.initialize(index);
        if (config.storeRebuildTokenBasedOnCompactionHistory) {
          compactor.enablePersistIndexSegmentOffsets();
          index.enableRebuildTokenBasedOnCompactionHistory();
          buildCompactionHistory();
        }
        if (blobStoreStats == null) {
          blobStoreStats =
              new BlobStoreStats(storeId, index, config, time, longLivedTaskScheduler, taskScheduler, diskIOScheduler,
                  metrics);
        }
        metrics.initializeIndexGauges(storeId, index, capacityInBytes, blobStoreStats,
            config.storeEnableCurrentInvalidSizeMetric, config.storeEnableIndexDirectMemoryUsageMetric);
        checkCapacityAndUpdateReplicaStatusDelegate();
        remoteTokenTracker.start(config.storePersistRemoteTokenIntervalInSeconds);
        logger.trace("The store {} is successfully started", storeId);
        onSuccess("START");
        isDisabled.set(false);
        started = true;
        resolveStoreInitialState();
        if (replicaId != null) {
          replicaId.markDiskUp();
        }
        enableReplicaIfNeeded();
      } catch (Exception e) {
        if (fileLock != null) {
          // Release the file lock
          try {
            fileLock.unlock();
          } catch (Exception lockException) {
            logger.error("Failed to unlock file lock for dir " + dataDir, lockException);
          }
        }
        initialized = false;
        String err = String.format("Error while starting store for dir %s due to %s", dataDir, e.getMessage());
        throw new StoreException(err, e, StoreErrorCodes.InitializationError);
      }
    }
  }

  @Override
  public void start() throws StoreException {
    synchronized (storeWriteLock) {
      if (started) {
        throw new StoreException("Store already started", StoreErrorCodes.StoreAlreadyStarted);
      }
      final Timer.Context context = metrics.storeStartTime.time();
      try {
        initialize();
        load();
      } finally {
        context.stop();
      }
    }
  }

  public FileStore getFileStore(){
    return fileStore;
  }

  /**
   * Returns CRC of blob-content
   * @param msg Metadata of blob
   * @return CRC of blob-content
   */
  @Override
  public Long getBlobContentCRC(MessageInfo msg) throws StoreException, IOException {
    EnumSet<StoreGetOptions> storeGetOptions =
        EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired);
    MessageReadSet rdset = null;
    try {
      StoreInfo stinfo = this.get(Collections.singletonList(msg.getStoreKey()), storeGetOptions);
      rdset = stinfo.getMessageReadSet();
      MessageInfo minfo = stinfo.getMessageReadSetInfo().get(0);
      rdset.doPrefetch(0, minfo.getSize() - MessageFormatRecord.Crc_Size, MessageFormatRecord.Crc_Size);
      return rdset.getPrefetchedData(0).getLong(0);
    } finally {
      if (rdset != null && rdset.count() > 0 && rdset.getPrefetchedData(0) != null) {
        rdset.getPrefetchedData(0).release();
      }
    }
  }

  /**
   * Find the last modified file in the folder
   */
  private File getLatestFilefromDir(String dirPath) {
    File dir = new File(dirPath);
    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      return null;
    }

    File lastModifiedFile = files[0];
    for (int i = 1; i < files.length; i++) {
      if (lastModifiedFile.lastModified() < files[i].lastModified()) {
        lastModifiedFile = files[i];
      }
    }
    return lastModifiedFile;
  }

  /**
   * Check if the store is stale.
   * If yes, throw exception
   */
  private void checkIfStoreIsStale() throws StoreException {
    File lastModifiedFile = getLatestFilefromDir(replicaId.getReplicaPath());

    long lastModifiedTimeInMilliseconds = time.milliseconds();
    if (lastModifiedFile != null) {
      lastModifiedTimeInMilliseconds = lastModifiedFile.lastModified();
    }
    Date date = new Date(lastModifiedTimeInMilliseconds);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    String lastAliveTime = formatter.format(date);
    logger.info(
        "checkIfStoreIsStale " + replicaId.getReplicaPath() + " last modified file " + ((lastModifiedFile != null)
            ? lastModifiedFile.getAbsolutePath() : "") + " last alive time " + lastAliveTime);

    if (config.storeStaleTimeInDays != 0
        && lastModifiedTimeInMilliseconds + TimeUnit.DAYS.toMillis(config.storeStaleTimeInDays) < time.milliseconds()) {
      logger.error("checkIfStoreIsStale " + replicaId.getReplicaPath() + " is stale. ");
      BlobStore.staleBlobCount.getAndIncrement();
      if (config.storeBlockStaleBlobStoreToStart) {
        throw new StoreException("BlobStore " + dataDir + " is stale ", StoreErrorCodes.StoreStaleError);
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
                StoreErrorCodes.AuthorizationFailure);
          } else {
            logger.warn("GET authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                key.getID(), readInfo.getMessageInfo().getAccountId(), readInfo.getMessageInfo().getContainerId());
            metrics.getAuthorizationFailureCount.inc();
          }
        }
      }

      // We call onSuccess and onError in the MessageReadSet IOPHandler instead of at the end of this method call like
      // other write methods because we are not really reading any bytes from disks in the get method, we are only
      // returning an object that triggers the reads when in different thread.
      // Thus we have to to call onSuccess and onError when we actually read bytes from disk in MessageReadSet.
      MessageReadSet readSet = new StoreMessageReadSet(readOptions, new StoreMessageReadSet.IOPHandler() {
        @Override
        public void onSuccess() {
          BlobStore.this.onSuccess("GET");
        }

        @Override
        public void onError() {
          try {
            BlobStore.this.onError();
          } catch (Exception e) {
          }
        }
      });
      // We ensure that the metadata list is ordered with the order of the message read set view that the
      // log provides. This ensures ordering of all messages across the log and metadata from the index.
      List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(readSet.count());
      for (int i = 0; i < readSet.count(); i++) {
        messageInfoList.add(indexMessages.get(readSet.getKeyAt(i)));
      }
      return new StoreInfo(readSet, messageInfoList);
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown exception while trying to fetch blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
    } finally {
      context.stop();
    }
  }

  public ReplicaId getReplicaId() {
    return this.replicaId;
  }

  String getDataDir() {
    return dataDir;
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
        if (index.wasRecentlySeenOrCrcIsNull(info)) {
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
      updateSealedStatus();
      if (!started && replicaStatusDelegates.size() > 1) {
        reconcileSealedStatus();
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
    checkPartition(messageSetToWrite.getMessageSetInfo());
    final Timer.Context context = metrics.putResponse.time();
    try {
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      MessageWriteSetStateInStore state =
          checkWriteSetStateInStore(messageSetToWrite, new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
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
            long diskWriteStartTime = time.milliseconds();
            long sizeWritten = messageSetToWrite.writeTo(log);

            if (diskMetrics != null) {
              diskMetrics.diskWriteTimePerMbInMs.update(
                  ((time.milliseconds() - diskWriteStartTime) << 20) / sizeWritten);
            }
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
              blobStoreStats.handleNewPutEntry(newEntry.getKey(), newEntry.getValue());
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
              StoreErrorCodes.AlreadyExist);
        case SOME_NOT_ALL_DUPLICATE:
          throw new StoreException(
              "At least one message but not all in the write set is identical to an existing entry",
              StoreErrorCodes.AlreadyExist);
        case ALL_DUPLICATE:
          logger.trace("All entries to put already exist in the store, marking operation as successful");
          break;
        case ALL_ABSENT:
          logger.trace("All entries were absent, and were written to the store successfully");
          break;
      }
      onSuccess("PUT");
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to put blobs to store " + dataDir, e,
          StoreErrorCodes.UnknownError);
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
      for (MessageInfo info : infosToDelete) {
        validateMessageInfoForDelete(info, indexEndOffsetBeforeCheck, indexValuesPriorToDelete, lifeVersions,
            originalPuts);
      }
      synchronized (storeWriteLock) {
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          int i = 0;
          for (MessageInfo info : infosToDelete) {
            IndexValue value =
                index.findKey(info.getStoreKey(), fileSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
            if (value != null) {
              validateDeleteForDuplicateAndLifeVersioConflicts(info, value, lifeVersions.get(i));
              indexValuesPriorToDelete.set(i, value);
            }
            i++;
          }
        }
        List<InputStream> inputStreams = new ArrayList<>(infosToDelete.size());
        List<MessageInfo> updatedInfos = new ArrayList<>(infosToDelete.size());
        Offset endOffsetOfLastMessage =
            writeDeleteMessagesToLogSegment(infosToDelete, inputStreams, updatedInfos, lifeVersions);
        logger.trace("Store : {} delete mark written to log", dataDir);
        updateIndexAndStats(updatedInfos, endOffsetOfLastMessage, originalPuts, indexValuesPriorToDelete);
      }
      onSuccess("DELETE");
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
    } finally {
      context.stop();
    }
  }

  @Override
  public StoreBatchDeleteInfo batchDelete(List<MessageInfo> infosToDelete) throws StoreException {
    checkStarted();
    List<MessageInfo> infosToDeleteCopy = new ArrayList<>(infosToDelete);
    checkDuplicates(infosToDelete);
    StoreBatchDeleteInfo storeBatchDeleteInfo = new StoreBatchDeleteInfo(replicaId.getPartitionId(), new ArrayList<>());
    final Timer.Context context = metrics.batchDeleteResponse.time();
    try {
      List<IndexValue> indexValuesPriorToDelete = new ArrayList<>();
      List<IndexValue> originalPuts = new ArrayList<>();
      List<Short> lifeVersions = new ArrayList<>();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      // Update infosToDelete to filteredInfosToDelete returned from this function.
      infosToDelete = validateAndFilterMessageInfosForBatchDelete(infosToDelete, indexEndOffsetBeforeCheck,
          indexValuesPriorToDelete, lifeVersions, originalPuts, storeBatchDeleteInfo);
      if (infosToDelete.isEmpty()) {
        logger.trace("Batch Delete completely failed since all blobs failed. Received InfosToDelete: {}",
            infosToDeleteCopy);
        return storeBatchDeleteInfo;
      }
      synchronized (storeWriteLock) {
        // Update storeBatchDeleteInfo for the remaining infosToDelete via store lock operations.
        validateAndBatchDelete(infosToDelete, indexEndOffsetBeforeCheck, indexValuesPriorToDelete, lifeVersions,
            originalPuts, storeBatchDeleteInfo, infosToDeleteCopy);
      }
      onSuccess("BATCH_DELETE");
      return storeBatchDeleteInfo;
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
    } finally {
      context.stop();
    }
  }

  @Override
  public void purge(List<MessageInfo> infosToPurge) throws StoreException {
    // TODO Efficient_Metadata_Operations_TODO : implement purge.
  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) throws StoreException {
    checkStarted();
    checkDuplicates(infosToDelete);
    final Timer.Context context = metrics.deleteResponse.time();
    try {
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      // If the PubBlob exists, we are supposed to call BlobStore.delete.
      // forceDelete is supposed to use when the blob doesn't exist.
      for (MessageInfo info : infosToDelete) {
        IndexValue value =
            index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
        if (value != null) {
          throw new StoreException("Cannot force delete id " + info.getStoreKey() + " since the id exists",
              StoreErrorCodes.AlreadyExist);
        }
      }
      synchronized (storeWriteLock) {
        // findKey again with the file span from the previous end offset to the new end offset
        // to make sure there is no new entry is added while we acquire the lock.
        Offset currentIndexEndOffset = index.getCurrentEndOffset();
        if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
          FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
          for (MessageInfo info : infosToDelete) {
            IndexValue value = index.findKey(info.getStoreKey(), fileSpan);
            if (value != null) {
              throw new StoreException("Cannot force delete id " + info.getStoreKey() + " since the id exists",
                  StoreErrorCodes.AlreadyExist);
            }
          }
        }

        List<InputStream> inputStreams = new ArrayList<>(infosToDelete.size());
        List<MessageInfo> updatedInfos = new ArrayList<>(infosToDelete.size());

        for (MessageInfo info : infosToDelete) {
          MessageFormatInputStream stream =
              new DeleteMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
                  info.getOperationTimeMs(), info.getLifeVersion());
          updatedInfos.add(
              new MessageInfo(info.getStoreKey(), stream.getSize(), info.getAccountId(), info.getContainerId(),
                  info.getOperationTimeMs(), info.getLifeVersion()));
          inputStreams.add(stream);
        }

        Offset endOffsetOfLastMessage = log.getEndOffset();
        MessageFormatWriteSet writeSet =
            new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), updatedInfos,
                false);
        writeSet.writeTo(log);
        logger.trace("Store : {} force delete mark written to log", dataDir);
        for (MessageInfo info : updatedInfos) {
          FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
          index.markAsDeleted(info.getStoreKey(), fileSpan, info, info.getOperationTimeMs(), info.getLifeVersion());
          endOffsetOfLastMessage = fileSpan.getEndOffset();
        }
        logger.trace("Store : {} force delete has been marked in the index ", dataDir);
      }
      onSuccess("DELETE");
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to force delete blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
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
              StoreErrorCodes.UpdateNotAllowed);
        }
        IndexValue value =
            index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
        if (value == null) {
          throw new StoreException("Cannot update TTL of " + info.getStoreKey() + " since it's not in the index",
              StoreErrorCodes.IDNotFound);
        } else if (!info.getStoreKey().isAccountContainerMatch(value.getAccountId(), value.getContainerId())) {
          if (config.storeValidateAuthorization) {
            throw new StoreException(
                "UPDATE authorization failure. Key: " + info.getStoreKey() + " AccountId in store: "
                    + value.getAccountId() + " ContainerId in store: " + value.getContainerId(),
                StoreErrorCodes.AuthorizationFailure);
          } else {
            logger.warn("UPDATE authorization failure. Key: {} AccountId in store: {} ContainerId in store: {}",
                info.getStoreKey(), value.getAccountId(), value.getContainerId());
            metrics.ttlUpdateAuthorizationFailureCount.inc();
          }
        } else if (value.isDelete()) {
          throw new StoreException(
              "Cannot update TTL of " + info.getStoreKey() + " since it is already deleted in the index.",
              StoreErrorCodes.IDDeleted);
        } else if (value.isTtlUpdate()) {
          throw new StoreException("TTL of " + info.getStoreKey() + " is already updated in the index.",
              StoreErrorCodes.AlreadyUpdated);
        } else if (!IndexValue.hasLifeVersion(info.getLifeVersion()) && value.getExpiresAtMs() != Utils.Infinite_Time
            && value.getExpiresAtMs() < info.getOperationTimeMs() + ttlUpdateBufferTimeMs) {
          // When the request is from frontend, make sure it's not too close to expiry date.
          // When the request is from replication, we don't care about the operation time.
          throw new StoreException(
              "TTL of " + info.getStoreKey() + " cannot be updated because it is too close to expiry. Op time (ms): "
                  + info.getOperationTimeMs() + ". ExpiresAtMs: " + value.getExpiresAtMs(),
              StoreErrorCodes.UpdateNotAllowed);
        }
        indexValuesToUpdate.add(value);
        lifeVersions.add(value.getLifeVersion());
      }
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
                    StoreErrorCodes.IDDeleted);
              } else if (value.isTtlUpdate()) {
                throw new StoreException("TTL of " + info.getStoreKey() + " is already updated in the index.",
                    StoreErrorCodes.AlreadyUpdated);
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
          blobStoreStats.handleNewTtlUpdateEntry(info.getStoreKey(), ttlUpdateValue,
              indexValuesToUpdate.get(correspondingPutIndex++));
        }
        logger.trace("Store : {} ttl update has been marked in the index ", dataDir);
      }
      onSuccess("TTL_UPDATE");
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to update ttl of blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
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
              StoreErrorCodes.AuthorizationFailure);
        } else {
          logger.warn("UNDELETE authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
              info.getStoreKey(), latestValue.getAccountId(), latestValue.getContainerId());
          metrics.undeleteAuthorizationFailureCount.inc();
        }
      }
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
                  StoreErrorCodes.LifeVersionConflict);
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
      onSuccess("UNDELETE");
      return revisedLifeVersion;
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.IOError) {
        onError();
      }
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to undelete blobs from store " + dataDir, e,
          StoreErrorCodes.UnknownError);
    } finally {
      context.stop();
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
      // We don't call onSuccess here because findEntries only involves reading index entries from the index
      // files, which have already been loaded to memory, thus there is no disk operations.
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
            StoreErrorCodes.IDNotFound);
      }
      return fromIndexValue(key, value);
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
  public Map<String, MessageInfo> findAllMessageInfosForKey(StoreKey key) throws StoreException {
    checkStarted();
    final Timer.Context context = metrics.findAllMessageInfosResponse.time();
    try {
      List<IndexValue> values = index.findAllIndexValuesForKey(key, null);
      if (values == null) {
        return Collections.emptyMap();
      }
      Map<String, MessageInfo> result = new HashMap<>();
      for (IndexValue value : values) {
        Offset offset = value.getOffset();
        String mapKey = offset.getName().toString() + "_" + offset.getOffset();
        result.put(mapKey, fromIndexValue(key, value));
      }
      return result;
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
            StoreErrorCodes.IDNotFound);
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
   * Test if the storage for this blob is still available or not. This test would be triggered after a blob store
   * on the same disk, encounters an io error. If this io error is indeed caused by disk error, we would want to
   * shut down all the blob stores on this disk as soon as possible, so we can move all the replicas on this disk
   * to a different place.
   *
   * In order to test underlying storage's availability, this method would try read one chunk of data from disk
   * to memory for several times. If any of those read works, this method would return true.
   * @return
   */
  boolean testStorageAvailability() {
    if (!isStarted()) {
      return false;
    }
    BlobReadOptions readOptions = index.getRandomPutEntryReadOptions();
    if (readOptions == null) {
      // There is no put entry in this blob store, then we don't determine if this storage is still available,
      // just assume it's available and return true.
      logger.info("Failed to get a random put entry from store {}", dataDir);
      return true;
    }
    // Read the same chunk for several times, The first time would load data from disk to page cache, and if it does,
    // we can return true right away. If there is any error, the following reads would try again until the blob store
    // is shut down.
    // It's possible that data is already in page cache. We should either invalidate the page cache before reading
    // or read the data using direct io.
    for (int i = 0; i < config.storeIoErrorCountToTriggerShutdown; i++) {
      if (!isStarted()) {
        break;
      }
      try {
        // TODO: avoid page cache when testing storage availability
        readOptions.doPrefetch(0, readOptions.getMessageInfo().getSize());
        readOptions.getPrefetchedData().release();
        onSuccess("EXAMINE");
        // close read option to release the reference to the log segment
        readOptions.close();
        return true;
      } catch (Exception e) {
        if (e.getMessage().contains("Input/output error")) {
          tryOnError();
        } else {
          logger.error("Failed to prefetch PUT data from read options: {}", readOptions, e);
        }
      }
    }
    // close read option to release the reference to the log segment
    readOptions.close();
    return false;
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
  public boolean hasPartialRecovery() {
    return index.hasPartialRecovery();
  }

  @Override
  public boolean isBootstrapInProgress() {
    return (new File(dataDir, config.storeBootstrapInProgressFile)).exists();
  }

  @Override
  public boolean isDecommissionInProgress() {
    // note that, the decommission file will be removed by calling deleteStoreFiles() when replica is being dropped. We
    // don't need to explicitly delete it. The file is also used for failure recovery to resume decommission process.
    return (new File(dataDir, DECOMMISSION_FILE_NAME)).exists();
  }

  @Override
  public void completeBootstrap() {
    File bootstrapFile = new File(dataDir, config.storeBootstrapInProgressFile);
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
    if (currentState != state) {
      logger.info("storeId = {}, State change from {} to {}", storeId, currentState, state);
      previousState = currentState;
      currentState = state;
    }
  }

  @Override
  public ReplicaState getCurrentState() {
    return currentState;
  }

  public ReplicaState getPreviousState() {
    return previousState;
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
   * Return log object, only for testing.
   * @return
   */
  Log getLog() {
    return log;
  }

  // Only for testing.
  void setLog(Log log) {
    this.log = log;
  }

  /**
   * Set if the store is recovering from decommission
   * @param value {@code true if store is recovering from decommission}. {@code false} otherwise.
   */
  public void setRecoverFromDecommission(boolean value) {
    recoverFromDecommission = value;
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
    if (started || initialized) {
      throw new IllegalStateException("Store is still started. Deleting store files is not allowed.");
    }
    // Step 1: return occupied swap segments (if any) to reserve pool
    if (compactor != null) {
      String[] swapSegmentsInUse = compactor.getSwapSegmentsInUse();
      for (String fileName : swapSegmentsInUse) {
        logger.info("Returning swap segment {} to reserve pool", fileName);
        File swapSegmentTempFile = new File(dataDir, fileName);
        diskSpaceAllocator.free(swapSegmentTempFile, config.storeSegmentSizeInBytes, storeId, true);
      }
    }

    // Step 2: if segmented, delete remaining store segments in reserve pool
    if (log != null && log.isLogSegmented()) {
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
   * Gets the log segment metadata files from in-memory data structures
   * This method returns List of LogSegmentFiles along with its IndexFiles, BloomFilterFiles
   */
  @Override
  public List<LogInfo> getLogSegmentMetadataFiles(boolean includeActiveLogSegment) {
    List<LogInfo> result = new ArrayList<>();
    final Timer.Context context = metrics.fileCopyGetMetadataResponse.time();

    List<FileInfo> logSegments = getLogSegments(includeActiveLogSegment);
    if (null != logSegments) {
      for (FileInfo storeFileInfo : logSegments) {
        LogSegmentName logSegmentName;

        if (storeFileInfo.getFileName().isEmpty()) {
          // This happens when the code is running locally and a single LS exists with the name "log_current"
          logSegmentName = LogSegmentName.fromFilename(LogSegmentName.SINGLE_SEGMENT_LOG_FILE_NAME);
        } else {
          logSegmentName = LogSegmentName.fromFilename(storeFileInfo.getFileName() + LogSegmentName.SUFFIX);
        }
        List<FileInfo> indexFiles = getIndexSegmentFilesForLogSegment(dataDir, logSegmentName);
        List<FileInfo> bloomFiles = getBloomFilterFilesForLogSegment(dataDir, logSegmentName);

        result.add(new StoreLogInfo(storeFileInfo, indexFiles, bloomFiles));
      }
    }
    context.stop();
    return result;
  }

  /**
   * Get all log segments in the store.
   * The list of files returned are sorted by their logSegmentNames
   * Param includeActiveLogSegment is used to determine if the active log segment should be included in the result.
   */
  private List<FileInfo> getLogSegments(boolean includeActiveLogSegment) {
    logger.info("FCH TEST: Returning Log Segments: {}", log.getAllLogSegmentNames().stream().map(x -> x.toString()));
    return log.getAllLogSegmentNames().stream()
        .filter(segmentName -> includeActiveLogSegment || !segmentName.equals(log.getActiveSegment().getName()))
        .map(segmentName -> log.getSegment(segmentName))
        .map(segment -> {
          FileInfo storeFileInfo = new StoreFileInfo(segment.getName().toString(), segment.getView().getFirst().length());
          segment.closeView();
          return storeFileInfo;
        })
        .collect(Collectors.toList());
  }

  /**
   * Get all index segments for a log segment.
   * The list of files returned are sorted by their start offsets using {@link #INDEX_SEGMENT_FILE_INFO_COMPARATOR}
   */
  private List<FileInfo> getIndexSegmentFilesForLogSegment(String dataDir, LogSegmentName logSegmentName) {
    List<FileInfo> indexFileInfos = Arrays.stream(PersistentIndex.getIndexSegmentFilesForLogSegment(dataDir, logSegmentName))
        .map(file -> new StoreFileInfo(file.getName(), file.length()))
        .collect(Collectors.toList());
    Collections.sort(indexFileInfos, PersistentIndex.INDEX_SEGMENT_FILE_INFO_COMPARATOR);

    return indexFileInfos;
  }

  /**
   * Get all bloom filter files for a log segment.
   */
  private List<FileInfo> getBloomFilterFilesForLogSegment(String dataDir, LogSegmentName logSegmentName) {
    return Arrays.stream(PersistentIndex.getBloomFilterFilesForLogSegment(dataDir, logSegmentName))
        .map(file -> new StoreFileInfo(file.getName(), file.length()))
        .collect(Collectors.toList());
  }

  /**
   * Update the sealed status of the replica.
   */
  void updateSealedStatus() {
    // In two zk clusters case, "replicaId.isSealed()" might be true in first cluster but is still unsealed in second
    // cluster, as server only spectates the first cluster. To guarantee both clusters have consistent seal/unseal
    // state, we bypass "isSealed()" check if there are more than one replicaStatusDelegates.
    ReplicaSealStatus oldReplicaSealStatus = replicaSealStatus.get();
    ReplicaSealStatus resolvedReplicaSealStatus = resolveReplicaSealStatusFromLogSize();
    if (resolvedReplicaSealStatus == ReplicaSealStatus.SEALED && ((replicaStatusDelegates.size() > 1
        && replicaSealStatus.getAndSet(ReplicaSealStatus.SEALED) != ReplicaSealStatus.SEALED)
        || !replicaId.isSealed())) {
      for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
        if (!replicaStatusDelegate.seal(replicaId)) {
          metrics.sealSetError.inc();
          logger.warn("Could not set the partition as read-only status on {}", replicaId);
          replicaSealStatus.set(oldReplicaSealStatus);
        } else {
          // From the sealDoneCount metric, we can identify the hosts which frequently seal and unseal partitions.
          metrics.sealDoneCount.inc();
          logger.info(
              "[CAPACITY] Store is successfully sealed for partition : {} because current used capacity : {} bytes exceeds ReadOnly threshold : (low : {}, high : {}) bytes",
              replicaId.getPartitionId(), index.getLogUsedCapacity(), sealThresholdBytesLow, sealThresholdBytesHigh);
        }
      }
    } else if (resolvedReplicaSealStatus == ReplicaSealStatus.PARTIALLY_SEALED && ((replicaStatusDelegates.size() > 1
        && replicaSealStatus.getAndSet(ReplicaSealStatus.PARTIALLY_SEALED) != ReplicaSealStatus.PARTIALLY_SEALED)
        || !replicaId.isPartiallySealed())) {
      for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
        if (!replicaStatusDelegate.partialSeal(replicaId)) {
          metrics.partialSealSetError.inc();
          logger.warn("Could not set the partition as partially-sealed status on {}", replicaId);
          replicaSealStatus.set(oldReplicaSealStatus);
        } else {
          metrics.partialSealDoneCount.inc();
          logger.info(
              "Store is successfully partial-sealed for partition : {} because current used capacity : {} bytes is exceeds Partial Read threshold : (low : {}, high : {}) bytes",
              replicaId.getPartitionId(), index.getLogUsedCapacity(), partialSealThresholdBytesLow,
              partialSealThresholdBytesHigh);
        }
      }
    } else if (resolvedReplicaSealStatus == ReplicaSealStatus.NOT_SEALED && ((replicaStatusDelegates.size() > 1
        && replicaSealStatus.getAndSet(ReplicaSealStatus.NOT_SEALED) != ReplicaSealStatus.NOT_SEALED)
        || !replicaId.isUnsealed())) {
      for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
        if (!replicaStatusDelegate.unseal(replicaId)) {
          metrics.unsealSetError.inc();
          logger.warn("Could not set the partition as not sealed on {}", replicaId);
          replicaSealStatus.set(oldReplicaSealStatus);
        } else {
          metrics.unsealDoneCount.inc();
          logger.info(
              "[CAPACITY] Store is successfully unsealed for partition : {} because current used capacity : {} bytes is below Read Write threshold : {} bytes",
              replicaId.getPartitionId(), index.getLogUsedCapacity(), partialSealThresholdBytesHigh);
        }
      }
    }
  }

  /**
   * Reconcile the sealed status of the replica during startup.
   */
  void reconcileSealedStatus() {
    // During startup, we also need to reconcile the replica state from both ZK clusters.
    // reconcile the state by reading sealing state from both clusters
    ReplicaSealStatus sealStatus = resolveReplicaSealStatusFromLogSize();
    String partitionName = replicaId.getPartitionId().toPathString();
    for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
      Set<String> sealedReplicas = new HashSet<>(replicaStatusDelegate.getSealedReplicas());
      Set<String> partiallySealedReplicas = new HashSet<>(replicaStatusDelegate.getPartiallySealedReplicas());
      ReplicaSealStatus delegateSealStatus = sealedReplicas.contains(partitionName) ? ReplicaSealStatus.SEALED
          : (partiallySealedReplicas.contains(partitionName) ? ReplicaSealStatus.PARTIALLY_SEALED
              : ReplicaSealStatus.NOT_SEALED);
      sealStatus = ReplicaSealStatus.mergeReplicaSealStatus(sealStatus, delegateSealStatus);
    }
    boolean success = false;
    for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
      switch (sealStatus) {
        case SEALED:
          success = replicaStatusDelegate.seal(replicaId);
          break;
        case PARTIALLY_SEALED:
          success = replicaStatusDelegate.partialSeal(replicaId);
          break;
        case NOT_SEALED:
          success = replicaStatusDelegate.unseal(replicaId);
          break;
      }
    }
    if (success) {
      logger.info("Succeeded in reconciling replica state to {} state", sealStatus.name());
      replicaSealStatus.set(sealStatus);
    } else {
      logger.error("Failed on reconciling replica state to {} state", sealStatus.name());
    }
  }

  /**
   * Resolve the {@link ReplicaSealStatus} of this store's replica based on the current {@link ReplicaSealStatus} and
   * current log used capacity.
   * @return ReplicaSealStatus object.
   */
  ReplicaSealStatus resolveReplicaSealStatusFromLogSize() {
    // If the size of log exceeds seal high threshold then the log should be marked as sealed.
    // If the log is already sealed, but the size exceeds low threshold, then we will wait for size to go below low
    // threshold before changing the replica state to partially_sealed.
    if (index.getLogUsedCapacity() > sealThresholdBytesHigh || (replicaId.isSealed()
        && index.getLogUsedCapacity() >= sealThresholdBytesLow)) {
      return ReplicaSealStatus.SEALED;
    }
    // If the size of log exceeds partial seal high threshold then the log should be marked as partially sealed.
    // If the log is already partially sealed, but the size exceeds low threshold, then we will wait for size to go
    // below low threshold before changing the replica state to partially_sealed.
    if (index.getLogUsedCapacity() > partialSealThresholdBytesHigh || (replicaId.isPartiallySealed()
        && index.getLogUsedCapacity() >= partialSealThresholdBytesLow)) {
      return ReplicaSealStatus.PARTIALLY_SEALED;
    }
    return ReplicaSealStatus.NOT_SEALED;
  }

  /**
   * Shuts down the store.
   * @param skipDiskFlush {@code true} should skip any disk flush operations during shutdown. {@code false} otherwise.
   * @throws StoreException
   */
  private void shutdown(boolean skipDiskFlush) throws StoreException {
    long startTimeInMs = time.milliseconds();
    synchronized (storeWriteLock) {
      checkInitialized();
      try {
        checkStarted();
        logger.info("Store : {} shutting down", dataDir);
        blobStoreStats.close();
        compactor.close(30);
        index.close(skipDiskFlush);
        log.close(skipDiskFlush);
        remoteTokenTracker.close();
        metrics.deregisterMetrics(storeId);
        setCurrentState(ReplicaState.OFFLINE);
        started = false;
      } catch (Exception e) {
        logger.error("Store : {} shutdown of store failed for directory ", dataDir, e);
      } finally {
        try {
          fileLock.destroy();
        } catch (IOException e) {
          logger.error("Store : {} IO Exception while trying to close the file lock", dataDir, e);
        }
        fileStore.shutdown();
        metrics.storeShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
      }
      initialized = false;
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
      if (config.storeSetLocalPartitionStateEnabled && !isDisabled.getAndSet(true) && replicaStatusDelegates != null) {
        try {
          replicaStatusDelegates.forEach(delegate -> delegate.disableReplica(replicaId));
        } catch (Exception e) {
          logger.error("Failed to disable replica {} due to exception ", replicaId, e);
        }
      }
      metrics.storeIoErrorTriggeredShutdownCount.inc();
      if (diskManager != null) {
        diskManager.onBlobStoreIOError();
      }
    }
  }

  /**
   * Wrapping {@link #onError()} method is a try-catch statement to avoid dealing with
   * exceptions.
   */
  private void tryOnError() {
    try {
      onError();
    } catch (Exception e) {
    }
  }

  /**
   * If store restarted successfully or at least one operation succeeded, reset the error count.
   */
  private void onSuccess(String method) {
    if (errorCount.getAndSet(0) != 0) {
      logger.info("Store {}: method {} set error count back to 0", storeId, method);
    }
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
   * @return {@code true} if this store has been initialized successfully.
   */
  public boolean isInitialized(){
    return initialized;
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
    blobStoreStats.onCompactionFinished();
    logger.info("One cycle of compaction is completed on the store {}", storeId);
  }

  /**
   * Closes the last log segment periodically if replica is in sealed status.
   * Hybrid compaction policy will support both statsBasedCompactionPolicy and compactAllPolicy.
   * Make sure this method is running before compactAllPolicy so it will compact the auto closed log segment afterwards.
   * @throws StoreException if any store exception occurred as part of ensuring capacity.
   */
  void closeLastLogSegmentIfQualified() throws StoreException {
    synchronized (storeWriteLock) {
      if (compactor.closeLastLogSegmentIfQualified()) {
        //refresh journal.
        long startTime = SystemTime.getInstance().milliseconds();
        index.journal.cleanUpJournal();
        logger.debug("Time to clean up journal size for store : {} in dataDir: {} is {} ms", storeId, dataDir,
            SystemTime.getInstance().milliseconds() - startTime);
      }
    }
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
      blobStoreStats.onCompactionFinished();
    }
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.StoreNotStarted);
    }
  }

  private void checkInitialized() throws StoreException {
    if (!initialized) {
      throw new StoreException("Store not initialized", StoreErrorCodes.StoreNotInitialized);
    }
  }

  CompactionDetails getCompactionDetailsInProgress() throws StoreException {
    if (CompactionLog.isCompactionInProgress(dataDir, storeId)) {
      return compactor.getCompactionDetailsInProgress();
    }
    return null;
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
   * Sanity check, make sure all blobs belong to this partition.
   * @param infos The list of {@link MessageInfo} to check partition
   */
  private void checkPartition(List<MessageInfo> infos) {
    for (MessageInfo info : infos) {
      StoreKey storeKey = info.getStoreKey();
      if (storeKey instanceof BlobId && replicaId != null) {
        BlobId blobId = (BlobId) storeKey;
        if (blobId.getPartition().getId() != replicaId.getPartitionId().getId()) {
          metrics.blobPartitionUnmatchError.inc();
          throw new IllegalArgumentException(
              "WriteSet contains unexpected blob id : " + info.getStoreKey() + ", it belongs to partition "
                  + blobId.getPartition() + " but this blobstore's partition is " + replicaId.getPartitionId());
        }
      }
    }
  }

  private MessageInfo fromIndexValue(StoreKey key, IndexValue value) {
    return new MessageInfo(key, value.getSize(), value.isDelete(), value.isTtlUpdate(), value.isUndelete(),
        value.getExpiresAtMs(), null, value.getAccountId(), value.getContainerId(), value.getOperationTimeInMs(),
        value.getLifeVersion());
  }

  /**
   * Resolve initial state of current store.
   */
  private void resolveStoreInitialState() {
    // Determine if HelixParticipant is adopted.
    if (replicaStatusDelegates != null && !replicaStatusDelegates.isEmpty() && replicaStatusDelegates.get(0)
        .supportsStateChanges()) {
      // If store is managed by Helix, Helix controller will update its state after participation.
      setCurrentState(ReplicaState.OFFLINE);
    } else {
      // This is to be compatible with static clustermap. If static cluster manager is adopted, all replicas are supposed to be STANDBY
      // and router relies on failure detector to track liveness of replicas(stores).
      setCurrentState(ReplicaState.STANDBY);
    }
  }

  /**
   * Build the compaction history.
   */
  private void buildCompactionHistory() {
    long now = time.milliseconds();
    long cutoffTime = now - Duration.ofDays(config.storeCompactionHistoryInDay).toMillis();
    AtomicInteger count = new AtomicInteger();
    AtomicLong startTime = new AtomicLong(0);
    AtomicLong endTime = new AtomicLong();
    try {
      CompactionLog.processCompactionLogs(dataDir, storeId, factory, time, config, compactionLog -> {
        if (compactionLog.getStartTime() <= cutoffTime) {
          // Ignore CompactionLogs that are created before the cutoffTime.
          return false;
        }
        // This change is probably not forward compatible. If we deploy this new version and start
        // persisting index segment offsets in CompactionLog, we expect all the following CompactionLogs would
        // keep persisting index segment offsets so there is no gap between CompactionLog history.
        if (compactionLog.isIndexSegmentOffsetsPersisted()) {
          logger.trace("Store {}: build compaction history by processing compaction log started at {}", storeId,
              compactionLog.getStartTime());
          index.updateBeforeAndAfterCompactionIndexSegmentOffsets(compactionLog.getStartTime(),
              compactionLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles(), false);
          count.incrementAndGet();
          startTime.compareAndSet(0, compactionLog.getStartTime());
          endTime.set(compactionLog.getStartTime());
        }
        return true;
      });
      logger.info("Store {}: Build compaction history with {} compaction logs from {} to {}", storeId, count.get(),
          startTime.get(), endTime.get());
      index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    } catch (Exception e) {
      // Don't fatal the process just because we can't build the compaction history.
      logger.error("Store {}: Failed to process CompactionLogs", storeId, e);
    }

    try {
      CompactionLog.cleanupCompactionLogs(dataDir, storeId, cutoffTime);
    } catch (Throwable e) {
      logger.error("Store {}: Failed to clean up compaction files", storeId, e);
    }
  }

  @Override
  public String toString() {
    return "StoreId: " + storeId + ". DataDir: " + dataDir + ". Capacity: " + capacityInBytes;
  }

  /*
   * Runs 2 validations for blobs to delete for both delete and batchDelete. Throws StoreException for validation failure.
   * This message is separate from common validation function due to store locking required for these validations.
   */
  private void validateDeleteForDuplicateAndLifeVersioConflicts(MessageInfo info, IndexValue value,
      short expectedLifeVersion) throws StoreException {
    // There are several possible cases that can exist here. Delete has to follow either PUT, TTL_UPDATE or UNDELETE.
    // let EOBC be end offset before check, and [RECORD] means RECORD is optional
    // 1. PUT [TTL_UPDATE DELETE UNDELETE] EOBC DELETE
    // 2. PUT EOBC TTL_UPDATE
    // 3. PUT EOBC DELETE UNDELETE: this is really extreme case
    // From these cases, we can have value being DELETE, TTL_UPDATE AND UNDELETE, we have to deal with them accordingly.
    if (value.getLifeVersion() == expectedLifeVersion) {
      if (value.isDelete()) {
        throw new StoreException(
            "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
            StoreErrorCodes.IDDeleted);
      }
      // value being ttl update is fine, we can just append DELETE to it.
    } else {
      // For the extreme case, we log it out and throw an exception.
      logger.warn("Concurrent operation for id " + info.getStoreKey() + " in store " + dataDir + ". Newly added value "
          + value);
      throw new StoreException(
          "Cannot delete id " + info.getStoreKey() + " since there are concurrent operation while delete",
          StoreErrorCodes.LifeVersionConflict);
    }
  }

  /*
   * Runs validations for blobs to delete for both delete and batchDelete. Throws StoreException for validation failure.
   */
  private void validateMessageInfoForDelete(MessageInfo info, Offset indexEndOffsetBeforeCheck,
      List<IndexValue> indexValuesPriorToDelete, List<Short> lifeVersions, List<IndexValue> originalPuts)
      throws StoreException {
    IndexValue value =
        index.findKey(info.getStoreKey(), new FileSpan(index.getStartOffset(), indexEndOffsetBeforeCheck));
    if (value == null) {
      throw new StoreException(
          "BATCH_DELETE: Cannot delete id " + info.getStoreKey() + " because it is not present in the index",
          StoreErrorCodes.IDNotFound);
    }
    if (!info.getStoreKey().isAccountContainerMatch(value.getAccountId(), value.getContainerId())) {
      String errorStr = "BATCH_DELETE authorization failure. Key: " + info.getStoreKey() + "Actually accountId: "
          + value.getAccountId() + "Actually containerId: " + value.getContainerId();
      if (config.storeValidateAuthorization) {
        throw new StoreException(errorStr, StoreErrorCodes.AuthorizationFailure);
      } else {
        logger.warn(errorStr);
        metrics.deleteAuthorizationFailureCount.inc();
      }
    }
    short revisedLifeVersion = info.getLifeVersion();
    if (info.getLifeVersion() == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
      // This is a delete request from frontend
      if (value.isDelete()) {
        throw new StoreException(
            "BATCH_DELETE: Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
            StoreErrorCodes.IDDeleted);
      }
      revisedLifeVersion = value.getLifeVersion();
    } else {
      // This is a delete request from replication
      if (value.isDelete() && value.getLifeVersion() == info.getLifeVersion()) {
        throw new StoreException("BATCH_DELETE: Cannot delete id " + info.getStoreKey()
            + " since it is already deleted in the index with lifeVersion " + value.getLifeVersion() + ".",
            StoreErrorCodes.IDDeleted);
      }
      if (value.getLifeVersion() > info.getLifeVersion()) {
        throw new StoreException("BATCH_DELETE: Cannot delete id " + info.getStoreKey()
            + " since it has a higher lifeVersion than the message info: " + value.getLifeVersion() + ">"
            + info.getLifeVersion(), StoreErrorCodes.LifeVersionConflict);
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

  /*
   * Runs validations and filtration for blobs to delete for batchDelete.
   */
  private List<MessageInfo> validateAndFilterMessageInfosForBatchDelete(List<MessageInfo> infosToDelete,
      Offset indexEndOffsetBeforeCheck, List<IndexValue> indexValuesPriorToDelete, List<Short> lifeVersions,
      List<IndexValue> originalPuts, StoreBatchDeleteInfo storeBatchDeleteInfo) throws StoreException {
    List<MessageInfo> filteredInfos = new ArrayList<>();
    for (MessageInfo info : infosToDelete) {
      try {
        validateMessageInfoForDelete(info, indexEndOffsetBeforeCheck, indexValuesPriorToDelete, lifeVersions,
            originalPuts);
        filteredInfos.add(info);
      } catch (StoreException e) {
        storeBatchDeleteInfo.addMessageErrorInfo(new MessageErrorInfo(info, e.getErrorCode()));
        if (e.getErrorCode() == StoreErrorCodes.IOError) {
          onError();
        }
      }
    }
    return filteredInfos;
  }

  /*
   * Appends delete messages (tombstomb) to active log segment and returns the end offset.
   */
  private Offset writeDeleteMessagesToLogSegment(List<MessageInfo> infosToDelete, List<InputStream> inputStreams,
      List<MessageInfo> updatedInfos, List<Short> lifeVersions)
      throws IOException, StoreException, MessageFormatException {
    int i = 0;
    for (MessageInfo info : infosToDelete) {
      MessageFormatInputStream stream =
          new DeleteMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
              info.getOperationTimeMs(), lifeVersions.get(i));
      // Don't change the lifeVersion here, there are other logic in markAsDeleted that relies on this lifeVersion.
      updatedInfos.add(new MessageInfo(info.getStoreKey(), stream.getSize(), info.getAccountId(), info.getContainerId(),
          info.getOperationTimeMs(), info.getLifeVersion()));
      inputStreams.add(stream);
      i++;
    }
    Offset endOffsetOfLastMessage = log.getEndOffset();
    MessageFormatWriteSet writeSet =
        new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), updatedInfos, false);
    writeSet.writeTo(log);
    return endOffsetOfLastMessage;
  }

  /*
   * Updaing the persistent index and store stats post delete of blobs in updatedInfos. Common usage across delete and
   * batchDelete.
   */
  private void updateIndexAndStats(List<MessageInfo> updatedInfos, Offset endOffsetOfLastMessage,
      List<IndexValue> originalPuts, List<IndexValue> indexValuesPriorToDelete) throws StoreException {
    int correspondingPutIndex = 0;
    for (MessageInfo info : updatedInfos) {
      FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
      IndexValue deleteIndexValue =
          index.markAsDeleted(info.getStoreKey(), fileSpan, null, info.getOperationTimeMs(), info.getLifeVersion());
      endOffsetOfLastMessage = fileSpan.getEndOffset();
      blobStoreStats.handleNewDeleteEntry(info.getStoreKey(), deleteIndexValue, originalPuts.get(correspondingPutIndex),
          indexValuesPriorToDelete.get(correspondingPutIndex));
      correspondingPutIndex++;
    }
    logger.trace("Store : {} delete has been marked in the index ", dataDir);
  }

  /*
   * This method is used to validate and batch delete messages in the store. It is used for batch_delete.
   */
  private void validateAndBatchDelete(List<MessageInfo> infosToDelete, Offset indexEndOffsetBeforeCheck,
      List<IndexValue> indexValuesPriorToDelete, List<Short> lifeVersions, List<IndexValue> originalPuts,
      StoreBatchDeleteInfo storeBatchDeleteInfo, List<MessageInfo> infosToDeleteCopy)
      throws StoreException, IOException, MessageFormatException {
    Offset currentIndexEndOffset = index.getCurrentEndOffset();
    int i = 0;
    if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
      FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
      List<MessageInfo> filteredInfos = new ArrayList<>();
      for (MessageInfo info : infosToDelete) {
        try {
          IndexValue value =
              index.findKey(info.getStoreKey(), fileSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
          if (value != null) {
            validateDeleteForDuplicateAndLifeVersioConflicts(info, value, lifeVersions.get(i));
            indexValuesPriorToDelete.set(i, value);
          }
          i++;
          filteredInfos.add(info);
        } catch (StoreException e) {
          storeBatchDeleteInfo.addMessageErrorInfo(new MessageErrorInfo(info, e.getErrorCode()));
          if (e.getErrorCode() == StoreErrorCodes.IOError) {
            onError();
          }
        }
      }
      infosToDelete = filteredInfos;
      if (infosToDelete.isEmpty()) {
        logger.trace("BATCH_DELETE: Operation completely failed since all blobs failed. Received InfosToDelete: {}",
            infosToDeleteCopy);
        return;
      }
    }
    List<InputStream> inputStreams = new ArrayList<>(infosToDelete.size());
    List<MessageInfo> updatedInfos = new ArrayList<>(infosToDelete.size());
    Offset endOffsetOfLastMessage =
        writeDeleteMessagesToLogSegment(infosToDelete, inputStreams, updatedInfos, lifeVersions);
    logger.trace("BATCH_DELETE: Store : {} delete mark written to log", dataDir);
    updateIndexAndStats(updatedInfos, endOffsetOfLastMessage, originalPuts, indexValuesPriorToDelete);
    for (MessageInfo info : updatedInfos) {
      storeBatchDeleteInfo.addMessageErrorInfo(new MessageErrorInfo(info, null));
    }
  }
}
