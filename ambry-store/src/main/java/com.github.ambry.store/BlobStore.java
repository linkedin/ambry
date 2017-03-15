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
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The blob store that controls the log and index
 */
class BlobStore implements Store {
  static final String SEPARATOR = "_";
  private final static String LockFile = ".lock";

  private final String dataDir;
  private final ScheduledExecutorService taskScheduler;
  private final DiskIOScheduler diskIOScheduler;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  /* A lock that prevents concurrent writes to the log */
  private final Object lock = new Object();
  private final StoreConfig config;
  private final long capacityInBytes;
  private final StoreKeyFactory factory;
  private final MessageStoreRecovery recovery;
  private final MessageStoreHardDelete hardDelete;
  private final StoreMetrics metrics;
  private final Time time;
  private final UUID sessionId = UUID.randomUUID();

  private Log log;
  private PersistentIndex index;
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

  BlobStore(String storeId, StoreConfig config, ScheduledExecutorService taskScheduler, DiskIOScheduler diskIOScheduler,
      StorageManagerMetrics storageManagerMetrics, String dataDir, long capacityInBytes, StoreKeyFactory factory,
      MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, Time time) {
    this.metrics = storageManagerMetrics.createStoreMetrics(storeId);
    this.dataDir = dataDir;
    this.taskScheduler = taskScheduler;
    this.diskIOScheduler = diskIOScheduler;
    this.config = config;
    this.capacityInBytes = capacityInBytes;
    this.factory = factory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.time = time;
  }

  @Override
  public void start() throws StoreException {
    synchronized (lock) {
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
        log = new Log(dataDir, capacityInBytes, config.storeSegmentSizeInBytes, metrics);
        index = new PersistentIndex(dataDir, taskScheduler, log, config, factory, recovery, hardDelete, metrics, time,
            sessionId, storeDescriptor.getIncarnationId());
        metrics.initializeIndexGauges(index, capacityInBytes);
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
        synchronized (lock) {
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
              IndexValue value =
                  new IndexValue(info.getSize(), fileSpan.getStartOffset(), info.getExpirationTimeInMs());
              IndexEntry entry = new IndexEntry(info.getStoreKey(), value, info.getCrc());
              indexEntries.add(entry);
              endOffsetOfLastMessage = fileSpan.getEndOffset();
            }
            FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(), endOffsetOfLastMessage);
            index.addToIndex(indexEntries, fileSpan);
            logger.trace("Store : {} message set written to index ", dataDir);
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
      List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
      Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
      for (MessageInfo info : infoList) {
        IndexValue value = index.findKey(info.getStoreKey());
        if (value == null) {
          throw new StoreException("Cannot delete id " + info.getStoreKey() + " since it is not present in the index.",
              StoreErrorCodes.ID_Not_Found);
        } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
          throw new StoreException(
              "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
              StoreErrorCodes.ID_Deleted);
        }
      }
      synchronized (lock) {
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
        for (MessageInfo info : infoList) {
          FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
          index.markAsDeleted(info.getStoreKey(), fileSpan);
          endOffsetOfLastMessage = fileSpan.getEndOffset();
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
   * Return total capacity of the {@link BlobStore} in bytes
   * @return the total capacity of the {@link BlobStore} in bytes
   */
  long getCapacityInBytes() {
    return capacityInBytes;
  }

  /**
   * Fetches a list of {@link LogSegment} names whose entries don't over lap with {@link Journal}. Returns {@code null}
   * if there aren't any
   * @return list of {@link LogSegment} names whose entries don't over lap with {@link Journal}. {@code null}
   * if there aren't any
   */
  List<String> getLogSegmentsNotInJournal() throws StoreException {
    checkStarted();
    return index.getLogSegmentsNotInJournal();
  }

  @Override
  public void shutdown() throws StoreException {
    synchronized (lock) {
      checkStarted();
      try {
        logger.info("Store : " + dataDir + " shutting down");
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
      }
    }
  }

  /**
   * @return {@code true} if this store has been started successfully.
   */
  boolean isStarted() {
    return started;
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }
}
