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
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A persistent index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 **/
public class PersistentIndex implements LogSegmentSizeProvider {

  /**
   * Represents the different types of index entries.
   */
  public enum IndexEntryType {
    TTL_UPDATE, PUT, DELETE, UNDELETE
  }

  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  static final short VERSION_3 = 3;
  static final short VERSION_4 = 4;
  static short CURRENT_VERSION = VERSION_4;
  static final String CLEAN_SHUTDOWN_FILENAME = "cleanshutdown";
  private static final AtomicInteger recoveryFromPartialLogSegmentCounter = new AtomicInteger(0);

  static final FilenameFilter INDEX_SEGMENT_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX);
    }
  };
  static final Comparator<File> INDEX_SEGMENT_FILE_COMPARATOR = new Comparator<File>() {
    @Override
    public int compare(File o1, File o2) {
      if (o1 == null || o2 == null) {
        throw new NullPointerException("arguments to compare two files is null");
      }
      Offset o1Offset = IndexSegment.getIndexSegmentStartOffset(o1.getName());
      return o1Offset.compareTo(IndexSegment.getIndexSegmentStartOffset(o2.getName()));
    }
  };
  static final Comparator<FileInfo> INDEX_SEGMENT_FILE_INFO_COMPARATOR = new Comparator<FileInfo>() {
    @Override
    public int compare(FileInfo o1, FileInfo o2) {
      if (o1 == null || o2 == null) {
        throw new NullPointerException("arguments to compare two files is null");
      }
      Offset o1Offset = IndexSegment.getIndexSegmentStartOffset(o1.getFileName());
      return o1Offset.compareTo(IndexSegment.getIndexSegmentStartOffset(o2.getFileName()));
    }
  };
  static final Comparator<IndexEntry> INDEX_ENTRIES_OFFSET_COMPARATOR = new Comparator<IndexEntry>() {

    @Override
    public int compare(IndexEntry e1, IndexEntry e2) {
      return e1.getValue().getOffset().compareTo(e2.getValue().getOffset());
    }
  };

  final Journal journal;
  final HardDeleter hardDeleter;
  final Thread hardDeleteThread;
  final Log log;

  private final long maxInMemoryIndexSizeInBytes;
  private final int maxInMemoryNumElements;
  private final String dataDir;
  private final MessageStoreHardDelete hardDelete;
  private final StoreKeyFactory factory;
  private final DiskIOScheduler diskIOScheduler;
  private final StoreConfig config;
  private final boolean cleanShutdown;
  private final Offset logEndOffsetOnStartup;
  private final StoreMetrics metrics;
  private final UUID sessionId;
  private final UUID incarnationId;
  private final Time time;
  private final File cleanShutdownFile;

  // switching the ref to this is thread safe as long as there are no modifications to IndexSegment instances whose
  // offsets are still present in the journal.
  private volatile ConcurrentSkipListMap<Offset, IndexSegment> validIndexSegments = new ConcurrentSkipListMap<>();
  // this is used by addToIndex() and changeIndexSegments() to resolve concurrency b/w them and is not for general use.
  private volatile ConcurrentSkipListMap<Offset, IndexSegment> inFluxIndexSegments = validIndexSegments;
  private static final Logger logger = LoggerFactory.getLogger(PersistentIndex.class);
  private final IndexPersistor persistor = new IndexPersistor();
  private final ScheduledFuture<?> persistorTask;
  // When undelete is enabled, DELETE is not the final state of the blob, since a DELETEd blob can be UNDELTEd.
  private final boolean isDeleteFinalStateOfBlob;

  private final AtomicInteger partialLogSegmentCount = new AtomicInteger(0);
  private volatile boolean hasPartialRecovery = false;
  private final AtomicLong wastedLogSegmentSpace = new AtomicLong(0);

  // ReadWriteLock to make sure index values are always pointing to valid log segments.
  // In compaction's final steps, persistent index and log will update their internal maps to reflect the result of
  // compaction. This might have race condition with getBlobReadInfo method.
  // --------------------------------------------------------------------------------------
  // compaction thread                          | getBlobReadInfo thread
  // --------------------------------------------------------------------------------------
  // 1. RenameTempLogSegmentFilenames           | 1. FindKeyToReturnIndexValue
  // 2. AddNewLogSegmentToLogMap                |
  // 3. UpdateIndexMap                          |
  // 4. WaitForLogSegmentRefCountToZero         |
  // 5. RemoveIndexSegmentFiles                 |
  // 6. RemoveOldLogSegmentFromLogMap           |
  // 7. RemoveLogSegmentFiles                   | 2. GetLogSegmentFromIndexValueFromLogMap
  //                                            | 3. IncreaseLogSegmentRefCount
  // -------------------------------------------------------------------------------------
  // If two threads are executing in the above given order, then the GetLogSegmentFromIndexValueFromLogMap would
  // throw a NullPointerException since RemoveOldLogSegmentFromLogMap already removed it from the map.
  //
  // This ReadWriteLock would prevent this from happening.
  // 1. In getBlobReadInfo method, we use read lock to lock all three steps
  // 2. In compaction thread, we use write lock to lock step 3 (in method changeIndexSegments)
  //
  // If getBlobReadInfo happens before compaction's step 3, then compaction thread would be blocked at acquiring
  // the write lock in step 3, so getBlobReadInfo's step 2 would not happen after compaction's step 6.
  // If getBlobReadInfo happens after compaction's step 3, then the IndexValue returned by findKey method would reflect
  // the result of the compaction already, it will not point to any old log segment.
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  private volatile boolean shouldRebuildTokenBasedOnCompactionHistory = false;
  private volatile boolean sanityCheckFailed = false;
  // A map from index segment start offset before compaction to index segment start offset after compaction.
  // This map will only be modified in the compaction thread and accessed in the replication threads.
  private final NavigableMap<Offset, Offset> beforeAndAfterCompactionIndexSegmentOffsets =
      new ConcurrentSkipListMap<>();
  // A map from the compaction start timestamp to the set of offsets in the before and after compaction index segment
  // offset map. This map will only be accessed in the compaction thread after startup.
  private final TreeMap<Long, Set<Offset>> compactionTimestampToIndexSegmentOffsets = new TreeMap<>();
  private Runnable getBlobReadInfoTestCallback = null;

  /**
   * Creates a new persistent index
   * @param datadir The directory to use to store the index files
   * @param storeId The ID for the store that this index represents.
   * @param scheduler The scheduler that runs regular background tasks
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @param hardDelete  The hard delete handle used to perform hard deletes
   * @param diskIOScheduler the {@link DiskIOScheduler} to use.
   * @param metrics the metrics object
   * @param time the time instance to use
   * @param sessionId the ID of the current session.
   * @param incarnationId to uniquely identify the store's incarnation
   * @throws StoreException
   */
  PersistentIndex(String datadir, String storeId, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      DiskIOScheduler diskIOScheduler, StoreMetrics metrics, Time time, UUID sessionId, UUID incarnationId)
      throws StoreException {
    /* it is no longer required that the journal have twice the size of the index segment size since the index
    segment will not squash entries anymore but useful to keep this here in order to allow for a new index segment
    to be created that doesn't squash entries.
     */
    this(datadir, storeId, scheduler, log, config, factory, recovery, hardDelete, diskIOScheduler, metrics,
        new Journal(datadir, 2 * config.storeIndexMaxNumberOfInmemElements,
            config.storeMaxNumberOfEntriesToReturnFromJournal), time, sessionId, incarnationId,
        CLEAN_SHUTDOWN_FILENAME);
  }

  /**
   * Creates a new persistent index
   * @param datadir The directory to use to store the index files
   * @param storeId The ID for the store that this index represents.
   * @param scheduler The scheduler that runs persistence tasks. {@code null} if auto-persistence is not required.
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @param hardDelete  The hard delete handle used to perform hard deletes. {@code null} if hard delete functionality
   *                    is not required.
   * @param diskIOScheduler the {@link DiskIOScheduler} to use.
   * @param metrics the metrics object
   * @param journal the journal to use
   * @param time the time instance to use
   * @param sessionId the ID of the current session.
   * @param incarnationId to uniquely identify the store's incarnation
   * @param cleanShutdownFileName the name of the file that will be stored on clean shutdown.
   * @throws StoreException
   */
  PersistentIndex(String datadir, String storeId, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      DiskIOScheduler diskIOScheduler, StoreMetrics metrics, Journal journal, Time time, UUID sessionId,
      UUID incarnationId, String cleanShutdownFileName) throws StoreException {
    this.dataDir = datadir;
    this.log = log;
    this.time = time;
    this.metrics = metrics;
    this.factory = factory;
    this.config = config;
    this.hardDelete = hardDelete;
    this.diskIOScheduler = diskIOScheduler;
    this.journal = journal;
    this.sessionId = sessionId;
    this.incarnationId = incarnationId;
    this.maxInMemoryIndexSizeInBytes = config.storeIndexMaxMemorySizeBytes;
    this.maxInMemoryNumElements = config.storeIndexMaxNumberOfInmemElements;
    if (config.storeCompactionFilter.equals(
        BlobStoreCompactor.IndexSegmentValidEntryFilterWithoutUndelete.class.getSimpleName())) {
      logger.info("Undelete is not enabled, DELETE is considered as the final state of a blob");
      isDeleteFinalStateOfBlob = true;
    } else {
      isDeleteFinalStateOfBlob = false;
    }

    List<File> indexFiles = getAllIndexSegmentFiles();
    try {
      journal.startBootstrap();
      for (int i = 0; i < indexFiles.size(); i++) {
        // We mark as sealed all the index segments except the most recent index segment.
        // The recent index segment would go through recovery after they have been
        // read into memory
        boolean sealed = determineSealStatusForIndexFiles(i, indexFiles);
        IndexSegment info = new IndexSegment(indexFiles.get(i), sealed, factory, config, metrics, journal, time);
        logger.info("Index : {} loaded index segment {} with start offset {} and end offset {} ", datadir,
            indexFiles.get(i), info.getStartOffset(), info.getEndOffset());
        validIndexSegments.put(info.getStartOffset(), info);
      }
      // delete the shutdown file
      cleanShutdownFile = new File(datadir, cleanShutdownFileName);
      cleanShutdown = cleanShutdownFile.exists();
      if (cleanShutdown) {
        cleanShutdownFile.delete();
      }

      if (recovery != null) {
        recover(recovery);
      }
      //Journal should only include the index entry which belongs to the last log segment to make sure it only prevents the last log segment
      //from being compacted.
      removeIndexEntryNotBelongsToLastLogSegmentFromJournal();
      setEndOffsets();
      //if recover successfully, the active log segment should be the last log segment, otherwise, it should be log segment matching with last index segment.
      if (!shouldSetActiveLogSegmentToAutoClosedSegment()) {
        log.setActiveSegment(getCurrentEndOffset().getName());
      }
      logEndOffsetOnStartup = log.getEndOffset();
      journal.finishBootstrap();

      if (hardDelete != null) {
        // After recovering the last messages, and setting the log end offset, let the hard delete thread do its recovery.
        // NOTE: It is safe to do the hard delete recovery after the regular recovery because we ensure that hard deletes
        // never work on the part of the log that is not yet flushed (by ensuring that the message retention
        // period is longer than the log flush time).
        logger.info("Index : {} Starting hard delete recovery", datadir);
        hardDeleter = new HardDeleter(config, metrics, datadir, log, this, hardDelete, factory, diskIOScheduler, time);
        hardDeleter.performRecovery();
        logger.info("Index : {} Finished performing hard delete recovery", datadir);
        metrics.initializeHardDeleteMetric(storeId, hardDeleter, this);
      } else {
        hardDeleter = null;
      }

      if (scheduler != null) {
        // start scheduler thread to persist index in the background
        persistorTask = scheduler.scheduleAtFixedRate(persistor,
            config.storeDataFlushDelaySeconds + new Random().nextInt(Time.SecsPerMin),
            config.storeDataFlushIntervalSeconds, TimeUnit.SECONDS);
      } else {
        persistorTask = null;
      }

      if (hardDelete != null && config.storeEnableHardDelete) {
        logger.info("Index : {} Starting hard delete thread ", datadir);
        hardDeleteThread = Utils.newThread(HardDeleter.getThreadName(datadir), hardDeleter, true);
        hardDeleteThread.start();
      } else if (hardDelete != null) {
        hardDeleter.close();
        hardDeleteThread = null;
      } else {
        hardDeleteThread = null;
      }
    } catch (StoreException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while creating index " + datadir, e,
          StoreErrorCodes.IndexCreationFailure);
    }
  }

  /**
   * @param index the index of the list of indexFiles.
   * @param indexFiles a list of file with index entry info.
   * @return {@code true} if the index file needs to be set to seal status.
   */
  private boolean determineSealStatusForIndexFiles(int index, List<File> indexFiles) {
    return index < indexFiles.size() - 1;
  }

  /**
   * If index recovery succeed and last log segment is empty due to auto close segment feature, the last log segment
   * should be set as active log segment.
   * @return {@code true} if the recovery succeed and last log segment is empty.
   */
  private boolean shouldSetActiveLogSegmentToAutoClosedSegment() {
    return log.getLastSegment().isEmpty() && getCurrentEndOffset().compareTo(log.getEndOffset()) < 0;
  }

  /**
   * Loads all index segment files that refer to segments in the log. The list of files returned are sorted by the
   * their start offsets using {@link #INDEX_SEGMENT_FILE_COMPARATOR}.
   * @return all index segment files that refer to segments in the log.
   * @throws StoreException if index segment files could not be listed from the directory.
   */
  private List<File> getAllIndexSegmentFiles() throws StoreException {
    List<File> indexFiles = new ArrayList<>();
    LogSegment logSegment = log.getFirstSegment();
    while (logSegment != null) {
      File[] files = getIndexSegmentFilesForLogSegment(dataDir, logSegment.getName());
      if (files == null) {
        throw new StoreException("Could not read index files from directory [" + dataDir + "]",
            StoreErrorCodes.IndexCreationFailure);
      }
      indexFiles.addAll(Arrays.asList(files));
      logSegment = log.getNextSegment(logSegment);
    }
    Collections.sort(indexFiles, INDEX_SEGMENT_FILE_COMPARATOR);
    return indexFiles;
  }

  /**
   * Remove entry not belongs to last log segment in journal
   */
  private void removeIndexEntryNotBelongsToLastLogSegmentFromJournal() {
    LogSegmentName lastLogSegmentName = log.getLastSegment().getName();
    Offset journalFirstOffset = journal.getFirstOffset();
    while (journalFirstOffset != null && !journalFirstOffset.getName().equals(lastLogSegmentName)) {
      journal.removeSpecificValueInJournal(journalFirstOffset);
      journalFirstOffset = journal.getFirstOffset();
    }
  }

  /**
   * Recovers a segment given the end offset in the log and a recovery handler
   * <p/>
   * Assumed to run only on startup.
   * @param recovery The recovery handler that is used to perform the recovery
   * @throws StoreException
   * @throws IOException
   */
  private void recover(MessageStoreRecovery recovery) throws StoreException, IOException {
    final Timer.Context context = metrics.recoveryTime.time();
    LogSegment firstSegment = log.getFirstSegment();
    Offset recoveryStartOffset = new Offset(firstSegment.getName(), firstSegment.getStartOffset());
    if (validIndexSegments.size() > 0) {
      IndexSegment indexSegment = validIndexSegments.lastEntry().getValue();
      recoveryStartOffset =
          indexSegment.getEndOffset() == null ? indexSegment.getStartOffset() : indexSegment.getEndOffset();
    }
    boolean recoveryOccurred = false;
    LogSegment logSegmentToRecover = log.getSegment(recoveryStartOffset.getName());
    Set<StoreKey> deleteExpectedKeys = new HashSet<>();
    while (logSegmentToRecover != null) {
      long endOffset = logSegmentToRecover.sizeInBytes();
      logger.info("Index : {} performing recovery on index with start offset {} and end offset {}", dataDir,
          recoveryStartOffset, endOffset);
      MessageStoreRecovery.RecoveryResult recoveryResult =
          recovery.recover(logSegmentToRecover, recoveryStartOffset.getOffset(), endOffset, factory);
      maybePerformaPartialRecovery(logSegmentToRecover, recoveryResult);
      List<MessageInfo> messagesRecovered = recoveryResult.recovered;
      recoveryOccurred = recoveryOccurred || messagesRecovered.size() > 0;
      Offset runningOffset = recoveryStartOffset;
      // Iterate through the recovered messages and update the index
      for (MessageInfo info : messagesRecovered) {
        logger.trace("Index : {} recovering key {} offset {} size {}", dataDir, info.getStoreKey(), runningOffset,
            info.getSize());
        Offset infoEndOffset = new Offset(runningOffset.getName(), runningOffset.getOffset() + info.getSize());
        IndexValue value = findKey(info.getStoreKey());
        if (info.isDeleted()) {
          markAsDeleted(info.getStoreKey(), new FileSpan(runningOffset, infoEndOffset), info, info.getOperationTimeMs(),
              info.getLifeVersion());
          logger.info(
              "Index : {} updated message with key {} by inserting delete entry of size {} ttl {} lifeVersion{}",
              dataDir, info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs(), info.getLifeVersion());
          // removes from the tracking structure if a delete was being expected for the key
          deleteExpectedKeys.remove(info.getStoreKey());
        } else if (info.isUndeleted()) {
          markAsUndeleted(info.getStoreKey(), new FileSpan(runningOffset, infoEndOffset), info,
              info.getOperationTimeMs(), info.getLifeVersion());
          logger.info(
              "Index : {} updated message with key {} by inserting undelete entry of size {} ttl {} lifeVersion {}",
              dataDir, info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs(), info.getLifeVersion());
          if (value == null) {
            // This undelete was forced even though there was no equivalent PUT record - this means that we MUST see
            // a DELETE for this key (because the PUT record is gone, compaction must have cleaned it up because a
            // DELETE must have been present). This also works when there are multiple DELETE and UNDELETE for the same
            // key. For instance, if we have UNDELETE, DELETE and then UNDELETE. The first UNDELETE would expect a DELETE
            // to follow given that the PUT is gone. Then recovery would encounter the first DELETE and remove the key
            // from the expected list. But the second UNDELETE would once again expect to see a DELETE follow. If there
            // is not DELETE after second UNDELETE, an exception would be thrown.
            deleteExpectedKeys.add(info.getStoreKey());
          }
        } else if (info.isTtlUpdated()) {
          markAsPermanent(info.getStoreKey(), new FileSpan(runningOffset, infoEndOffset), info,
              info.getOperationTimeMs(), info.getLifeVersion());
          logger.info(
              "Index : {} updated message with key {} by inserting TTL update entry of size {} ttl {} lifeVersion {}",
              dataDir, info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs(), info.getLifeVersion());
          if (value == null) {
            // this TTL update was forced even though there was no equivalent PUT record - this means that we MUST see
            // a DELETE for this key (because the PUT record is gone, compaction must have cleaned it up because a
            // DELETE must have been present)
            deleteExpectedKeys.add(info.getStoreKey());
          }
        } else if (value != null) {
          throw new StoreException(
              "Illegal message state during recovery. Duplicate PUT record for " + info.getStoreKey(),
              StoreErrorCodes.IndexRecoveryError);
        } else {
          // create a new entry in the index
          IndexValue newValue = new IndexValue(info.getSize(), runningOffset, IndexValue.FLAGS_DEFAULT_VALUE,
              info.getExpirationTimeInMs(), info.getOperationTimeMs(), info.getAccountId(), info.getContainerId(),
              info.getLifeVersion());
          addToIndex(new IndexEntry(info.getStoreKey(), newValue, null), new FileSpan(runningOffset, infoEndOffset));
          logger.info("Index : {} adding new message to index with key {} size {} ttl {} deleted {}", dataDir,
              info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs(), info.isDeleted());
        }
        runningOffset = infoEndOffset;
      }
      logSegmentToRecover = log.getNextSegment(logSegmentToRecover);
      if (logSegmentToRecover != null) {
        recoveryStartOffset = new Offset(logSegmentToRecover.getName(), logSegmentToRecover.getStartOffset());
      }
    }
    if (deleteExpectedKeys.size() > 0) {
      throw new StoreException("Deletes were expected for some keys but were not encountered: " + deleteExpectedKeys,
          StoreErrorCodes.IndexRecoveryError);
    }
    if (recoveryOccurred) {
      metrics.nonzeroMessageRecovery.inc();
    }
    context.stop();
  }

  /**
   * Sets the end offset of all log segments.
   * <p/>
   * Assumed to run only on startup.
   * @throws StoreException if an end offset could not be set due to store exception.
   */
  private void setEndOffsets() throws StoreException {
    for (Offset segmentStartOffset : validIndexSegments.keySet()) {
      Offset nextIndexSegmentStartOffset = validIndexSegments.higherKey(segmentStartOffset);
      if (nextIndexSegmentStartOffset == null || !segmentStartOffset.getName()
          .equals(nextIndexSegmentStartOffset.getName())) {
        // this is the last index segment for the log segment it refers to.
        IndexSegment indexSegment = validIndexSegments.get(segmentStartOffset);
        LogSegment logSegment = log.getSegment(segmentStartOffset.getName());
        logSegment.setEndOffset(indexSegment.getEndOffset().getOffset());
      }
    }
  }

  /**
   * Maybe try to recover from partial log segment. Calling this method to see if we should continue recovering messages
   * from the given {@link com.github.ambry.store.MessageStoreRecovery.RecoveryResult}.
   * If there is no exception in the result, then continue.
   * If partial recovery is not enable, or the log segment is not the last log segment, don't continue;
   * If there are too many bytes in the remainiikng file, don't continue;
   * @param logSegmentToRecover
   * @param recoveryResult
   * @throws StoreException
   * @throws IOException
   */
  private void maybePerformaPartialRecovery(LogSegment logSegmentToRecover,
      MessageStoreRecovery.RecoveryResult recoveryResult) throws StoreException, IOException {
    // There is no exception, then partial recovery is not necessary
    if (recoveryResult.recoveryException == null) {
      return;
    }
    if (!config.storeEnablePartialLogSegmentRecovery || logSegmentToRecover != log.getLastSegment()) {
      // Partial log segment recovery is not enabled, or the log segment is not the last one
      throw recoveryResult.recoveryException;
    }
    // If we are here, then this is the last log segment.
    long startOffsetForBrokenMessage = recoveryResult.currentStartOffset;
    long endOffset = logSegmentToRecover.sizeInBytes();
    long remainingDataSize = endOffset - startOffsetForBrokenMessage;
    if (remainingDataSize > config.storePartialLogSegmentRecoveryRemainingDataSizeThreshold) {
      throw recoveryResult.recoveryException;
    }
    logger.info("Index: {} Partial LogSegment recovery, log segment {} has valid message until {}, the file size is {}",
        dataDir, logSegmentToRecover.getName(), startOffsetForBrokenMessage, endOffset);
    recoveryFromPartialLogSegmentCounter.incrementAndGet();
    // Since we use fallocate --keep-size to preallocate log segment file, we can truncate log segment file here
    // It will only change the logical file size, not the preallocated disk space.
    logSegmentToRecover.truncateTo(startOffsetForBrokenMessage);
    hasPartialRecovery = true;
  }

  /**
   * @return the map of {@link Offset} to {@link IndexSegment} instances.
   */
  ConcurrentSkipListMap<Offset, IndexSegment> getIndexSegments() {
    return validIndexSegments;
  }

  /**
   * @return All the {@link LogSegment}s.
   */
  List<LogSegment> getLogSegments() {
    List<LogSegment> result = new ArrayList<>();
    LogSegment segment = log.getFirstSegment();
    while (segment != null) {
      result.add(segment);
      segment = log.getNextSegment(segment);
    }
    return result;
  }

  /**
   * Update the partial log segment info including the partialLogSegmentCount and wastedLogSegmentSpace
   */
  void updatePartialLogSegmentInfo() {
    int partialCount = 0;
    long wastedSpace = 0;
    LogSegment segment = log.getFirstSegment();
    LogSegmentName journalName = null;
    if (journal.getCurrentNumberOfEntries() != 0) {
      journalName = journal.getFirstOffset().getName();
    }
    while (segment != null) {
      // exclude journal
      if (journalName != null && segment.getName().equals(journalName)) {
        break;
      }
      if (segment.getEndOffset() < segment.getCapacityInBytes() * 0.8) {
        partialCount++;
      }
      wastedSpace += segment.getCapacityInBytes() - segment.getEndOffset();
      segment = log.getNextSegment(segment);
    }
    partialLogSegmentCount.set(partialCount);
    wastedLogSegmentSpace.set(wastedSpace);
  }

  /**
   * Atomically adds {@code segmentFilesToAdd} to and removes {@code segmentsToRemove} from the map of {@link Offset} to
   * {@link IndexSegment} instances.
   * @param segmentFilesToAdd the backing files of the {@link IndexSegment} instances to add.
   * @param segmentsToRemove the start {@link Offset} of {@link IndexSegment} instances to remove.
   * @throws IllegalArgumentException if any {@link IndexSegment} that needs to be added or removed has an offset higher
   * than that in the journal.
   * @throws StoreException if an {@link IndexSegment} instance cannot be created for the provided files to add.
   */
  void changeIndexSegments(List<File> segmentFilesToAdd, Set<Offset> segmentsToRemove) throws StoreException {
    rwLock.writeLock().lock();
    try {
      Offset journalFirstOffset = journal.getFirstOffset();

      TreeMap<Offset, IndexSegment> segmentsToAdd = new TreeMap<>();
      for (File indexSegmentFile : segmentFilesToAdd) {
        IndexSegment indexSegment = new IndexSegment(indexSegmentFile, true, factory, config, metrics, journal, time);
        if (journalFirstOffset != null && indexSegment.getEndOffset().compareTo(journalFirstOffset) > 0) {
          throw new IllegalArgumentException(
              "One of the index segments has an end offset " + indexSegment.getEndOffset()
                  + " that is higher than the first offset in the journal " + journalFirstOffset);
        }
        segmentsToAdd.put(indexSegment.getStartOffset(), indexSegment);
      }

      for (Offset offset : segmentsToRemove) {
        IndexSegment segmentToRemove = validIndexSegments.get(offset);
        if (journalFirstOffset != null && segmentToRemove.getEndOffset().compareTo(journalFirstOffset) >= 0) {
          throw new IllegalArgumentException(
              "End Offset of the one of the segments to remove [" + segmentToRemove.getFile() + "] is"
                  + " higher than the first offset in the journal");
        }
      }

      // first update the influx index segments reference
      inFluxIndexSegments = new ConcurrentSkipListMap<>();
      // now copy over all valid segments to the influx reference, remove ones that need removing and add the new ones.
      inFluxIndexSegments.putAll(validIndexSegments);
      inFluxIndexSegments.keySet().removeAll(segmentsToRemove);
      inFluxIndexSegments.putAll(segmentsToAdd);
      // change the reference (this is guaranteed to be atomic by java)
      validIndexSegments = inFluxIndexSegments;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Update the before and after compaction index segment offsets.
   * @param compactionStartTime The compaction start time in milliseconds.
   * @param offsets The map from index segment offsets before compaction to index segment offsets after compaction.
   * @param fromInProgressCompaction True if this update is from an in progress compaction.
   */
  void updateBeforeAndAfterCompactionIndexSegmentOffsets(long compactionStartTime, Map<Offset, Offset> offsets,
      boolean fromInProgressCompaction) {
    if (offsets.size() == 0) {
      return;
    }
    if (fromInProgressCompaction) {
      // Sanity checking
      for (Offset before : offsets.keySet()) {
        if (validIndexSegments.get(before) == null) {
          String message =
              String.format("Index %s: Before offset %s is missing from the index segment map", dataDir, before);
          throw new IllegalStateException(message);
        }
      }
    }
    compactionTimestampToIndexSegmentOffsets.computeIfAbsent(compactionStartTime, k -> new HashSet<>())
        .addAll(offsets.keySet());
    beforeAndAfterCompactionIndexSegmentOffsets.putAll(offsets);
    long now = time.milliseconds();
    long cutoffTime = now - Duration.ofDays(config.storeCompactionHistoryInDay).toMillis();
    // Any compaction history before this cutoff timestamp would be removed
    // compactionTimestampToIndexSegmentOffsets is a tree map order by time
    Iterator<Map.Entry<Long, Set<Offset>>> iterator = compactionTimestampToIndexSegmentOffsets.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Set<Offset>> entry = iterator.next();
      if (entry.getKey() > cutoffTime) {
        break;
      }
      Set<Offset> offsetsToRemove = entry.getValue();
      beforeAndAfterCompactionIndexSegmentOffsets.keySet().removeAll(offsetsToRemove);
      iterator.remove();
    }
  }

  /**
   * Sanity check the before and after compaction index segment offsets.
   * - All before offsets shouldn't point to any existing valid index segment
   * - All before offsets should have a valid path to an existing valid index segment.
   */
  void sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets() {
    Set<Offset> seen = new HashSet<>();
    for (Map.Entry<Offset, Offset> entry : beforeAndAfterCompactionIndexSegmentOffsets.entrySet()) {
      // Before should not in the index segment map
      Offset before = entry.getKey();
      Offset after = entry.getValue();
      if (validIndexSegments.get(before) != null) {
        logger.error("Index {}: Before Offset {} shouldn't be in the index segment map", dataDir, before);
        metrics.beforeAndAfterOffsetSanityCheckFailureCount.inc();
        sanityCheckFailed = true;
        return;
      }
      seen.clear();
      seen.add(before);
      seen.add(after);
      // There should be a path from before offset to the current valid index segment.
      IndexSegment segment = validIndexSegments.get(after);
      while (segment == null) {
        after = beforeAndAfterCompactionIndexSegmentOffsets.get(after);
        if (after == null) {
          break;
        }
        if (seen.contains(after)) {
          logger.error("Index {}: loop in the before and after for Offset: {}", dataDir, before);
          sanityCheckFailed = true;
          return;
        } else {
          seen.add(after);
        }
        segment = validIndexSegments.get(after);
      }
      if (segment == null) {
        logger.error("Index {}: No path for Offset {} to reach a valid index segment", dataDir, before);
        metrics.beforeAndAfterOffsetSanityCheckFailureCount.inc();
        sanityCheckFailed = true;
        return;
      }
    }
    logger.info("Index {}: Sanity check for before and after compaction index segment offsets succeeded", dataDir);
    sanityCheckFailed = false;
  }

  /**
   * Return the before and after compaction index segment offsets map, this should only be used for tests.
   * @return
   */
  Map<Offset, Offset> getBeforeAndAfterCompactionIndexSegmentOffsets() {
    return Collections.unmodifiableMap(beforeAndAfterCompactionIndexSegmentOffsets);
  }

  /**
   * Return the compaction timestamp to index segment offsets map, this should only be used for tests.
   * @return
   */
  Map<Long, Set<Offset>> getCompactionTimestampToIndexSegmentOffsets() {
    return Collections.unmodifiableMap(compactionTimestampToIndexSegmentOffsets);
  }

  /**
   * Enable RebuildTokenBasedOnCompactionHistory
   */
  void enableRebuildTokenBasedOnCompactionHistory() {
    logger.info("Index {}: Enable rebuild token based on compaction history", dataDir);
    shouldRebuildTokenBasedOnCompactionHistory = true;
  }

  /**
   * @return {@link #sanityCheckFailed}. Only used in tests.
   */
  boolean isSanityCheckFailed() {
    return sanityCheckFailed;
  }

  /**
   * Adds a new entry to the index. For recovery, only the index segments belongs to last log segment should be put into
   * journal, otherwise it won't be compacted. And we will face the issue when last log segment is empty but journal size
   * is non-zero.
   * @param entry The entry to be added to the index
   * @param fileSpan The file span that this entry represents in the log
   * @throws StoreException
   */
  void addToIndex(IndexEntry entry, FileSpan fileSpan) throws StoreException {
    validateFileSpan(fileSpan, true);
    if (needToRollOverIndex(entry)) {
      int valueSize = entry.getValue().getBytes().capacity();
      int entrySize = entry.getKey().sizeInBytes() + valueSize;
      IndexSegment info =
          new IndexSegment(dataDir, entry.getValue().getOffset(), factory, entrySize, valueSize, config, metrics, time);
      info.addEntry(entry, fileSpan.getEndOffset());
      // always add to both valid and in-flux index segment map to account for the fact that changeIndexSegments()
      // might be in the process of updating the reference to validIndexSegments
      validIndexSegments.put(info.getStartOffset(), info);
      inFluxIndexSegments.put(info.getStartOffset(), info);
    } else {
      validIndexSegments.lastEntry().getValue().addEntry(entry, fileSpan.getEndOffset());
    }
    journal.addEntry(entry.getValue().getOffset(), entry.getKey(), entry.getCrc());
  }

  /**
   * Adds a set of entries to the index
   * @param entries The entries to be added to the index
   * @param fileSpan The file span that the entries represent in the log
   * @throws StoreException
   */
  void addToIndex(List<IndexEntry> entries, FileSpan fileSpan) throws StoreException {
    validateFileSpan(fileSpan, false);
    for (IndexEntry entry : entries) {
      Offset startOffset = entry.getValue().getOffset();
      Offset endOffset = new Offset(startOffset.getName(), startOffset.getOffset() + entry.getValue().getSize());
      addToIndex(entry, new FileSpan(startOffset, endOffset));
    }
  }

  /**
   * Return the start offset of the active index segment. If there is no index segment at all, just return the beginning
   * offset of the entire log.
   * @return
   */
  Offset getActiveIndexSegmentOffset() {
    Offset logEndOffset = log.getEndOffset();
    Map.Entry<Offset, IndexSegment> lastEntry = validIndexSegments.lastEntry();
    // Fetch the log end offset before last key in the map
    if (lastEntry != null) {
      return lastEntry.getKey();
    } else {
      // No index segment in the map, then this log is empty. End offset is the start offset.
      logger.trace("Index {}: returning log end offset as the active index segment offset: {}", dataDir, logEndOffset);
      return logEndOffset;
    }
  }

  /**
   * Checks if the index segment needs to roll over to a new segment
   * @param entry The new entry that needs to be added to the existing active segment
   * @return True, if segment needs to roll over. False, otherwise
   */
  private boolean needToRollOverIndex(IndexEntry entry) {
    Offset offset = entry.getValue().getOffset();
    int thisValueSize = entry.getValue().getBytes().capacity();
    if (validIndexSegments.size() == 0) {
      logger.info("Creating first segment with start offset {}", offset);
      return true;
    }
    IndexSegment lastSegment = validIndexSegments.lastEntry().getValue();
    if (lastSegment.getVersion() != PersistentIndex.CURRENT_VERSION) {
      logger.info("Index: {} Rolling over because the Index version has changed from {} to {}", dataDir,
          lastSegment.getVersion(), PersistentIndex.CURRENT_VERSION);
      return true;
    }
    //Must check the log segment name before checking the size written for the edge case when last index segment is
    //sealed after log segment gets auto closed.
    if (!offset.getName().equals(lastSegment.getLogSegmentName())) {
      logger.info("Index: {} Rolling over from {} to {} because the log has rolled over from {} to {}", dataDir,
          lastSegment.getStartOffset(), offset, lastSegment.getLogSegmentName(), offset.getName());
      return true;
    }
    if (lastSegment.getSizeWritten() >= maxInMemoryIndexSizeInBytes) {
      logger.info("Index: {} Rolling over from {} to {} because the size written {} >= maxInMemoryIndexSizeInBytes {}",
          dataDir, lastSegment.getStartOffset(), offset, lastSegment.getSizeWritten(), maxInMemoryIndexSizeInBytes);
      return true;
    }
    if (lastSegment.getNumberOfItems() >= maxInMemoryNumElements) {
      logger.info(
          "Index: {} Rolling over from {} to {} because the number of items in the last segment: {} >= maxInMemoryNumElements {}",
          dataDir, lastSegment.getStartOffset(), offset, lastSegment.getNumberOfItems(), maxInMemoryNumElements);
      return true;
    }
    if (lastSegment.getValueSize() != thisValueSize) {
      logger.info(
          "Index: {} Rolling over from {} to {} because the segment value size: {} != current entry value size: {}",
          dataDir, lastSegment.getStartOffset(), offset, lastSegment.getValueSize(), thisValueSize);
      return true;
    }
    return false;
  }

  /**
   * Finds {@link IndexValue} that represents the latest state of the given {@code key} in the index and return it. If
   * not found, returns null.
   * <br>
   * This method returns the final state of the given {@code key}. The final state of a key can be a Put value, a Delete
   * value and a Undelete value. Ttl_update isn't considered as final state as it just update the expiration date.
   * {@link IndexValue} returned by this method would carry expiration date from Ttl_update if there is one.
   * @param key  The key to find in the index
   * @return The blob index value associated with the key. Null if the key is not found.
   * @throws StoreException
   */
  IndexValue findKey(StoreKey key) throws StoreException {
    return findKey(key, null, EnumSet.of(IndexEntryType.PUT, IndexEntryType.DELETE, IndexEntryType.UNDELETE));
  }

  /**
   * Finds the latest {@link IndexValue} associated with the {@code key} that matches any of the provided {@code types}
   * if present in the index within the given {@code fileSpan}.
   * @param key The key to do the exist check against
   * @param fileSpan FileSpan which specifies the range within which search should be made
   * @return The latest {@link IndexValue} for {@code key} conforming to one of the types {@code types} - if one exists
   * within the {@code fileSpan}, {@code null} otherwise.
   * @throws StoreException
   */
  IndexValue findKey(StoreKey key, FileSpan fileSpan) throws StoreException {
    return findKey(key, fileSpan, EnumSet.of(IndexEntryType.PUT, IndexEntryType.DELETE, IndexEntryType.UNDELETE));
  }

  /**
   * Finds the latest {@link IndexValue} associated with the {@code key} that matches any of the provided {@code types}
   * if present in the index within the given {@code fileSpan}.
   * @param key The key to do the exist check against
   * @param fileSpan FileSpan which specifies the range within which search should be made
   * @param types the types of {@link IndexEntryType} to look for. The latest entry matching one of the types will be
   *              returned
   * @return The latest {@link IndexValue} for {@code key} conforming to one of the types {@code types} - if one exists
   * within the {@code fileSpan}, {@code null} otherwise.
   * @throws StoreException
   */
  IndexValue findKey(StoreKey key, FileSpan fileSpan, EnumSet<IndexEntryType> types) throws StoreException {
    return findKey(key, fileSpan, types, validIndexSegments);
  }

  /**
   * Finds the latest {@link IndexValue} associated with the {@code key} that matches any of the provided {@code types}
   * if present in the index within the given {@code fileSpan}.
   * @param key the {@link StoreKey} whose {@link IndexValue} is required.
   * @param fileSpan {@link FileSpan} which specifies the range within which search should be made
   * @param types the types of {@link IndexEntryType} to look for. The latest entry matching one of the types will be
   *              returned
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return The latest {@link IndexValue} for {@code key} conforming to one of the types {@code types} - if one exists
   * within the {@code fileSpan}, {@code null} otherwise.
   * @throws StoreException
   */
  private IndexValue findKey(StoreKey key, FileSpan fileSpan, EnumSet<IndexEntryType> types,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    if (fileSpan != null && fileSpan.isEmpty()) {
      return null;
    }
    IndexValue latest = null;
    IndexValue retCandidate = null;
    final Timer.Context context = metrics.findTime.time();
    try {
      ConcurrentNavigableMap<Offset, IndexSegment> segmentsMapToSearch;
      if (fileSpan == null) {
        logger.trace("Searching for {} in the entire index", key);
        segmentsMapToSearch = indexSegments.descendingMap();
      } else {
        logger.trace("Searching for {} in index with filSpan ranging from {} to {}", key, fileSpan.getStartOffset(),
            fileSpan.getEndOffset());
        Offset fromIndexOffset = indexSegments.floorKey(fileSpan.getStartOffset());
        Offset toIndexOffset = indexSegments.lowerKey(fileSpan.getEndOffset());
        if (toIndexOffset == null) {
          segmentsMapToSearch = new ConcurrentSkipListMap<>();
        } else {
          segmentsMapToSearch =
              indexSegments.subMap(fromIndexOffset == null ? indexSegments.firstKey() : fromIndexOffset, true,
                  toIndexOffset, true).descendingMap();
        }
        metrics.segmentSizeForExists.update(segmentsMapToSearch.size());
      }
      int segmentsSearched = 0;
      for (Map.Entry<Offset, IndexSegment> entry : segmentsMapToSearch.entrySet()) {
        segmentsSearched++;
        logger.trace("Index : {} searching index with start offset {}", dataDir, entry.getKey());
        NavigableSet<IndexValue> values = entry.getValue().find(key);
        if (values != null) {
          Iterator<IndexValue> it = values.descendingIterator();
          while (it.hasNext()) {
            IndexValue value = it.next();
            if (fileSpan != null) {
              // Start <= value.offset < End
              if (value.getOffset().compareTo(fileSpan.getEndOffset()) >= 0) {
                continue;
              }
              if (value.getOffset().compareTo(fileSpan.getStartOffset()) < 0) {
                break;
              }
            }
            if (latest == null) {
              latest = value;
            }
            logger.trace("Index : {} found value offset {} size {} ttl {}", dataDir, value.getOffset(), value.getSize(),
                value.getExpiresAtMs());
            if (types.contains(IndexEntryType.DELETE) && value.isDelete()) {
              retCandidate = value;
              break;
            } else if (types.contains(IndexEntryType.UNDELETE) && value.isUndelete()) {
              retCandidate = value;
              break;
            } else if (types.contains(IndexEntryType.TTL_UPDATE) && !value.isDelete() && !value.isUndelete()
                && value.isTtlUpdate()) {
              retCandidate = value;
              break;
            } else if (types.contains(IndexEntryType.PUT) && value.isPut()) {
              retCandidate = value;
              break;
            }
            // note that it is not possible for a TTL update record to exist for a key but not have a PUT or DELETE
            // record.
          }
          if (retCandidate != null) {
            // merge entries if required to account for updated fields
            if (latest.isTtlUpdate() && !retCandidate.isTtlUpdate()) {
              retCandidate = new IndexValue(retCandidate.getOffset().getName(), retCandidate.getBytes(),
                  retCandidate.getFormatVersion());
              retCandidate.setFlag(IndexValue.Flags.Ttl_Update_Index);
              retCandidate.setExpiresAtMs(latest.getExpiresAtMs());
            }
            break;
          }
        }
      }
      metrics.segmentsAccessedPerBlobCount.update(segmentsSearched);
    } finally {
      context.stop();
    }
    if (retCandidate != null) {
      logger.trace("Index : {} Returning value offset {} size {} ttl {}", dataDir, retCandidate.getOffset(),
          retCandidate.getSize(), retCandidate.getExpiresAtMs());
    }
    return retCandidate;
  }

  /**
   * Finds all the {@link IndexValue}s associated with the given {@code key} that matches any of the provided {@code types}
   * if present in the index with the given {@code fileSpan} and return them in reversed chronological order. If there is
   * no matched {@link IndexValue}, this method would return null;
   * @param key the {@link StoreKey} whose {@link IndexValue} is required.
   * @param fileSpan {@link FileSpan} which specifies the range within which search should be made.
   * @return The list of the {@link IndexValue}s for {@code key} conforming to one of the types {@code types}.
   * @throws StoreException any error.
   */
  List<IndexValue> findAllIndexValuesForKey(StoreKey key, FileSpan fileSpan) throws StoreException {
    return findAllIndexValuesForKey(key, fileSpan, EnumSet.allOf(IndexEntryType.class), validIndexSegments);
  }

  /**
   * Finds all the {@link IndexValue}s associated with the given {@code key} that matches any of the provided {@code types}
   * if present in the index with the given {@code fileSpan} and return them in reversed chronological order. If there is
   * no matched {@link IndexValue}, this method would return null;
   * @param key the {@link StoreKey} whose {@link IndexValue} is required.
   * @param fileSpan {@link FileSpan} which specifies the range within which search should be made.
   * @param types the types of {@link IndexEntryType} to look for.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return The list of the {@link IndexValue}s for {@code key} conforming to one of the types {@code types} ordered
   *         from most to least recent.
   * @throws StoreException any error.
   */
  List<IndexValue> findAllIndexValuesForKey(StoreKey key, FileSpan fileSpan, EnumSet<IndexEntryType> types,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    List<IndexValue> result = new ArrayList<>();
    if (fileSpan != null && fileSpan.isEmpty()) {
      return result;
    }
    final Timer.Context context = metrics.findTime.time();
    try {
      ConcurrentNavigableMap<Offset, IndexSegment> segmentsMapToSearch;
      if (fileSpan == null) {
        logger.trace("Searching all indexes for {} in the entire index", key);
        segmentsMapToSearch = indexSegments.descendingMap();
      } else {
        logger.trace("Searching all indexes for {} in index with fileSpan ranging from {} to {}", key,
            fileSpan.getStartOffset(), fileSpan.getEndOffset());
        Offset fromIndexOffset = indexSegments.floorKey(fileSpan.getStartOffset());
        Offset toIndexOffset = indexSegments.lowerKey(fileSpan.getEndOffset());
        if (toIndexOffset == null) {
          segmentsMapToSearch = new ConcurrentSkipListMap<>();
        } else {
          segmentsMapToSearch =
              indexSegments.subMap(fromIndexOffset == null ? indexSegments.firstKey() : fromIndexOffset, true,
                  indexSegments.lowerKey(fileSpan.getEndOffset()), true).descendingMap();
        }
        metrics.segmentSizeForExists.update(segmentsMapToSearch.size());
      }
      int segmentsSearched = 0;
      for (Map.Entry<Offset, IndexSegment> entry : segmentsMapToSearch.entrySet()) {
        segmentsSearched++;
        logger.trace("Index : {} searching all indexes with start offset {}", dataDir, entry.getKey());
        NavigableSet<IndexValue> values = entry.getValue().find(key);
        if (values != null) {
          Iterator<IndexValue> it = values.descendingIterator();
          while (it.hasNext()) {
            IndexValue value = it.next();
            if (fileSpan != null) {
              // Start <= value.offset < End
              if (value.getOffset().compareTo(fileSpan.getEndOffset()) >= 0) {
                continue;
              }
              if (value.getOffset().compareTo(fileSpan.getStartOffset()) < 0) {
                break;
              }
            }
            if ((types.contains(IndexEntryType.DELETE) && value.isDelete()) || (types.contains(IndexEntryType.UNDELETE)
                && value.isUndelete()) || (types.contains(IndexEntryType.TTL_UPDATE) && !value.isDelete()
                && !value.isUndelete() && value.isTtlUpdate()) || (types.contains(IndexEntryType.PUT)
                && value.isPut())) {
              // Add a copy of the value to the result since we return a modifiable list to the caller.
              result.add(new IndexValue(value));
            }
          }
        }
      }
      metrics.segmentsAccessedPerBlobCount.update(segmentsSearched);
    } finally {
      context.stop();
    }
    if (!result.isEmpty()) {
      logger.trace("Index: {} Returning values {}", dataDir, result);
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Ensure that the previous {@link IndexValue}s is structured correctly for undeleting the {@code key}.
   * @param key the key to be undeleted.
   * @param values the previous {@link IndexValue}s in reversed order.
   * @param lifeVersion lifeVersion for the undelete record, it's only valid when in recovery or replication.
   */
  void validateSanityForUndelete(StoreKey key, List<IndexValue> values, short lifeVersion) throws StoreException {
    if (values == null || values.isEmpty()) {
      throw new StoreException("Id " + key + " not found in " + dataDir, StoreErrorCodes.IDNotFound);
    }
    if (!IndexValue.hasLifeVersion(lifeVersion)) {
      validateSanityForUndeleteWithoutLifeVersion(key, values);
      return;
    }
    // This is from replication. For replication, undelete should be permitted only when
    // 1. The oldest record is PUT
    // 2. the latest record's lifeVersion is less then undelete's lifeVersion.
    // 3. Replication doesn't care if the latest record is delete or not. In local, we can have a PUT record when
    // the remote has PUT, DELETE, UNDELETE. When replicating from remote, local has to insert UNDELETE, where
    // there is no prior DELETE.
    // 4. Replication doesn't care if the latest record is expired or not. In local, we can have a PUT record when
    // the remote has PUT, DELETE, UNDELETE TTL_UPDATE. When replicating from remote, PUT might already have expired.
    // But later we will apply TTL_UPDATE. The alternative idea is to change the logic of checking expiration from
    // comparing current time to comparing operation time.
    IndexValue latestValue = values.get(0);
    IndexValue oldestValue = values.get(values.size() - 1);
    if (!oldestValue.isPut()) {
      throw new StoreException("Id " + key + " requires first value to be a put in index " + dataDir,
          StoreErrorCodes.IDDeletedPermanently);
    }
    if (latestValue.getLifeVersion() == lifeVersion && latestValue.isUndelete()) {
      throw new IdUndeletedStoreException(
          "Id " + key + " already undeleted at lifeVersion " + lifeVersion + " in index " + dataDir, lifeVersion);
    }
    if (latestValue.getLifeVersion() >= lifeVersion) {
      throw new StoreException(
          "LifeVersion conflict in index. Id " + key + " LifeVersion: " + latestValue.getLifeVersion()
              + " Undelete LifeVersion: " + lifeVersion, StoreErrorCodes.LifeVersionConflict);
    }
  }

  /**
   * Ensure that the previous {@link IndexValue}s is structured correctly for undeleting the {@code key} when there is
   * no lifeVersion provided.
   * <p/>
   * Undelete should be permitted only when
   * <ol>
   *   <li>The earliest record is PUT</li>
   *   <li>The latest record is DELETE</li>
   *   <li>DELETE hasn't reached it's retention date</li>
   *   <li>Key is not expired</li>
   * </ol>
   * @param key the key to be undeleted.
   * @param values the previous {@link IndexValue}s in reversed order.
   */
  void validateSanityForUndeleteWithoutLifeVersion(StoreKey key, List<IndexValue> values) throws StoreException {
    if (values.size() == 1) {
      IndexValue value = values.get(0);
      if (value.isDelete() || value.isTtlUpdate()) {
        throw new StoreException("Id " + key + " is compacted in index " + dataDir,
            StoreErrorCodes.IDDeletedPermanently);
      } else if (value.isPut()) {
        throw new StoreException("Id " + key + " is not deleted yet in index " + dataDir, StoreErrorCodes.IDNotDeleted);
      } else {
        throw new IdUndeletedStoreException("Id " + key + " is already undeleted in index " + dataDir,
            value.getLifeVersion());
      }
    }
    // Oldest value has to be put and latest value has to be a delete.
    // PutRecord can't expire and delete record can't be older than the delete retention time.
    IndexValue latestValue = values.get(0);
    IndexValue oldestValue = values.get(values.size() - 1);
    if (latestValue.isUndelete()) {
      throw new IdUndeletedStoreException("Id " + key + " is already undeleted in index " + dataDir,
          latestValue.getLifeVersion());
    }
    if (!oldestValue.isPut() || !latestValue.isDelete()) {
      throw new StoreException(
          "Id " + key + " requires first value to be a put and last value to be a delete in index " + dataDir,
          StoreErrorCodes.IDNotDeleted);
    }
    if (latestValue.getOperationTimeInMs() + TimeUnit.MINUTES.toMillis(config.storeDeletedMessageRetentionMinutes)
        < time.milliseconds()) {
      throw new StoreException("Id " + key + " already permanently deleted in index " + dataDir,
          StoreErrorCodes.IDDeletedPermanently);
    }
    if (isExpired(latestValue)) {
      throw new StoreException("Id " + key + " already expired in index " + dataDir, StoreErrorCodes.TTLExpired);
    }
  }

  /**
   * Returns true if the given message was recently seen by this Index or crc is null from either {@link MessageInfo} or
   * {@link Journal}.
   * @param info the {@link MessageInfo} to check.
   * @return true if the exact message was recently added to this index; false otherwise.
   */
  boolean wasRecentlySeenOrCrcIsNull(MessageInfo info) {
    Long crcInJournal = journal.getCrcOfKey(info.getStoreKey());
    return info.getCrc() == null || crcInJournal == null || info.getCrc().equals(crcInJournal);
  }

  /**
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file span represented by this entry in the log
   * @param deletionTimeMs deletion time of the entry in ms
   * @return the {@link IndexValue} of the delete record
   * @throws StoreException
   */
  IndexValue markAsDeleted(StoreKey id, FileSpan fileSpan, long deletionTimeMs) throws StoreException {
    return markAsDeleted(id, fileSpan, null, deletionTimeMs, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file span represented by this entry in the log
   * @param info this needs to be non-null in the case of recovery. Can be {@code null} otherwise. Used if the PUT
   *             record could not be found
   * @param deletionTimeMs deletion time of the blob. In-case of recovery, deletion time is obtained from {@code info}.
   * @param lifeVersion lifeVersion of this delete record.
   * @return the {@link IndexValue} of the delete record
   * @throws StoreException
   */
  IndexValue markAsDeleted(StoreKey id, FileSpan fileSpan, MessageInfo info, long deletionTimeMs, short lifeVersion)
      throws StoreException {
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    validateFileSpan(fileSpan, true);
    IndexValue value = findKey(id);
    if (value == null && info == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.IDNotFound);
    } else if (value != null) {
      if (hasLifeVersion) {
        // When this method is invoked in either recovery or replication, delete can follow any index value.
        if ((value.isDelete() && value.getLifeVersion() >= lifeVersion) || (value.getLifeVersion() > lifeVersion)) {
          throw new StoreException("LifeVersion conflict in index. Id " + id + " LifeVersion: " + value.getLifeVersion()
              + " Delete LifeVersion: " + lifeVersion, StoreErrorCodes.LifeVersionConflict);
        }
      } else {
        if (value.isDelete()) {
          throw new StoreException("Id " + id + " already deleted in index " + dataDir, StoreErrorCodes.IDDeleted);
        }
      }
    }
    long size = fileSpan.getEndOffset().getOffset() - fileSpan.getStartOffset().getOffset();
    IndexValue newValue;
    if (value == null) {
      // It is possible that the PUT has been cleaned by compaction or forceDelete by the replication
      if (!hasLifeVersion) {
        throw new StoreException("MessageInfo of delete carries invalid lifeVersion",
            StoreErrorCodes.InitializationError);
      }
      newValue =
          new IndexValue(size, fileSpan.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE, info.getExpirationTimeInMs(),
              info.getOperationTimeMs(), info.getAccountId(), info.getContainerId(), lifeVersion);
      newValue.clearOriginalMessageOffset();
    } else {
      lifeVersion = hasLifeVersion ? lifeVersion : value.getLifeVersion();
      newValue =
          new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(), deletionTimeMs,
              value.getAccountId(), value.getContainerId(), lifeVersion);
      newValue.setNewOffset(fileSpan.getStartOffset());
      // Only set the original message offset when the value is put
      if (!value.isPut()) {
        newValue.clearOriginalMessageOffset();
      }
      newValue.setNewSize(size);
    }
    newValue.clearFlag(IndexValue.Flags.Undelete_Index);
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    addToIndex(new IndexEntry(id, newValue, null), fileSpan);
    return newValue;
  }

  /**
   * Marks a blob as permanent
   * @param id the {@link StoreKey} of the blob
   * @param fileSpan the file span represented by this entry in the log
   * @param operationTimeMs the time of the update operation
   * @return the {@link IndexValue} of the ttl update record
   * @throws StoreException if there is any problem writing the index record
   */
  IndexValue markAsPermanent(StoreKey id, FileSpan fileSpan, long operationTimeMs) throws StoreException {
    return markAsPermanent(id, fileSpan, null, operationTimeMs, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Marks a blob as permanent
   * @param id the {@link StoreKey} of the blob
   * @param fileSpan the file span represented by this entry in the log
   * @param operationTimeMs the time of the update operation
   * @param info this needs to be non-null in the case of recovery. Can be {@code null} otherwise. Used if the PUT
   *             record could not be found
   * @param lifeVersion lifeVersion of this ttlUpdate record.
   * @return the {@link IndexValue} of the ttl update record
   * @throws StoreException if there is any problem writing the index record
   */
  IndexValue markAsPermanent(StoreKey id, FileSpan fileSpan, MessageInfo info, long operationTimeMs, short lifeVersion)
      throws StoreException {
    validateFileSpan(fileSpan, true);
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    IndexValue value = findKey(id);
    if (value == null && info == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.IDNotFound);
    }
    short retrievedLifeVersion = value == null ? info.getLifeVersion() : value.getLifeVersion();
    if (value != null && value.isDelete()) {
      throw new StoreException("Id " + id + " deleted in index " + dataDir, StoreErrorCodes.IDDeleted);
    } else if (value != null && value.isTtlUpdate()) {
      throw new StoreException("TTL of " + id + " already updated in index" + dataDir, StoreErrorCodes.AlreadyUpdated);
    } else if (hasLifeVersion && retrievedLifeVersion > lifeVersion) {
      throw new StoreException("LifeVersion conflict in index. Id " + id + " LifeVersion: " + retrievedLifeVersion
          + " Undelete LifeVersion: " + lifeVersion, StoreErrorCodes.LifeVersionConflict);
    }
    long size = fileSpan.getEndOffset().getOffset() - fileSpan.getStartOffset().getOffset();
    IndexValue newValue;

    if (value == null) {
      // It is possible that the PUT has been cleaned by compaction
      // but the TTL update is going to still be placed?
      if (!hasLifeVersion) {
        throw new StoreException("MessageInfo of ttlUpdate carries invalid lifeVersion",
            StoreErrorCodes.InitializationError);
      }
      newValue =
          new IndexValue(size, fileSpan.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE, info.getExpirationTimeInMs(),
              info.getOperationTimeMs(), info.getAccountId(), info.getContainerId(), lifeVersion);
      newValue.clearOriginalMessageOffset();
    } else {
      lifeVersion = hasLifeVersion ? lifeVersion : value.getLifeVersion();
      newValue =
          new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), Utils.Infinite_Time, operationTimeMs,
              value.getAccountId(), value.getContainerId(), lifeVersion);
      newValue.setNewOffset(fileSpan.getStartOffset());
      newValue.setNewSize(size);
    }
    newValue.clearFlag(IndexValue.Flags.Undelete_Index);
    newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
    addToIndex(new IndexEntry(id, newValue, null), fileSpan);
    return newValue;
  }

  /**
   * Marks a blob as undeleted
   * @param id the {@link StoreKey} of the blob
   * @param fileSpan the file span represented by this entry in the log
   * @param operationTimeMs the time of the update operation
   * @return the {@link IndexValue} of the ttl update record
   * @throws StoreException if there is any problem writing the index record
   */
  IndexValue markAsUndeleted(StoreKey id, FileSpan fileSpan, long operationTimeMs) throws StoreException {
    return markAsUndeleted(id, fileSpan, null, operationTimeMs, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Marks a blob as undeleted
   * @param id the {@link StoreKey} of the blob
   * @param fileSpan the file span represented by this entry in the log
   * @param operationTimeMs the time of the update operation
   * @param info this needs to be non-null in the case of recovery. Can be {@code null} otherwise. Used if the PUT
   *             record could not be found
   * @param lifeVersion lifeVersion of this undelete record.
   * @return the {@link IndexValue} of the undelete record
   * @throws StoreException if there is any problem writing the index record
   */
  IndexValue markAsUndeleted(StoreKey id, FileSpan fileSpan, MessageInfo info, long operationTimeMs, short lifeVersion)
      throws StoreException {
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    validateFileSpan(fileSpan, true);
    long size = fileSpan.getEndOffset().getOffset() - fileSpan.getStartOffset().getOffset();
    List<IndexValue> values =
        findAllIndexValuesForKey(id, null, EnumSet.allOf(IndexEntryType.class), validIndexSegments);
    IndexValue newValue;
    if (info != null) {
      // This is from recovery. In recovery, we don't need to do any sanity check because
      // 1. we already know the IndexValue has it's source in the log.
      // 2. some sanity check will fail.
      //    assume we have P, D, U, D in the log, then a compaction cycle compacted P and first D,
      //    then we only have U and second D. U in this case, will have no prior records.
      if (!hasLifeVersion) {
        throw new StoreException("MessageInfo of undelete carries invalid lifeVersion " + lifeVersion,
            StoreErrorCodes.InitializationError);
      }
      newValue =
          new IndexValue(size, fileSpan.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE, info.getExpirationTimeInMs(),
              info.getOperationTimeMs(), info.getAccountId(), info.getContainerId(), lifeVersion);
      if (info.isTtlUpdated() || (values != null && values.get(0).isTtlUpdate())) {
        newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
      }
    } else {
      validateSanityForUndelete(id, values, lifeVersion);
      IndexValue value = values.get(0);
      lifeVersion = hasLifeVersion ? lifeVersion : (short) (value.getLifeVersion() + 1);
      newValue =
          new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(), operationTimeMs,
              value.getAccountId(), value.getContainerId(), lifeVersion);
      newValue.setNewOffset(fileSpan.getStartOffset());
      newValue.setNewSize(size);
    }
    newValue.setFlag(IndexValue.Flags.Undelete_Index);
    newValue.clearFlag(IndexValue.Flags.Delete_Index);
    newValue.clearOriginalMessageOffset();
    addToIndex(new IndexEntry(id, newValue, null), fileSpan);
    return newValue;
  }

  /**
   * Returns the blob read info for a given key
   * @param id The id of the entry whose info is required
   * @param getOptions the get options that indicate whether blob read info for deleted/expired blobs are to be returned.
   * @return The blob read info that contains the information for the given key
   * @throws StoreException
   */
  BlobReadOptions getBlobReadInfo(StoreKey id, EnumSet<StoreGetOptions> getOptions) throws StoreException {
    rwLock.readLock().lock();
    try {
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
      IndexValue value = findKey(id);
      BlobReadOptions readOptions;
      if (value == null) {
        throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.IDNotFound);
      } else if (value.isDelete()) {
        if (!getOptions.contains(StoreGetOptions.Store_Include_Deleted)) {
          throw new StoreException("Id " + id + " has been deleted in index " + dataDir, StoreErrorCodes.IDDeleted);
        } else {
          if (value.getOperationTimeInMs() + TimeUnit.MINUTES.toMillis(config.storeDeletedMessageRetentionMinutes)
              < time.milliseconds()) {
            // fetching a blob that was deleted more than the retention minutes.
            metrics.getOutOfRetentionDeletedBlobCount.inc();
          }
          readOptions = getDeletedBlobReadOptions(value, id, indexSegments);
        }
      } else if (isExpired(value) && !getOptions.contains(StoreGetOptions.Store_Include_Expired)) {
        throw new StoreException("Id " + id + " has expired ttl in index " + dataDir, StoreErrorCodes.TTLExpired);
      } else if (value.isUndelete()) {
        readOptions = getUndeletedBlobReadOptions(value, id, indexSegments);
      } else {
        // This is only for test
        if (getBlobReadInfoTestCallback != null) {
          getBlobReadInfoTestCallback.run();
        }
        readOptions = fromIndexValue(id, value);
      }
      return readOptions;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Gets {@link BlobReadOptions} for a deleted blob.
   * @param value the {@link IndexValue} of the delete index entry for the blob.
   * @param key the {@link StoreKey} for which {@code value} is the delete {@link IndexValue}
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return the {@link BlobReadOptions} that contains the information for the given {@code id}
   * @throws StoreException
   */
  private BlobReadOptions getDeletedBlobReadOptions(IndexValue value, StoreKey key,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    BlobReadOptions readOptions;
    IndexValue putValue =
        findKey(key, new FileSpan(getStartOffset(indexSegments), value.getOffset()), EnumSet.of(IndexEntryType.PUT),
            indexSegments);
    if (putValue != null) {
      // use the expiration time from the original value because it may have been updated
      readOptions = new BlobReadOptions(log, putValue.getOffset(),
          new MessageInfo(key, putValue.getSize(), true, value.isTtlUpdate(), false, value.getExpiresAtMs(), null,
              putValue.getAccountId(), putValue.getContainerId(), putValue.getOperationTimeInMs(),
              value.getLifeVersion()));
    } else {
      // PUT record no longer available. When the PutBlob is compacted or it's force deleted, it throws the ID_Deleted exception.
      throw new StoreException("Did not find PUT index entry for key [" + key
          + "] and the the original offset in value of the DELETE entry was [" + value.getOriginalMessageOffset() + "]",
          StoreErrorCodes.IDDeleted);
    }
    return readOptions;
  }

  /**
   * Gets {@link BlobReadOptions} for a undeleted blob.
   * @param value the {@link IndexValue} of the undelete index entry for the blob.
   * @param key the {@link StoreKey} for which {@code value} is the undelete {@link IndexValue}
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return the {@link BlobReadOptions} that contains the information for the given {@code id}
   * @throws StoreException
   */
  private BlobReadOptions getUndeletedBlobReadOptions(IndexValue value, StoreKey key,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    IndexValue putValue =
        findKey(key, new FileSpan(getStartOffset(indexSegments), value.getOffset()), EnumSet.of(IndexEntryType.PUT),
            indexSegments);
    if (putValue != null) {
      // use the expiration time from the original value because it may have been updated
      // since we are here dealing with undelete blob, we have to return the right life version
      return new BlobReadOptions(log, putValue.getOffset(),
          new MessageInfo(key, putValue.getSize(), false, value.isTtlUpdate(), true, value.getExpiresAtMs(), null,
              putValue.getAccountId(), putValue.getContainerId(), putValue.getOperationTimeInMs(),
              value.getLifeVersion()));
    } else {
      // PUT record no longer available.
      throw new StoreException("Did not find PUT index entry for key [" + key + "] when there is an undelete entry",
          StoreErrorCodes.IDNotFound);
    }
  }

  BlobReadOptions fromIndexValue(StoreKey id, IndexValue value) {
    return new BlobReadOptions(log, value.getOffset(),
        new MessageInfo(id, value.getSize(), value.isDelete(), value.isTtlUpdate(), value.isDelete(),
            value.getExpiresAtMs(), journal.getCrcOfKey(id), value.getAccountId(), value.getContainerId(),
            value.getOperationTimeInMs(), value.getLifeVersion()));
  }

  boolean isExpired(IndexValue value) {
    return value.getExpiresAtMs() != Utils.Infinite_Time && time.milliseconds() > value.getExpiresAtMs();
  }

  /**
   * Returns the list of keys that are not found in the index from the given input keys. This also checks
   * keys that are marked for deletion and those that have an expired ttl
   * @param keys The list of keys that needs to be tested against the index
   * @return The list of keys that are not found in the index
   * @throws StoreException
   */
  Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    Set<StoreKey> missingKeys = new HashSet<StoreKey>();
    for (StoreKey key : keys) {
      if (findKey(key) == null) {
        missingKeys.add(key);
      }
    }
    return missingKeys;
  }

  /**
   * Finds all the entries from the given start token(inclusive). The token defines the start position in the index from
   * where entries needs to be fetched
   * @param token The token that signifies the start position in the index from where entries need to be retrieved
   * @param maxTotalSizeOfEntries The maximum total size of entries that needs to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value.
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    long startTimeInMs = time.milliseconds();
    try {
      Offset logEndOffsetBeforeFind = log.getEndOffset();
      StoreFindToken storeToken = resetTokenIfRequired((StoreFindToken) token);
      logger.trace("Time used to validate token: {}", (time.milliseconds() - startTimeInMs));
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
      storeToken = revalidateStoreFindToken(storeToken, indexSegments, false);

      List<MessageInfo> messageEntries = new ArrayList<>();
      if (!storeToken.getType().equals(FindTokenType.IndexBased)) {
        startTimeInMs = time.milliseconds();
        Offset offsetToStart = storeToken.getOffset();
        if (storeToken.getType().equals(FindTokenType.Uninitialized)) {
          offsetToStart = getStartOffset();
        }
        logger.trace("Index : {} getting entries since {}", dataDir, offsetToStart);
        // check journal
        List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, storeToken.getInclusive());
        logger.trace("Journal based token, Time used to get entries: {}", (time.milliseconds() - startTimeInMs));
        // we deliberately obtain a snapshot of the index segments AFTER fetching from the journal. This ensures that
        // any and all entries returned from the journal are guaranteed to be in the obtained snapshot of indexSegments.
        indexSegments = validIndexSegments;

        // recheck to make sure that offsetToStart hasn't slipped out of the journal. It is possible (but highly
        // improbable) that the offset has slipped out of the journal and has been compacted b/w the time of getting
        // entries from the journal to when another view of the index segments was obtained.
        Offset journalFirstOffset = journal.getFirstOffset();
        if (entries != null && journalFirstOffset != null && offsetToStart.compareTo(journalFirstOffset) >= 0) {
          startTimeInMs = time.milliseconds();
          logger.trace("Index : {} retrieving from journal from offset {} total entries {}", dataDir, offsetToStart,
              entries.size());
          Offset offsetEnd = offsetToStart;
          AtomicLong currentTotalSizeOfEntries = new AtomicLong(0);
          Offset endOffsetOfSnapshot = getCurrentEndOffset(indexSegments);
          Map<StoreKey, MessageInfo> messageInfoCache = new HashMap<>();
          for (JournalEntry entry : entries) {
            addMessageInfoForJournalEntry(entry, endOffsetOfSnapshot, indexSegments, messageInfoCache, messageEntries,
                currentTotalSizeOfEntries);
            offsetEnd = entry.getOffset();
            if (currentTotalSizeOfEntries.get() >= maxTotalSizeOfEntries) {
              break;
            }
          }
          logger.trace("Journal based token, Time used to generate message entries: {}",
              (time.milliseconds() - startTimeInMs));

          startTimeInMs = time.milliseconds();
          logger.trace("Index : {} new offset from find info {}", dataDir, offsetEnd);
          eliminateDuplicates(messageEntries);
          logger.trace("Journal based token, Time used to eliminate duplicates: {}",
              (time.milliseconds() - startTimeInMs));
          IndexSegment segmentOfToken = indexSegments.floorEntry(offsetEnd).getValue();
          StoreFindToken storeFindToken =
              new StoreFindToken(offsetEnd, sessionId, incarnationId, false, segmentOfToken.getResetKey(),
                  segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
          // use the latest ref of indexSegments
          long bytesRead = getTotalBytesRead(storeFindToken, messageEntries, logEndOffsetBeforeFind, indexSegments);
          storeFindToken.setBytesRead(bytesRead);
          return new FindInfo(messageEntries, storeFindToken);
        } else {
          storeToken = revalidateStoreFindToken(storeToken, indexSegments, false);
          offsetToStart = storeToken.getType().equals(FindTokenType.Uninitialized) ? getStartOffset(indexSegments)
              : storeToken.getOffset();
          // Find index segment closest to the token offset.
          // Get entries starting from the first key in this offset.
          Map.Entry<Offset, IndexSegment> entry = indexSegments.floorEntry(offsetToStart);
          StoreFindToken newToken;
          //There are several reasons that we could reach here:
          //1.StoreFindToken is Uninitialized, and we have more index segments than journal can cover.
          //2.StoreFindToken is JournalBased, but the journal somehow moved too much ahead, the offset in the FindToken is no longer within journal.
          //  2.1 There might just be lots of new entries coming to journal so the journal moves ahead.
          //  2.2 The journal might be cleared and new log segment is created for compaction (auto-close last segment)
          if (entry != null && !entry.getKey().equals(indexSegments.lastKey()) || isJournalEmptyAfterAutoClose()) {
            startTimeInMs = time.milliseconds();
            newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries), indexSegments, storeToken.getInclusive());
            logger.trace("Journal based to segment based token, Time used to find entries: {}",
                (time.milliseconds() - startTimeInMs));

            startTimeInMs = time.milliseconds();
            updateStateForMessages(messageEntries);
            logger.trace("Journal based to segment based token, Time used to update state: {}",
                (time.milliseconds() - startTimeInMs));
          } else {
            newToken = storeToken;
          }
          startTimeInMs = time.milliseconds();
          eliminateDuplicates(messageEntries);
          logger.trace("Journal based to segment based token, Time used to eliminate duplicates: {}",
              (time.milliseconds() - startTimeInMs));
          logger.trace("Index [{}]: new FindInfo [{}]", dataDir, newToken);
          long totalBytesRead = getTotalBytesRead(newToken, messageEntries, logEndOffsetBeforeFind, indexSegments);
          newToken.setBytesRead(totalBytesRead);
          return new FindInfo(messageEntries, newToken);
        }
      } else {
        // Index based token
        // Find the index segment corresponding to the token indexStartOffset.
        // Get entries starting from the token Key in this index.
        startTimeInMs = time.milliseconds();
        StoreFindToken newToken;
        //if index based token points to the last entry of last index segment, and journal size cleaned up after last log
        //segment auto close, we should return the original index based token instead of getting from journal.
        if (isJournalEmptyAfterAutoClose() && indexBasedTokenPointsToLastEntryOfLastIndexSegment(storeToken,
            indexSegments)) {
          newToken = storeToken;
        } else {
          newToken = findEntriesFromSegmentStartOffset(storeToken.getOffset(), storeToken.getStoreKey(), messageEntries,
              new FindEntriesCondition(maxTotalSizeOfEntries), indexSegments, storeToken.getInclusive());
        }
        logger.trace("Segment based token, Time used to find entries: {}", (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        updateStateForMessages(messageEntries);
        logger.trace("Segment based token, Time used to update state: {}", (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        eliminateDuplicates(messageEntries);
        logger.trace("Segment based token, Time used to eliminate duplicates: {}",
            (time.milliseconds() - startTimeInMs));

        long totalBytesRead = getTotalBytesRead(newToken, messageEntries, logEndOffsetBeforeFind, indexSegments);
        newToken.setBytesRead(totalBytesRead);
        return new FindInfo(messageEntries, newToken);
      }
    } catch (StoreException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error when finding entries for index " + dataDir, e,
          StoreErrorCodes.UnknownError);
    }
  }

  /**
   * Validate the {@link StoreFindToken} and reset if required.
   * There are several cases where the {@code storeFindToken} has to be reset.
   * <li>Incarnation id is different, meaning that we reconstructed the store, orders of keys might be different.</li>
   * <li>Blob shuts down ungracefully and the offset in the token is larger than the log, meaning the token is ahead of the log.</li>
   * @param storeToken the {@link StoreFindToken} that needs to be validated
   * @return the new {@link StoreFindToken} after validating
   */
  StoreFindToken resetTokenIfRequired(StoreFindToken storeToken) throws StoreException {
    UUID remoteIncarnationId = storeToken.getIncarnationId();
    // if incarnationId is null, for backwards compatibility purposes, the token is considered as good.
    /// if not null, we check for a match
    if (!storeToken.getType().equals(FindTokenType.Uninitialized) && remoteIncarnationId != null
        && !remoteIncarnationId.equals(incarnationId)) {
      // incarnationId mismatch, hence resetting the token to beginning
      logger.info("Index : {} resetting offset after incarnation, new incarnation Id {}, "
          + "incarnationId from store token {}", dataDir, incarnationId, remoteIncarnationId);
      storeToken = new StoreFindToken();
    } else if (storeToken.getSessionId() == null || storeToken.getSessionId().compareTo(sessionId) != 0) {
      // the session has changed. check if we had an unclean shutdown on startup
      if (!cleanShutdown) {
        // if we had an unclean shutdown and the token offset is larger than the logEndOffsetOnStartup
        // we reset the token to logEndOffsetOnStartup
        if (!storeToken.getType().equals(FindTokenType.Uninitialized)
            && storeToken.getOffset().compareTo(logEndOffsetOnStartup) > 0) {
          logger.info("Index : {} resetting offset after not clean shutdown {} before offset {}", dataDir,
              logEndOffsetOnStartup, storeToken.getOffset());
          IndexSegment segmentOfToken = validIndexSegments.floorEntry(logEndOffsetOnStartup).getValue();
          storeToken =
              new StoreFindToken(logEndOffsetOnStartup, sessionId, incarnationId, true, segmentOfToken.getResetKey(),
                  segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
        }
      } else if (!storeToken.getType().equals(FindTokenType.Uninitialized)
          && storeToken.getOffset().compareTo(logEndOffsetOnStartup) > 0) {
        logger.error("Index : {} invalid token. Provided offset is outside the log range after clean shutdown",
            dataDir);
        // if the shutdown was clean, the offset should always be lesser or equal to the logEndOffsetOnStartup
        throw new IllegalArgumentException(
            "Invalid token. Provided offset is outside the log range after clean shutdown");
      }
    }
    return storeToken;
  }

  /**
   * Gets the total number of bytes from the beginning of the log to the position of {@code token}. This includes any
   * overhead due to headers and empty space.
   * @param token the point until which the log has been read.
   * @param messageEntries the list of {@link MessageInfo} that were read when producing {@code token}.
   * @param logEndOffsetBeforeFind the end offset of the log before a find was attempted.
   * @param indexSegments the list of index segments to use.
   * @return the total number of bytes read from the log at the position of {@code token}.
   */
  private long getTotalBytesRead(StoreFindToken token, List<MessageInfo> messageEntries, Offset logEndOffsetBeforeFind,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    long bytesRead = 0;
    if (token.getType().equals(FindTokenType.IndexBased)) {
      try {
        if (token.getOffset().equals(validIndexSegments.lastKey())) {
          bytesRead = getAbsoluteReadBytesFromIndexBasedToken(token, indexSegments);
        } else {
          bytesRead = getAbsolutePositionInLogForOffset(token.getOffset(), indexSegments);
        }
      } catch (StoreException e) {
        //If get read bytes from storeKey fails, use the start offset in index based token.
        logger.error(
            "Store exception on getAbsoluteReadBytesFromIndexBasedToken request with error code {} in datadir {} ",
            e.getErrorCode(), dataDir);
        bytesRead = getAbsolutePositionInLogForOffset(token.getOffset(), indexSegments);
      }
    } else if (token.getType().equals(FindTokenType.JournalBased)) {
      if (messageEntries.size() > 0) {
        bytesRead = getAbsolutePositionInLogForOffset(token.getOffset(), indexSegments) + messageEntries.get(
            messageEntries.size() - 1).getSize();
      } else {
        bytesRead = getAbsolutePositionInLogForOffset(logEndOffsetBeforeFind, indexSegments);
      }
    }
    return bytesRead;
  }

  /**
   * @return the currently capacity used of the log abstraction. Includes "wasted" space at the end of
   * {@link LogSegment} instances that are not fully filled.
   */
  long getLogUsedCapacity() {
    ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
    return getAbsolutePositionInLogForOffset(getCurrentEndOffset(indexSegments), indexSegments);
  }

  /**
   * @return The direct memory usage for this persistent index in bytes.
   */
  long getDirectMemoryUsage() {
    return validIndexSegments.values().stream().mapToLong(IndexSegment::getDirectMemoryUsage).sum();
  }

  /**
   * @return absolute end position (in bytes) of latest PUT record when this method is invoked. If no PUT is found in
   *         current store, -1 will be returned.
   * @throws StoreException
   */
  long getAbsoluteEndPositionOfLastPut() throws StoreException {
    IndexValue indexValueOfLastPut = null;
    // 1. go through keys in journal in reverse order to see if there is a PUT index entry associated with the key
    List<JournalEntry> journalEntries = journal.getAllEntries();
    ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
    // check entry in reverse order (from the most recent one) to find last PUT record in index
    for (int i = journalEntries.size() - 1; i >= 0; --i) {
      JournalEntry entry = journalEntries.get(i);
      indexValueOfLastPut = findKey(entry.getKey(), new FileSpan(entry.getOffset(), getCurrentEndOffset(indexSegments)),
          EnumSet.of(IndexEntryType.PUT), indexSegments);
      if (indexValueOfLastPut != null) {
        break;
      }
    }
    if (!journalEntries.isEmpty() && indexValueOfLastPut == null) {
      // 2. if not find in the journal, check index segments starting from most recent one (until latest PUT is found)
      JournalEntry firstJournalEntry = journalEntries.get(0);
      // generate a segment map in reverse order
      ConcurrentNavigableMap<Offset, IndexSegment> segmentsMapToSearch =
          indexSegments.subMap(indexSegments.firstKey(), true, indexSegments.lowerKey(firstJournalEntry.getOffset()),
              true).descendingMap();
      for (Map.Entry<Offset, IndexSegment> entry : segmentsMapToSearch.entrySet()) {
        indexValueOfLastPut = entry.getValue().getIndexValueOfLastPut();
        if (indexValueOfLastPut != null) {
          break;
        }
      }
    }
    if (indexValueOfLastPut == null) {
      // if no PUT record is found in this store, return -1. This is possible when current store is a brand new store.
      logger.info("No PUT record is found for store {}", dataDir);
      return -1;
    }
    return getAbsolutePositionInLogForOffset(indexValueOfLastPut.getOffset(), indexSegments)
        + indexValueOfLastPut.getSize();
  }

  /**
   * Get a random PUT entry and its {@link BlobReadOptions}. Return null if there is no PUT entry found
   * in this index.
   * @return
   */
  BlobReadOptions getRandomPutEntryReadOptions() {
    rwLock.readLock().lock();
    try {
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
      if (indexSegments.isEmpty()) {
        return null;
      }
      long start = System.currentTimeMillis();
      ArrayList<Offset> indexSegmentOffsets = new ArrayList<>(indexSegments.keySet());
      while (!indexSegmentOffsets.isEmpty()) {
        int idx = ThreadLocalRandom.current().nextInt(indexSegmentOffsets.size());
        Offset offset = indexSegmentOffsets.get(idx);
        IndexSegment segment = indexSegments.get(offset);
        if (segment != null) {
          try {
            IndexEntry indexEntry = segment.getFirstPutEntry();
            if (indexEntry != null) {
              metrics.getRandomPutEntryReadOptionsInMs.update(System.currentTimeMillis() - start);
              return fromIndexValue(indexEntry.getKey(), indexEntry.getValue());
            } else {
              logger.trace("Store {}, offset {} index segment doesn't have any put entry, move on", dataDir, offset);
            }
          } catch (Exception e) {
            logger.error("Store {}, offset {} failed to get the first put entry, move on", dataDir, offset, e);
          }
        } else {
          logger.trace("Store {}, offset {} doesn't exist when getting a random put entry", dataDir, offset);
        }
        // Remove the offset at the idx by swapping it with last element and pop the last element
        indexSegmentOffsets.set(idx, indexSegmentOffsets.get(indexSegmentOffsets.size() - 1));
        indexSegmentOffsets.remove(indexSegmentOffsets.size() - 1);
      }
      return null;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * @return the number of valid log segments starting from the first segment.
   */
  long getLogSegmentCount() {
    return getLogSegmentToIndexSegmentMapping(validIndexSegments).size();
  }

  @Override
  public long getLogSegmentSize(LogSegmentName logSegmentName) {
    LogSegment segment = log.getSegment(logSegmentName);
    if (segment == null) {
      throw new IllegalArgumentException("LogSegment " + logSegmentName + " doesn't exist");
    }
    return segment.getEndOffset() - segment.getStartOffset();
  }

  /**
   * @return the absolute position represented by {@code offset} in the {@link Log}.
   */
  long getAbsolutePositionInLogForOffset(Offset offset) {
    return getAbsolutePositionInLogForOffset(offset, validIndexSegments);
  }

  /**
   * @return the partial log segment count
   */
  int getPartialLogSegmentCount() {
    return partialLogSegmentCount.get();
  }

  /**
   * @return the wasted log segment space
   */
  long getWastedLogSegmentSpace() {
    return wastedLogSegmentSpace.get();
  }

  /**
   * Gets the absolute position represented by {@code offset} in the {@link Log}.
   * @param offset the {@link Offset} whose absolute position is required.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return he absolute position represented by {@code offset} in the {@link Log}.
   */
  private long getAbsolutePositionInLogForOffset(Offset offset,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    LogSegment logSegment = log.getSegment(offset.getName());
    if (logSegment == null || offset.getOffset() > logSegment.getEndOffset()) {
      throw new IllegalArgumentException("Offset is invalid: " + offset + "; LogSegment: " + logSegment);
    }
    int numPrecedingLogSegments = getLogSegmentToIndexSegmentMapping(indexSegments).headMap(offset.getName()).size();
    return numPrecedingLogSegments * log.getSegmentCapacity() + offset.getOffset();
  }

  /**
   * @return the absolute read bytes represented by token.
   * @throws StoreException
   */
  long getAbsoluteReadBytesFromIndexBasedToken(StoreFindToken indexBasedToken) throws StoreException {
    return getAbsoluteReadBytesFromIndexBasedToken(indexBasedToken, validIndexSegments);
  }

  /**
   * Get the absolute read bytes from index based token {@link StoreKey}.
   * @param indexBasedToken the point until which the log has been read.
   * @param indexSegments the list of index segments to use.
   * @return the absolute read bytes represented by token.
   * @throws StoreException
   */
  private long getAbsoluteReadBytesFromIndexBasedToken(StoreFindToken indexBasedToken,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    Offset offset = indexBasedToken.getOffset();
    LogSegment logSegment = log.getSegment(offset.getName());
    if (logSegment == null || offset.getOffset() > logSegment.getEndOffset()) {
      throw new IllegalArgumentException("Offset is invalid: " + offset + "; LogSegment: " + logSegment);
    }
    int numPrecedingLogSegments = getLogSegmentToIndexSegmentMapping(indexSegments).headMap(offset.getName()).size();
    return numPrecedingLogSegments * log.getSegmentCapacity() + logSegment.getEndOffset()
        - getTotalOffsetAfterTokenPointedStoreKey(indexBasedToken);
  }

  /**
   * Get the absolute read bytes after index based token {@link StoreKey}.
   * @param indexBasedToken the point until which the log has been read.
   * @return the absolute read bytes after index based token {@link StoreKey}.
   * @throws StoreException
   */
  private long getTotalOffsetAfterTokenPointedStoreKey(StoreFindToken indexBasedToken) throws StoreException {
    StoreKey storeKey = indexBasedToken.getStoreKey();
    List<MessageInfo> entries = new ArrayList<>();
    IndexSegment indexSegment = validIndexSegments.get(indexBasedToken.getOffset());
    indexSegment.getEntriesSince(storeKey, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0), false);
    long totalSizeNotReplicated = 0;
    for (MessageInfo messageInfo : entries) {
      totalSizeNotReplicated += messageInfo.getSize();
    }
    return totalSizeNotReplicated;
  }

  /**
   * Fetches {@link LogSegment} names whose entries don't over lap with {@link Journal}. Returns {@code null}
   * if there aren't any
   * @return a {@link List<String>} of {@link LogSegment} names whose entries don't overlap with {@link Journal}.
   * {@code null} if there aren't any
   */
  List<LogSegmentName> getLogSegmentsNotInJournal() {
    LogSegment logSegment = log.getFirstSegment();
    List<LogSegmentName> logSegmentNamesToReturn = new ArrayList<>();
    while (log.getNextSegment(logSegment) != null && validIndexSegments.size() > 0) {
      logSegmentNamesToReturn.add(logSegment.getName());
      logSegment = log.getNextSegment(logSegment);
    }
    if (journal.getCurrentNumberOfEntries() != 0) {
      Offset firstOffsetInJournal = journal.getFirstOffset();
      int index = logSegmentNamesToReturn.indexOf(firstOffsetInJournal.getName());
      if (index != -1) {
        logSegmentNamesToReturn = logSegmentNamesToReturn.subList(0, index);
      }
    }
    //if logSegmentNamesToReturn size equals 0, should return null to avoid through IllegalArgumentException in CompactionDetails.
    return logSegmentNamesToReturn.size() > 0 ? logSegmentNamesToReturn : null;
  }

  /**
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return mapping from log segment names to {@link IndexSegment} instances that refer to them.
   */
  private TreeMap<LogSegmentName, List<IndexSegment>> getLogSegmentToIndexSegmentMapping(
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    TreeMap<LogSegmentName, List<IndexSegment>> mapping = new TreeMap<>();
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : indexSegments.entrySet()) {
      IndexSegment indexSegment = indexSegmentEntry.getValue();
      LogSegmentName logSegmentName = indexSegment.getLogSegmentName();
      if (!mapping.containsKey(logSegmentName)) {
        mapping.put(logSegmentName, new ArrayList<IndexSegment>());
      }
      mapping.get(logSegmentName).add(indexSegment);
    }
    return mapping;
  }

  /**
   * Finds entries starting from a key from the segment with start offset initialSegmentStartOffset. The key represents
   * the position in the segment starting from where entries needs to be fetched.
   * @param initialSegmentStartOffset The segment start offset of the segment to start reading entries from.
   * @param key The key representing the position in the segment to start reading entries from (whether to read this key
   *           as well depends on inclusive variable). If the key is null, all the keys will be read.
   * @param messageEntries the list to be populated with the MessageInfo for every entry that is read.
   * @param findEntriesCondition that determines whether to fetch more entries based on a maximum total size of entries
   *                             that needs to be returned and a time that determines the latest segment to be scanned.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @param inclusive whether to start reading entries from the key, {@code true} inclusive, {@code false} exclusive.
   * @return A token representing the position in the segment/journal up to which entries have been read and returned.
   */
  private StoreFindToken findEntriesFromSegmentStartOffset(Offset initialSegmentStartOffset, StoreKey key,
      List<MessageInfo> messageEntries, FindEntriesCondition findEntriesCondition,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments, boolean inclusive) throws StoreException {
    Offset segmentStartOffset = initialSegmentStartOffset;
    //Always checking journal before indexSegments to avoid race condition since when we put new entry, the index segment
    //will be updated first.
    if (!isJournalEmptyAfterAutoClose() && segmentStartOffset.equals(indexSegments.lastKey())) {
      //We would never have given away a token with a segmentStartOffset of the latest segment unless the journal has
      //been cleaned up by (auto close last log segment).
      throw new IllegalArgumentException(
          "Index : " + dataDir + " findEntriesFromOffset segment start offset " + segmentStartOffset
              + " is of the last segment");
    }

    Offset newTokenSegmentStartOffset = null;
    Offset newTokenOffsetInJournal = null;

    IndexSegment segmentToProcess = indexSegments.get(segmentStartOffset);
    AtomicLong currentTotalSizeOfEntries = new AtomicLong(0);

    /* First, get keys from the segment corresponding to the passed in segment start offset if the token has a non-null
       key. Otherwise, since all the keys starting from the offset have to be read, skip this and check in the journal
       first. */
    if (key != null) {
      if (segmentToProcess.getEntriesSince(key, findEntriesCondition, messageEntries, currentTotalSizeOfEntries,
          inclusive)) {
        // if we did fetch entries from this segment, set the new token info accordingly.
        newTokenSegmentStartOffset = segmentStartOffset;
      }
      logger.trace("Index : {} findEntriesFromOffset segment start offset {} with key {} total entries received {}",
          dataDir, segmentStartOffset, key, messageEntries.size());
      // Notice that we might not finish scanning the segmentToProcess, it's possible that current index segment contains
      // enough index values so that the findEntriesCondition returns false. But we are still moving to the next index segment
      // because when findEntriesCondition returns false, we will skip the while loop below and return the new find token
      // right away. And if findEntriesCondition returns true, that means we finish scanning of the current index segment so
      // we can move to the next one. If key is not null and the segmentStartOffset equals to the last index segment's start offset,
      // segmentToProcess will be set to null since the index based token has pointed to the last index segment and at this moment journal
      // should be null so we will skip the while loop below and return the new index based token right away.
      segmentStartOffset = indexSegments.higherKey(segmentStartOffset);
      segmentToProcess = segmentStartOffset == null ? null : indexSegments.get(segmentStartOffset);
    }
    while (segmentToProcess != null && findEntriesCondition.proceed(currentTotalSizeOfEntries.get(),
        segmentToProcess.getLastModifiedTimeSecs())) {
      // Check in the journal to see if we are already at an offset in the journal, if so get entries from it.
      Offset journalFirstOffsetBeforeCheck = journal.getFirstOffset();
      Offset journalLastOffsetBeforeCheck = journal.getLastOffset();
      // If segmentStartOffset is null, then the journal has to be empty and in journal.getEntriesSince method the
      // first entry in journal will be null and null will be returned right away.
      List<JournalEntry> entries = journal.getEntriesSince(segmentStartOffset, true);
      Offset endOffsetOfSnapshot = getCurrentEndOffset(indexSegments);
      if (entries != null) {
        logger.trace("Index : {} findEntriesFromOffset journal offset {} total entries received {}", dataDir,
            segmentStartOffset, entries.size());
        IndexSegment currentSegment = segmentToProcess;
        Map<StoreKey, MessageInfo> messageInfoCache = new HashMap<>();
        for (JournalEntry entry : entries) {
          if (entry.getOffset().compareTo(currentSegment.getEndOffset()) > 0) {
            Offset nextSegmentStartOffset = indexSegments.higherKey(currentSegment.getStartOffset());
            // since we were given a snapshot of indexSegments, it is not guaranteed that the snapshot contains all of
            // the entries obtained from journal.getEntriesSince() that was performed after the snapshot was obtained.
            // Therefore, it is possible that some of the entries obtained from the journal don't exist in the snapshot
            // Such entries can be detected when a rollover is required and we have no more index segments left to roll
            // over to.
            if (nextSegmentStartOffset == null) {
              // stop if there are no more index segments in this ref.
              break;
            }
            currentSegment = indexSegments.get(nextSegmentStartOffset);
            // stop if ineligible because of last modified time
            if (!findEntriesCondition.proceed(currentTotalSizeOfEntries.get(),
                currentSegment.getLastModifiedTimeSecs())) {
              break;
            }
          }
          newTokenOffsetInJournal = entry.getOffset();
          addMessageInfoForJournalEntry(entry, endOffsetOfSnapshot, indexSegments, messageInfoCache, messageEntries,
              currentTotalSizeOfEntries);
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntries.get(),
              currentSegment.getLastModifiedTimeSecs())) {
            break;
          }
        }
        break; // we have entered and finished reading from the journal, so we are done.
      }

      //If journal is empty due to auto close last log segment, there's no need to check this.
      //Always checking journal before indexSegments to avoid race condition since when we put new entry, the index segment
      //will be updated first.
      if (!isJournalEmptyAfterAutoClose() && segmentStartOffset.equals(validIndexSegments.lastKey())) {
        /* The start offset is of the latest segment, and was not found in the journal. This means an entry was added
         * to the index (creating a new segment) but not yet to the journal. However, if the journal does not contain
         * the latest segment's start offset, then it *must* have the previous segment's start offset (if it did not,
         * then the previous segment must have been the latest segment at the time it was scanned, which couldn't have
         * happened due to this same argument).
         * */
        throw new IllegalStateException(
            "Index : " + dataDir + " findEntriesFromOffset segment start offset " + segmentStartOffset
                + " is of the latest segment and not found in journal with range [" + journalFirstOffsetBeforeCheck
                + ", " + journalLastOffsetBeforeCheck + "]");
      } else {
        // Read and populate from the first key in the segment with this segmentStartOffset
        if (segmentToProcess.getEntriesSince(null, findEntriesCondition, messageEntries, currentTotalSizeOfEntries,
            false)) {
          newTokenSegmentStartOffset = segmentStartOffset;
        }
        logger.trace(
            "Index : {} findEntriesFromOffset segment start offset {} with all the keys, total entries received {}",
            dataDir, segmentStartOffset, messageEntries.size());
        segmentStartOffset = indexSegments.higherKey(segmentStartOffset);
        if (segmentStartOffset == null) {
          // we have reached the last index segment in the index ref we have. The journal has moved past this final
          // segment and we have nothing to do except return what we have. Our view is still consistent and "up-to-date"
          // w.r.t the time the call was made.
          break;
        }
        segmentToProcess = indexSegments.get(segmentStartOffset);
      }
    }
    if (newTokenOffsetInJournal != null) {
      IndexSegment segmentOfToken = indexSegments.floorEntry(newTokenOffsetInJournal).getValue();
      return new StoreFindToken(newTokenOffsetInJournal, sessionId, incarnationId, false, segmentOfToken.getResetKey(),
          segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    } else if (messageEntries.size() == 0 && !findEntriesCondition.hasEndTime()) {
      // If the condition does not have an end time, then since we have entered a segment, we should return at least one
      // message unless the new log segment only contains headers.
      throw new IllegalStateException(
          "Message entries cannot be null. At least one entry should have been returned, start offset: "
              + initialSegmentStartOffset + ", key: " + key + ", findEntriesCondition: " + findEntriesCondition);
    } else {
      if (newTokenSegmentStartOffset == null) {
        return new StoreFindToken();
      }
      // if newTokenSegmentStartOffset is set, then we did fetch entries from that segment, otherwise return an
      // uninitialized token
      IndexSegment segmentOfToken = indexSegments.get(newTokenSegmentStartOffset);
      return new StoreFindToken(messageEntries.get(messageEntries.size() - 1).getStoreKey(), newTokenSegmentStartOffset,
          sessionId, incarnationId, segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(),
          segmentOfToken.getResetKeyLifeVersion());
    }
  }

  /**
   * Check if the index based token has pointed to the last entry of the last index segment.
   * @param token the {@link StoreFindToken} need to be checked.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances.
   * @return {@code true} if the token points to the last entry of last index segment. Otherwise return {@code false}.
   */
  private boolean indexBasedTokenPointsToLastEntryOfLastIndexSegment(StoreFindToken token,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    StoreKey storeKey = token.getStoreKey();
    IndexSegment lastIndexSegment = indexSegments.lastEntry().getValue();
    if (token.getOffset().equals(lastIndexSegment.getStartOffset())) {
      StoreKey lastEntryInLastIndexSegment = lastIndexSegment.listIterator(lastIndexSegment.size()).previous().getKey();
      logger.trace("DataDir {}: StoreKey for token {} is {}, StoreKey in last index segment is {}", dataDir, token,
          storeKey, lastEntryInLastIndexSegment);
      return lastEntryInLastIndexSegment.equals(storeKey);
    }
    return false;
  }

  /**
   * The method checks three conditions: 1. journal is empty, 2. log segment is empty, 3. it's not the first log segment.
   * @return {@link true} if last log segment only contains the header info after auto closing last log segment during compaction.
   */
  private boolean isJournalEmptyAfterAutoClose() {
    logger.trace("DataDir: {} journal size is : {}", dataDir, journal.getCurrentNumberOfEntries());
    logger.trace("DataDir: {} last log segment start offset: {}, end offset: {}", dataDir,
        log.getLastSegment().getStartOffset(), log.getLastSegment().getEndOffset());
    return journal.getCurrentNumberOfEntries() == 0 && log.getLastSegment().isEmpty() && !log.getLastSegment()
        .getName()
        .equals(log.getFirstSegment().getName());
  }

  /**
   * Helper method for getting a {@link MessageInfo} and an estimate of fetch size corresponding to the provided
   * {@link JournalEntry} and adding it to the list of messages. This size added to currentTotalSizeOfEntries may
   * include the size of the put record if the put record is at or past the offset in the journal entry and the latest
   * value in the index is a ttlUpdate or undelete. In this case, we can make a safe guess that the original blob also
   * has to be fetched by a replicator anyways and should be counted towards the size limit.
   * @param entry the {@link JournalEntry} to look up in the index.
   * @param endOffsetOfSearchRange the end of the range to search in the index.
   * @param indexSegments the index snapshot to search.
   * @param messageInfoCache a cache to store previous find results for a key. Queries from an earlier offset will
   *                         return the same latest value, so there is no need to search the index a second time for
   *                         keys with multiple journal entries.
   * @param messageEntries the list of {@link MessageInfo} being constructed. This list will be added to.
   * @param currentTotalSizeOfEntries a counter for the size in bytes of the entries returned in the query.
   * @throws StoreException on index search errors.
   */
  private void addMessageInfoForJournalEntry(JournalEntry entry, Offset endOffsetOfSearchRange,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments, Map<StoreKey, MessageInfo> messageInfoCache,
      List<MessageInfo> messageEntries, AtomicLong currentTotalSizeOfEntries) throws StoreException {
    MessageInfo messageInfo = messageInfoCache.get(entry.getKey());
    if (messageInfo == null) {
      // we only need to do an index lookup once per key since the following method will honor any index
      // values for the key past entry.getOffset()
      List<IndexValue> valuesInRange =
          findAllIndexValuesForKey(entry.getKey(), new FileSpan(entry.getOffset(), endOffsetOfSearchRange),
              EnumSet.allOf(IndexEntryType.class), indexSegments);

      IndexValue latestValue = valuesInRange.get(0);
      if (valuesInRange.size() > 1 && (latestValue.isTtlUpdate() || latestValue.isUndelete())) {
        IndexValue earliestValue = valuesInRange.get(valuesInRange.size() - 1);
        if (earliestValue.isPut() && earliestValue.getOffset().compareTo(entry.getOffset()) >= 0) {
          currentTotalSizeOfEntries.addAndGet(earliestValue.getSize());
        }
      }
      messageInfo =
          new MessageInfo(entry.getKey(), latestValue.getSize(), latestValue.isDelete(), latestValue.isTtlUpdate(),
              latestValue.isUndelete(), latestValue.getExpiresAtMs(), null, latestValue.getAccountId(),
              latestValue.getContainerId(), latestValue.getOperationTimeInMs(), latestValue.getLifeVersion());
      messageInfoCache.put(entry.getKey(), messageInfo);
    }
    // We may add duplicate MessageInfos to the list here since the ordering of the list is used to calculate
    // bytes read for the token. The duplicates will be cleaned up by eliminateDuplicates.
    currentTotalSizeOfEntries.addAndGet(messageInfo.getSize());
    messageEntries.add(messageInfo);
  }

  /**
   * We can have duplicate entries in the message entries since updates can happen to the same key. For example,
   * insert a key followed by a delete. This would create two entries in the journal or the index. A single findInfo
   * could read both the entries. The findInfo should return as clean information as possible.
   * <p/>
   * This function choose a delete entry over all entries but chooses a put entry over a ttl update entry
   * @param messageEntries The message entry list where duplicates need to be removed
   */
  private void eliminateDuplicates(List<MessageInfo> messageEntries) {
    Set<StoreKey> setToFindDuplicate = new HashSet<>();
    // first remove the ttl update MessageInfo of any key whose put MessageInfo is already present
    messageEntries.removeIf(
        messageInfo -> !messageInfo.isDeleted() && !messageInfo.isUndeleted() && !setToFindDuplicate.add(
            messageInfo.getStoreKey()));
    // next, remove any put/ttl update if a delete is present
    setToFindDuplicate.clear();
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator(messageEntries.size());
    while (messageEntriesIterator.hasPrevious()) {
      MessageInfo messageInfo = messageEntriesIterator.previous();
      if (!setToFindDuplicate.add(messageInfo.getStoreKey())) {
        messageEntriesIterator.remove();
      }
    }
  }

  /**
   * Updates the messages with their updated state (ttl update/delete/undelete). This method can be used when
   * the messages have been retrieved from an old index segment and needs to be updated with the state from the new
   * index segment
   * @param messageEntries The message entries that may need to be updated.
   */
  private void updateStateForMessages(List<MessageInfo> messageEntries) throws StoreException {
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator();
    while (messageEntriesIterator.hasNext()) {
      MessageInfo messageInfo = messageEntriesIterator.next();
      // If the message is already deleted and DELETE is the final state of the blob, then we don't have
      // to update the state for this message.
      if (isDeleteFinalStateOfBlob && messageInfo.isDeleted()) {
        continue;
      }
      IndexValue indexValue = findKey(messageInfo.getStoreKey(), null,
          EnumSet.of(IndexEntryType.TTL_UPDATE, IndexEntryType.DELETE, IndexEntryType.UNDELETE));
      if (indexValue != null) {
        messageInfo = new MessageInfo(messageInfo.getStoreKey(), indexValue.getSize(), indexValue.isDelete(),
            indexValue.isTtlUpdate(), indexValue.isUndelete(), indexValue.getExpiresAtMs(), null,
            indexValue.getAccountId(), indexValue.getContainerId(), indexValue.getOperationTimeInMs(),
            indexValue.getLifeVersion());
        messageEntriesIterator.set(messageInfo);
      }
    }
  }

  /**
   * Filter out the put entries and only get the delete entries.
   * @param messageEntries The message entry list from which only delete entries have to be returned.
   */
  private void filterDeleteEntries(List<MessageInfo> messageEntries) {
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator();
    /* Note: Ideally we should also filter out duplicate delete entries here. However, due to past bugs,
       there could be multiple delete entries in the index for the same blob. At the cost of possibly
       duplicating the effort for those rare cases, we ensure as much deletes are covered as possible.
     */
    while (messageEntriesIterator.hasNext()) {
      if (!messageEntriesIterator.next().isDeleted()) {
        messageEntriesIterator.remove();
      }
    }
  }

  /**
   * Closes the index
   * @param skipDiskFlush whether to skip any disk flush operations.
   * @throws StoreException
   */
  void close(boolean skipDiskFlush) throws StoreException {
    long startTimeInMs = time.milliseconds();
    try {
      if (persistorTask != null) {
        persistorTask.cancel(false);
      }
      if (!skipDiskFlush) {
        persistor.write();
        if (hardDeleter != null) {
          try {
            hardDeleter.shutdown();
          } catch (Exception e) {
            logger.error("Index : {} error while persisting cleanup token", dataDir, e);
          }
        }
        try {
          cleanShutdownFile.createNewFile();
          if (config.storeSetFilePermissionEnabled) {
            Files.setPosixFilePermissions(cleanShutdownFile.toPath(), config.storeOperationFilePermission);
          }
        } catch (IOException e) {
          logger.error("Index : {} error while creating clean shutdown file", dataDir, e);
        }
      }
    } finally {
      metrics.indexShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
    }
  }

  /**
   * @return the start offset of the index.
   */
  Offset getStartOffset() {
    return getStartOffset(validIndexSegments);
  }

  /**
   * Gets the start {@link Offset} of the given instance of  {@code indexSegments}.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   */
  private Offset getStartOffset(ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    Offset startOffset;
    if (indexSegments.size() == 0) {
      LogSegment firstLogSegment = log.getFirstSegment();
      startOffset = new Offset(firstLogSegment.getName(), firstLogSegment.getStartOffset());
    } else {
      startOffset = indexSegments.firstKey();
    }
    return startOffset;
  }

  /**
   * Returns the current end offset that the index represents in the log
   * @return The end offset in the log that this index currently represents
   */
  Offset getCurrentEndOffset() {
    return getCurrentEndOffset(validIndexSegments);
  }

  /**
   * Gets the end {@link Offset} of the given instance of {@code indexSegments}.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   */
  private Offset getCurrentEndOffset(ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    // If no indexSegments exist, return start offset of first log segment
    return indexSegments.size() == 0 ? getStartOffset(indexSegments)
        : indexSegments.lastEntry().getValue().getEndOffset();
  }

  /**
   * @return true if the index contains no segments, otherwise false.
   */
  boolean isEmpty() {
    return validIndexSegments.isEmpty();
  }

  /**
   * Expose this information to replication manager to reset the replication token for this partition since we
   * recovered from partial log segment. When we persist replication tokens, we intentionally make delays so we don't
   * persist tokens that are not having all the data in log segment persisted, so we shouldn't have to reset the
   * replication token, but this is safe.
   * @return
   */
  boolean hasPartialRecovery() {
    return this.hasPartialRecovery;
  }

  /**
   * Ensures that the provided {@link FileSpan} is greater than the current index end offset and, if required, that it
   * is within a single segment.
   * @param fileSpan The filespan that needs to be verified
   * @param checkWithinSingleSegment if {@code true}, checks whether the end and start offsets in {@code fileSpan} lie
   *                                 in the same log segment.
   */
  private void validateFileSpan(FileSpan fileSpan, boolean checkWithinSingleSegment) {
    if (getCurrentEndOffset().compareTo(fileSpan.getStartOffset()) > 0) {
      throw new IllegalArgumentException(
          "The start offset " + fileSpan.getStartOffset() + " of the FileSpan provided " + " to " + dataDir
              + " is lesser than the current index end offset " + getCurrentEndOffset());
    }
    if (checkWithinSingleSegment && !fileSpan.getStartOffset().getName().equals(fileSpan.getEndOffset().getName())) {
      throw new IllegalArgumentException(
          "The FileSpan provided [" + fileSpan + "] does not lie within a single log " + "segment");
    }
  }

  /**
   * Finds all the deleted entries from the given start token. The token defines the start position in the index from
   * where entries needs to be fetched. All the entries returned from this method have delete as it's final state, which
   * means if we see a Delete entry but this key is undelete later, it will not be returned.
   * @param token The token that signifies the start position in the index from where deleted entries need to be
   *              retrieved
   * @param maxTotalSizeOfEntries The maximum total size of entries that need to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value.
   * @param endTimeSeconds The (approximate) time of the latest entry to be fetched. This is used at segment granularity.
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  FindInfo findDeletedEntriesSince(FindToken token, long maxTotalSizeOfEntries, long endTimeSeconds)
      throws StoreException {
    StoreFindToken storeToken = (StoreFindToken) token;
    StoreFindToken newToken;
    List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();

    if (storeToken.getType().equals(FindTokenType.IndexBased)) {
      // Case 1: index based
      // Find the index segment corresponding to the token indexStartOffset.
      // Get entries starting from the token Key in this index.
      newToken = findEntriesFromSegmentStartOffset(storeToken.getOffset(), storeToken.getStoreKey(), messageEntries,
          new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds), validIndexSegments, false);
      if (newToken.getType().equals(FindTokenType.Uninitialized)) {
        newToken = storeToken;
      }
    } else {
      // journal based or empty
      Offset offsetToStart = storeToken.getOffset();
      boolean inclusive = false;
      if (storeToken.getType().equals(FindTokenType.Uninitialized)) {
        offsetToStart = getStartOffset();
        inclusive = true;
      }
      List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, inclusive);
      // we deliberately obtain a snapshot of the index segments AFTER fetching from the journal. This ensures that
      // any and all entries returned from the journal are guaranteed to be in the obtained snapshot of indexSegments.
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;

      Offset offsetEnd = offsetToStart;
      if (entries != null) {
        // Case 2: offset based, and offset still in journal
        IndexSegment currentSegment = indexSegments.floorEntry(offsetToStart).getValue();
        long currentTotalSizeOfEntries = 0;
        for (JournalEntry entry : entries) {
          if (entry.getOffset().compareTo(currentSegment.getEndOffset()) > 0) {
            Offset nextSegmentStartOffset = indexSegments.higherKey(currentSegment.getStartOffset());
            currentSegment = indexSegments.get(nextSegmentStartOffset);
          }
          if (endTimeSeconds < currentSegment.getLastModifiedTimeSecs()) {
            break;
          }

          IndexValue value =
              findKey(entry.getKey(), new FileSpan(entry.getOffset(), getCurrentEndOffset(indexSegments)),
                  EnumSet.allOf(IndexEntryType.class), indexSegments);
          if (value.isDelete()) {
            messageEntries.add(
                new MessageInfo(entry.getKey(), value.getSize(), true, value.isTtlUpdate(), value.isUndelete(),
                    value.getExpiresAtMs(), null, value.getAccountId(), value.getContainerId(),
                    value.getOperationTimeInMs(), value.getLifeVersion()));
          }
          offsetEnd = entry.getOffset();
          currentTotalSizeOfEntries += value.getSize();
          if (currentTotalSizeOfEntries >= maxTotalSizeOfEntries) {
            break;
          }
        }
        // This method is only used by hard delete. Although reset key temporarily doesn't apply to hard delete, we still feed in reset key info for future use.
        IndexSegment segmentOfToken = indexSegments.floorEntry(offsetEnd).getValue();
        newToken = new StoreFindToken(offsetEnd, sessionId, incarnationId, false, segmentOfToken.getResetKey(),
            segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
      } else {
        // Case 3: offset based, but offset out of journal
        Map.Entry<Offset, IndexSegment> entry = indexSegments.floorEntry(offsetToStart);
        if (entry != null && entry.getKey() != indexSegments.lastKey()) {
          newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
              new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds), indexSegments, false);
          if (newToken.getType().equals(FindTokenType.Uninitialized)) {
            newToken = storeToken;
          }
        } else {
          newToken = storeToken; //use the same offset as before.
        }
      }
    }
    // Filter out all the messages that are not "deleted", then update state for remaining deleted message.
    // First, filter out all the entries that are not delete
    // second, update all the remaining delete entries' state since delete might not be the final state of those entries, they can be undeleted.
    // third, filter out undelete entries after
    // fourth, eliminate duplicate deleted entries. We might have more than one versions of delete entries here.
    filterDeleteEntries(messageEntries);
    updateStateForMessages(messageEntries);
    filterDeleteEntries(messageEntries);
    eliminateDuplicates(messageEntries);
    return new FindInfo(messageEntries, newToken);
  }

  /**
   * Persists index files to disk.
   * @throws StoreException
   */
  void persistIndex() throws StoreException {
    persistor.write();
  }

  /**
   * Re-validates the {@code token} to ensure that it is consistent with the current set of index segments. If it is
   * not, a {@link FindTokenType#Uninitialized} token is returned.
   * @param token the {@link StoreFindToken} to revalidate.
   * @return {@code token} if is consistent with the current set of index segments, a new
   * {@link FindTokenType#Uninitialized} token otherwise.
   */
  FindToken revalidateFindToken(FindToken token) throws StoreException {
    // we temporary disable reset key logic for token used by hard delete (although we can improve this in the future if needed)
    return revalidateStoreFindToken((StoreFindToken) token, validIndexSegments, true);
  }

  /**
   * Re-validates the {@code token} to ensure that it is consistent with the given view of {@code indexSegments}. If it
   * is not, a {@link FindTokenType#Uninitialized} token is returned.
   * @param token the {@link StoreFindToken} to revalidate.
   * @param indexSegments the view of the index segments to revalidate against.
   * @param ignoreResetKey whether to ignore reset key if a new token needs to be built.
   * @return {@code token} if is consistent with {@code indexSegments}, a new {@link FindTokenType#Uninitialized}
   * token otherwise.
   */
  private StoreFindToken revalidateStoreFindToken(StoreFindToken token,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments, boolean ignoreResetKey) throws StoreException {
    StoreFindToken revalidatedToken = token;
    Offset offset = token.getOffset();
    switch (token.getType()) {
      case Uninitialized:
        // nothing to do.
        break;
      case JournalBased:
        // A journal based token. The offset in journal based token is pointing to an IndexValue[IV], this IndexValue belongs
        // to an IndexSegment[IS]. And the offset should always be greater than or equal to IS's start offset. Calling
        // floorKey should return the IS's start offset. Since IV belongs to IS, the IS's start offset should have the
        // same LogSegmentName as the offset's LogSegmentName. But if floorKey is null or these two LogSegmentNames are
        // not the same, it means LogSegment is removed by compaction.
        Offset floorOffset = indexSegments.floorKey(offset);
        if (floorOffset == null || !floorOffset.getName().equals(offset.getName())) {
          if (shouldRebuildTokenBasedOnCompactionHistory && !sanityCheckFailed) {
            revalidatedToken = generateTokenBasedOnIndexSegmentOffset(token, indexSegments);
          } else {
            if (ignoreResetKey || token.getResetKey() == null) {
              revalidatedToken = new StoreFindToken();
            } else {
              // reset key is present in the token, we attempt to generate the new token based on reset key
              revalidatedToken = generateTokenBasedOnResetKey(token, indexSegments);
            }
          }
          metrics.journalBasedTokenResetCount.inc();
          logger.info("Index {}: Revalidated token {} because it is invalid for the index segment map", dataDir, token);
        }
        break;
      case IndexBased:
        // An index based token, but the offset is not in the segments, might be caused by the compaction
        if (!indexSegments.containsKey(offset)) {
          if (shouldRebuildTokenBasedOnCompactionHistory && !sanityCheckFailed) {
            revalidatedToken = generateTokenBasedOnIndexSegmentOffset(token, indexSegments);
          } else {
            if (!ignoreResetKey && token.getResetKey() != null) {
              // reset key is present in the token
              revalidatedToken = generateTokenBasedOnResetKey(token, indexSegments);
            } else {
              revalidatedToken = new StoreFindToken();
            }
          }
          metrics.indexBasedTokenResetCount.inc();
          logger.info("Index {}: Revalidated token {} because it is invalid for the index segment map", dataDir, token);
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized token type: " + token.getType());
    }
    return revalidatedToken;
  }

  /**
   * Generate store find token based on reset key within old token.
   * @param token the old {@link StoreFindToken} that contains reset key.
   * @param indexSegments current valid index segments to find.
   * @return new created token.
   */
  private StoreFindToken generateTokenBasedOnResetKey(StoreFindToken token,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    StoreKey resetKey = token.getResetKey();
    IndexEntryType resetKeyType = token.getResetKeyType();
    short resetKeyLifeVersion = token.getResetKeyVersion();
    List<IndexValue> indexValues = findAllIndexValuesForKey(resetKey, null, EnumSet.of(resetKeyType), indexSegments);
    IndexValue resetKeyIndexValue = null;
    if (indexValues != null) {
      for (IndexValue indexValue : indexValues) {
        if (indexValue.getLifeVersion() == resetKeyLifeVersion) {
          resetKeyIndexValue = indexValue;
          break;
        }
      }
    }
    StoreFindToken newToken = new StoreFindToken();
    if (resetKeyIndexValue == null) {
      // reset key is not found in current valid index segments, reset it to very beginning
      logger.info("Index {}: Reset key {} is not found in current index, token is reset to the beginning of whole log",
          dataDir, resetKey);
    } else {
      if (config.storeRebuildTokenBasedOnResetKey) {
        IndexSegment targetIndexSegment = indexSegments.floorEntry(resetKeyIndexValue.getOffset()).getValue();
        // We set inclusive = true in new token to make index segment find all entries it has (including the first one).
        // The new token should always be index-based
        newToken = new StoreFindToken(FindTokenType.IndexBased, targetIndexSegment.getStartOffset(),
            targetIndexSegment.getFirstKeyInSealedSegment(), sessionId, incarnationId, true,
            StoreFindToken.CURRENT_VERSION, targetIndexSegment.getResetKey(), targetIndexSegment.getResetKeyType(),
            targetIndexSegment.getResetKeyLifeVersion());
      }
      metrics.resetKeyFoundInCurrentIndex.inc();
    }
    return newToken;
  }

  /**
   * Generate new {@link StoreFindToken} based on the index segment offset information before and after compaction.
   * If the input token is an index based token, then this method would try to find the offset of after compaction index
   * segment for its offset. It will return the index segment after compaction (referenced by offset) as the StoreFindToken
   * if this method finds it, otherwise, it would return an uninitialized token.
   * If the input token is a journal based token, which means its offset doesn't show up in the offset information before
   * and after compaction. Then first this method will do is to find the offset of the before-compaction index segment
   * this journal token belongs to. If it can find the offset, it will then do the same thing as the index based token.
   * Otherwise it will return an uninitialized token.
   * @param token The token whose offset is now missing from the index segment map.
   * @param indexSegments The current index segment map.
   * @return A new {@link StoreFindToken} that is guaranteed to be safe for replication.
   * @throws StoreException
   */
  private StoreFindToken generateTokenBasedOnIndexSegmentOffset(StoreFindToken token,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    metrics.generateTokenBasedOnCompactionHistoryCount.inc();
    logger.trace("Index {}: Generating token based on index segment offset for token {}", dataDir, token);
    if (token.getType() == FindTokenType.Uninitialized) {
      throw new IllegalArgumentException("Index" + dataDir + ": Invalid token: " + token);
    }
    if (indexSegments.get(token.getOffset()) != null) {
      throw new IllegalArgumentException(
          "Index " + dataDir + ": Token's offset already exist in the index segment map: " + token);
    }
    boolean isIndexSegmentOffset = token.getType().equals(FindTokenType.IndexBased);
    Offset offset = token.getOffset();
    IndexSegment segment;
    while (true) {
      Offset afterOffset;
      if (isIndexSegmentOffset) {
        afterOffset = beforeAndAfterCompactionIndexSegmentOffsets.get(offset);
      } else {
        // This map only contains the index segments' start offsets, it doesn't have offsets from journal based token.
        // Lucky for us, offset in journal based token is the index entry's offset and the index entry's offset should
        // always be greater than or equal to the start offset of the index segment, which this index entry belongs to.
        // And this index segment's start offset should be saved in this map.
        Offset start = new Offset(offset.getName(), 0);
        Offset end = new Offset(offset.getName(), config.storeSegmentSizeInBytes + 1);
        afterOffset = beforeAndAfterCompactionIndexSegmentOffsets.subMap(start, false, end, false).floorKey(offset);
        if (afterOffset == null || !afterOffset.getName().equals(offset.getName())) {
          // LogSegmentName check is for sanity, we already have a start and end offset to limit the range for offset.
          logger.info("Index {}: Non-IndexSegment Offset {} can't find its index segment offset before compaction: {}",
              dataDir, offset, afterOffset);
          return new StoreFindToken();
        }
        isIndexSegmentOffset = true;
      }
      if (afterOffset == null) {
        logger.info(
            "Index {}: Offset {} is not found in compaction index segment offset maps, token is reset to the beginning of whole log",
            dataDir, offset);
        return new StoreFindToken();
      }
      segment = indexSegments.get(afterOffset);
      if (segment != null) {
        logger.trace("Index {}: Offset {} maps to {} after compaction, reset token to this index segment", dataDir,
            offset, afterOffset);
        metrics.offsetFoundInBeforeAfterMapCount.inc();
        return new StoreFindToken(FindTokenType.IndexBased, segment.getStartOffset(),
            segment.getFirstKeyInSealedSegment(), sessionId, incarnationId, true, StoreFindToken.CURRENT_VERSION,
            segment.getResetKey(), segment.getResetKeyType(), segment.getResetKeyLifeVersion());
      } else {
        logger.trace("Index {}: Offset {} maps to {} after compaction, but it doesn't exist in index", dataDir, offset,
            afterOffset);
      }
      offset = afterOffset;
    }
  }

  /**
   * This is only for test. Setting a getBlobReadInfo callback.
   * @param getBlobReadInfoTestCallback The callback to invoke when calling getBlobReadInfo method. It will be invoked
   *                                    right before we convert a PutValue into a BlobReadInfo.
   */
  void setGetBlobReadInfoTestCallback(Runnable getBlobReadInfoTestCallback) {
    this.getBlobReadInfoTestCallback = getBlobReadInfoTestCallback;
  }

  /**
   * Gets the list of {@link IndexSegment} files that refer to the log segment with name {@code logSegmentName}.
   * @param dataDir the directory where the index files are.
   * @param logSegmentName the name of the log segment whose index segment files are required.
   * @return the list of {@link IndexSegment} files that refer to the log segment with name {@code logSegmentName}.
   */
  static File[] getIndexSegmentFilesForLogSegment(String dataDir, final LogSegmentName logSegmentName) {
    return new File(dataDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // If not segmented, log name is log_current
        // index name has the format of <offset>_index, for example 0_index, 265_index
        // In this case, input parameter logSegmentName == ""
        if (logSegmentName.toString().isEmpty()) {
          return name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX);
        } else {
          return name.startsWith(logSegmentName.toString() + BlobStore.SEPARATOR) && name.endsWith(
              IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX);
        }
      }
    });
  }

  /**
   * Gets the list of bloom filter files that refer to the log segment with name {@code logSegmentName}.
   * @param dataDir the directory where the index files are.
   * @param logSegmentName the name of the log segment whose bloom filter files are required.
   * @return the list of bloom filter files that refer to the log segment with name {@code logSegmentName}.
   */
  static File[] getBloomFilterFilesForLogSegment(String dataDir, LogSegmentName logSegmentName) {
   return new File(dataDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (logSegmentName.toString().isEmpty()) {
          return name.endsWith(IndexSegment.BLOOM_FILE_NAME_SUFFIX);
        } else {
          return name.startsWith(logSegmentName + BlobStore.SEPARATOR) &&
              name.endsWith(IndexSegment.BLOOM_FILE_NAME_SUFFIX);
        }
      }
    });
  }

  /**
   * Cleans up all files related to index segments that refer to the log segment with name {@code logSegmentName}.
   *  @param dataDir the directory where the index files are.
   * @param logSegmentName the name of the log segment whose index segment related files need to be deleteds.
   * @throws StoreException if {@code dataDir} could not be read or if a file could not be deleted.
   */
  static void cleanupIndexSegmentFilesForLogSegment(String dataDir, final LogSegmentName logSegmentName)
      throws StoreException {
    File[] filesToCleanup = new File(dataDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (logSegmentName.toString().isEmpty()) {
          return (name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX) || name.endsWith(
              IndexSegment.BLOOM_FILE_NAME_SUFFIX));
        } else {
          return name.startsWith(logSegmentName.toString() + BlobStore.SEPARATOR) && (
              name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX) || name.endsWith(
                  IndexSegment.BLOOM_FILE_NAME_SUFFIX));
        }
      }
    });
    if (filesToCleanup == null) {
      throw new StoreException("Failed to list index segment files", StoreErrorCodes.IOError);
    }
    for (File file : filesToCleanup) {
      if (!file.delete()) {
        throw new StoreException("Could not delete file named " + file, StoreErrorCodes.UnknownError);
      }
    }
  }

  class IndexPersistor implements Runnable {

    /**
     * Writes all the individual index segments to disk. It flushes the log before starting the
     * index flush. The penultimate index segment is flushed if it is not already flushed and mapped.
     * The last index segment is flushed whenever write is invoked.
     * @throws StoreException
     */
    public synchronized void write() throws StoreException {
      final Timer.Context context = metrics.indexFlushTime.time();
      try {
        ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
        Map.Entry<Offset, IndexSegment> lastEntry = indexSegments.lastEntry();
        if (lastEntry != null) {
          IndexSegment currentInfo = lastEntry.getValue();
          // before iterating the map, get the current file end pointer
          Offset indexEndOffsetBeforeFlush = currentInfo.getEndOffset();
          Offset logEndOffsetBeforeFlush = log.getEndOffset();
          if (logEndOffsetBeforeFlush.compareTo(indexEndOffsetBeforeFlush) < 0) {
            throw new StoreException("LogEndOffset " + logEndOffsetBeforeFlush + " before flush cannot be less than "
                + "currentEndOffSet of index " + indexEndOffsetBeforeFlush, StoreErrorCodes.IllegalIndexState);
          }

          if (hardDeleter != null) {
            hardDeleter.preLogFlush();
            // flush the log to ensure everything till the fileEndPointerBeforeFlush is flushed
            log.flush();
            hardDeleter.postLogFlush();
          } else {
            log.flush();
          }

          Map.Entry<Offset, IndexSegment> prevEntry = indexSegments.lowerEntry(lastEntry.getKey());
          IndexSegment prevInfo = prevEntry != null ? prevEntry.getValue() : null;
          List<IndexSegment> prevInfosToWrite = new ArrayList<>();
          Offset currentLogEndPointer = log.getEndOffset();
          while (prevInfo != null && !prevInfo.isSealed()) {
            if (prevInfo.getEndOffset().compareTo(currentLogEndPointer) > 0) {
              String message = "The read only index cannot have a file end pointer " + prevInfo.getEndOffset()
                  + " greater than the log end offset " + currentLogEndPointer;
              throw new StoreException(message, StoreErrorCodes.IllegalIndexState);
            }
            prevInfosToWrite.add(prevInfo);
            Map.Entry<Offset, IndexSegment> infoEntry = indexSegments.lowerEntry(prevInfo.getStartOffset());
            prevInfo = infoEntry != null ? infoEntry.getValue() : null;
          }
          for (int i = prevInfosToWrite.size() - 1; i >= 0; i--) {
            IndexSegment toWrite = prevInfosToWrite.get(i);
            logger.trace("Index : {} writing prev index with end offset {}", dataDir, toWrite.getEndOffset());
            toWrite.writeIndexSegmentToFile(toWrite.getEndOffset());
            toWrite.seal();
          }
          // Compaction may seal the current last index segment.
          currentInfo.writeIndexSegmentToFile(indexEndOffsetBeforeFlush);
        }
      } catch (FileNotFoundException e) {
        throw new StoreException("File not found while writing index to file", e, StoreErrorCodes.FileNotFound);
      } catch (IOException e) {
        StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
        throw new StoreException(errorCode.toString() + " while persisting index to disk", e, errorCode);
      } finally {
        context.stop();
      }
    }

    @Override
    public void run() {
      try {
        write();
      } catch (Exception e) {
        logger.error("Index : {} error while persisting the index to disk", dataDir, e);
      }
    }
  }
}
