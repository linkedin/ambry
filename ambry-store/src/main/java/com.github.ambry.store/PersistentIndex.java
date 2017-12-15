/*
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
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A persistent index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 **/
class PersistentIndex {
  /**
   * Represents the different types of index entries.
   */
  enum IndexEntryType {
    ANY, PUT, DELETE,
  }

  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  static final short CURRENT_VERSION = VERSION_2;
  static final String CLEAN_SHUTDOWN_FILENAME = "cleanshutdown";

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
  static final Comparator<IndexEntry> INDEX_ENTRIES_OFFSET_COMPARATOR = new Comparator<IndexEntry>() {

    @Override
    public int compare(IndexEntry e1, IndexEntry e2) {
      return e1.getValue().getOffset().compareTo(e2.getValue().getOffset());
    }
  };

  final Journal journal;
  final HardDeleter hardDeleter;
  final Thread hardDeleteThread;

  private final Log log;
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
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final IndexPersistor persistor = new IndexPersistor();

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
    /*
    If a put and a delete of a key happens within the same segment, the segment will have only one entry for it,
    whereas the journal keeps both. In order to account for this, and to ensure that the journal always has all the
    elements held by the latest segment, the journal needs to be able to hold twice the max number of elements a
    segment can hold.
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

    List<File> indexFiles = getAllIndexSegmentFiles();
    try {
      for (int i = 0; i < indexFiles.size(); i++) {
        // We map all the index segments except the most recent index segment.
        // The recent index segment would go through recovery after they have been
        // read into memory
        boolean map = i < indexFiles.size() - 1;
        IndexSegment info = new IndexSegment(indexFiles.get(i), map, factory, config, metrics, journal, time);
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
      setEndOffsets();
      log.setActiveSegment(getCurrentEndOffset().getName());
      logEndOffsetOnStartup = log.getEndOffset();

      if (hardDelete != null) {
        // After recovering the last messages, and setting the log end offset, let the hard delete thread do its recovery.
        // NOTE: It is safe to do the hard delete recovery after the regular recovery because we ensure that hard deletes
        // never work on the part of the log that is not yet flushed (by ensuring that the message retention
        // period is longer than the log flush time).
        logger.info("Index : " + datadir + " Starting hard delete recovery");
        hardDeleter = new HardDeleter(config, metrics, datadir, log, this, hardDelete, factory, diskIOScheduler, time);
        hardDeleter.performRecovery();
        logger.info("Index : " + datadir + " Finished performing hard delete recovery");
        metrics.initializeHardDeleteMetric(storeId, hardDeleter, this);
      } else {
        hardDeleter = null;
      }

      if (scheduler != null) {
        // start scheduler thread to persist index in the background
        scheduler.scheduleAtFixedRate(persistor,
            config.storeDataFlushDelaySeconds + new Random().nextInt(Time.SecsPerMin),
            config.storeDataFlushIntervalSeconds, TimeUnit.SECONDS);
      }
      if (hardDelete != null && config.storeEnableHardDelete) {
        logger.info("Index : " + datadir + " Starting hard delete thread ");
        hardDeleteThread = Utils.newThread("hard delete thread " + datadir, hardDeleter, true);
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
          StoreErrorCodes.Index_Creation_Failure);
    }
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
            StoreErrorCodes.Index_Creation_Failure);
      }
      indexFiles.addAll(Arrays.asList(files));
      logSegment = log.getNextSegment(logSegment);
    }
    Collections.sort(indexFiles, INDEX_SEGMENT_FILE_COMPARATOR);
    return indexFiles;
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
    while (logSegmentToRecover != null) {
      long endOffset = logSegmentToRecover.sizeInBytes();
      logger.info("Index : {} performing recovery on index with start offset {} and end offset {}", dataDir,
          recoveryStartOffset, endOffset);
      List<MessageInfo> messagesRecovered =
          recovery.recover(logSegmentToRecover, recoveryStartOffset.getOffset(), endOffset, factory);
      recoveryOccurred = recoveryOccurred || messagesRecovered.size() > 0;
      Offset runningOffset = recoveryStartOffset;
      // Iterate through the recovered messages and update the index
      for (MessageInfo info : messagesRecovered) {
        logger.trace("Index : {} recovering key {} offset {} size {}", dataDir, info.getStoreKey(), runningOffset,
            info.getSize());
        Offset infoEndOffset = new Offset(runningOffset.getName(), runningOffset.getOffset() + info.getSize());
        IndexValue value = findKey(info.getStoreKey());
        if (info.isDeleted()) {
          markAsDeleted(info.getStoreKey(), new FileSpan(runningOffset, infoEndOffset), info,
              info.getOperationTimeMs());
          logger.info("Index : {} updated message with key {} by inserting delete entry of size {} ttl {}", dataDir,
              info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs());
        } else if (value != null) {
          throw new StoreException("Illegal message state during recovery. Duplicate PUT record",
              StoreErrorCodes.Initialization_Error);
        } else {
          // create a new entry in the index
          IndexValue newValue =
              new IndexValue(info.getSize(), runningOffset, info.getExpirationTimeInMs(), info.getOperationTimeMs(),
                  info.getAccountId(), info.getContainerId());
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
    if (recoveryOccurred) {
      metrics.nonzeroMessageRecovery.inc();
    }
    context.stop();
  }

  /**
   * Sets the end offset of all log segments.
   * <p/>
   * Assumed to run only on startup.
   * @throws IOException if an end offset could not be set due to I/O error.
   */
  private void setEndOffsets() throws IOException {
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
   * @return the map of {@link Offset} to {@link IndexSegment} instances.
   */
  ConcurrentSkipListMap<Offset, IndexSegment> getIndexSegments() {
    return validIndexSegments;
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
    Offset journalFirstOffset = journal.getFirstOffset();

    TreeMap<Offset, IndexSegment> segmentsToAdd = new TreeMap<>();
    for (File indexSegmentFile : segmentFilesToAdd) {
      IndexSegment indexSegment = new IndexSegment(indexSegmentFile, true, factory, config, metrics, journal, time);
      if (indexSegment.getEndOffset().compareTo(journalFirstOffset) > 0) {
        throw new IllegalArgumentException("One of the index segments has an end offset " + indexSegment.getEndOffset()
            + " that is higher than the first offset in the journal " + journalFirstOffset);
      }
      segmentsToAdd.put(indexSegment.getStartOffset(), indexSegment);
    }

    for (Offset offset : segmentsToRemove) {
      IndexSegment segmentToRemove = validIndexSegments.get(offset);
      if (segmentToRemove.getEndOffset().compareTo(journalFirstOffset) >= 0) {
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
  }

  /**
   * Adds a new entry to the index
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
   * Checks if the index segment needs to roll over to a new segment
   * @param entry The new entry that needs to be added to the existing active segment
   * @return True, if segment needs to roll over. False, otherwise
   */
  private boolean needToRollOverIndex(IndexEntry entry) {
    Offset offset = entry.getValue().getOffset();
    int thisValueSize = entry.getValue().getBytes().capacity();
    int thisEntrySize = entry.getKey().sizeInBytes() + thisValueSize;
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
    if (lastSegment.getPersistedEntrySize() < thisEntrySize) {
      logger.info(
          "Index: {} Rolling over from {} to {} because the segment persisted entry size: {} is less than the size of this entry: {}",
          dataDir, lastSegment.getStartOffset(), offset, lastSegment.getPersistedEntrySize(), thisEntrySize);
      return true;
    }
    if (lastSegment.getValueSize() != thisValueSize) {
      logger.info(
          "Index: {} Rolling over from {} to {} because the segment value size: {} != current entry value size: {}",
          dataDir, lastSegment.getStartOffset(), offset, lastSegment.getValueSize(), thisValueSize);
      return true;
    }
    if (!offset.getName().equals(lastSegment.getLogSegmentName())) {
      logger.info("Index: {} Rolling over from {} to {} because the log has rolled over from {} to {}", dataDir,
          lastSegment.getStartOffset(), offset, lastSegment.getLogSegmentName(), offset.getName());
      return true;
    }
    return false;
  }

  /**
   * Finds a key in the index and returns the blob index value associated with it. If not found,
   * returns null
   * @param key  The key to find in the index
   * @return The blob index value associated with the key. Null if the key is not found.
   * @throws StoreException
   */
  IndexValue findKey(StoreKey key) throws StoreException {
    return findKey(key, null);
  }

  /**
   * Finds the value associated with a key if it is present in the index within the passed in filespan.
   * Filespan represents the start offset and end offset in the log.
   * @param key The key to do the exist check against
   * @param fileSpan FileSpan which specifies the range within which search should be made
   * @return The associated IndexValue if one exists within the fileSpan, null otherwise.
   * @throws StoreException
   */
  IndexValue findKey(StoreKey key, FileSpan fileSpan) throws StoreException {
    return findKey(key, fileSpan, IndexEntryType.ANY, validIndexSegments);
  }

  /**
   * Finds the {@link IndexValue} of type {@code type} associated with the {@code key} if it is present in the index
   * within the given {@code fileSpan}.
   * @param key the {@link StoreKey} whose {@link IndexValue} is required.
   * @param fileSpan {@link FileSpan} which specifies the range within which search should be made
   * @param type the {@link IndexEntryType} desired.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return The associated {@link IndexValue} of the type {@code type} if it exists within the {@code fileSpan},
   * {@code null} otherwise.
   * @throws StoreException
   */
  private IndexValue findKey(StoreKey key, FileSpan fileSpan, IndexEntryType type,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws StoreException {
    IndexValue retValue = null;
    final Timer.Context context = metrics.findTime.time();
    try {
      ConcurrentNavigableMap<Offset, IndexSegment> segmentsMapToSearch;
      if (fileSpan == null) {
        logger.trace("Searching for " + key + " in the entire index");
        segmentsMapToSearch = indexSegments.descendingMap();
      } else {
        logger.trace(
            "Searching for " + key + " in index with filespan ranging from " + fileSpan.getStartOffset() + " to "
                + fileSpan.getEndOffset());
        segmentsMapToSearch = indexSegments.subMap(indexSegments.floorKey(fileSpan.getStartOffset()), true,
            indexSegments.floorKey(fileSpan.getEndOffset()), true).descendingMap();
        metrics.segmentSizeForExists.update(segmentsMapToSearch.size());
      }
      int segmentsSearched = 0;
      for (Map.Entry<Offset, IndexSegment> entry : segmentsMapToSearch.entrySet()) {
        segmentsSearched++;
        logger.trace("Index : {} searching index with start offset {}", dataDir, entry.getKey());
        IndexValue value = entry.getValue().find(key);
        if (value != null) {
          logger.trace("Index : {} found value offset {} size {} ttl {}", dataDir, value.getOffset(), value.getSize(),
              value.getExpiresAtMs());
          if (type.equals(IndexEntryType.ANY)) {
            retValue = value;
            break;
          } else if (type.equals(IndexEntryType.DELETE) && value.isFlagSet(IndexValue.Flags.Delete_Index)) {
            retValue = value;
            break;
          } else if (type.equals(IndexEntryType.PUT) && !value.isFlagSet(IndexValue.Flags.Delete_Index)) {
            retValue = value;
            break;
          }
        }
      }
      metrics.segmentsAccessedPerBlobCount.update(segmentsSearched);
    } finally {
      context.stop();
    }
    if (retValue != null) {
      logger.trace("Index : {} Returning value offset {} size {} ttl {}", dataDir, retValue.getOffset(),
          retValue.getSize(), retValue.getExpiresAtMs());
    }
    return retValue;
  }

  /**
   * Returns true if the given message was recently seen by this Index.
   * @param info the {@link MessageInfo} to check.
   * @return true if the exact message was recently added to this index; false otherwise.
   */
  boolean wasRecentlySeen(MessageInfo info) {
    Long crcInJournal = journal.getCrcOfKey(info.getStoreKey());
    return info.getCrc() != null && info.getCrc().equals(crcInJournal);
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
    return markAsDeleted(id, fileSpan, null, deletionTimeMs);
  }

  /**
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file span represented by this entry in the log
   * @param info this needs to be non-null in the case of recovery. Can be {@code null} otherwise.
   * @param deletionTimeMs deletion time of the blob. In-case of recovery, deletion time is obtained from {@code info}.
   * @return the {@link IndexValue} of the delete record
   * @throws StoreException
   */
  private IndexValue markAsDeleted(StoreKey id, FileSpan fileSpan, MessageInfo info, long deletionTimeMs)
      throws StoreException {
    validateFileSpan(fileSpan, true);
    IndexValue value = findKey(id);
    if (value == null && info == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.ID_Not_Found);
    } else if (value != null && value.isFlagSet(IndexValue.Flags.Delete_Index)) {
      throw new StoreException("Id " + id + " already deleted in index " + dataDir, StoreErrorCodes.ID_Deleted);
    }
    long size = fileSpan.getEndOffset().getOffset() - fileSpan.getStartOffset().getOffset();
    IndexValue newValue;
    if (value == null) {
      // it is possible that during recovery, the PUT record need not exist because it had been replaced by a
      // delete record in the map in IndexSegment but not written yet because the safe end point hadn't been reached
      // SEE: NOTE in IndexSegment::writeIndexSegmentToFile()
      newValue =
          new IndexValue(size, fileSpan.getStartOffset(), info.getExpirationTimeInMs(), info.getOperationTimeMs(),
              info.getAccountId(), info.getContainerId());
      newValue.clearOriginalMessageOffset();
    } else {
      newValue = new IndexValue(value.getSize(), value.getOffset(), value.getExpiresAtMs(), deletionTimeMs,
          value.getAccountId(), value.getContainerId());
      newValue.setNewOffset(fileSpan.getStartOffset());
      newValue.setNewSize(size);
    }
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    addToIndex(new IndexEntry(id, newValue, null), fileSpan);
    return new IndexValue(newValue.getSize(), newValue.getOffset(), newValue.getFlags(), newValue.getExpiresAtMs(),
        newValue.getOperationTimeInMs(), newValue.getAccountId(), newValue.getContainerId());
  }

  /**
   * Returns the blob read info for a given key
   * @param id The id of the entry whose info is required
   * @param getOptions the get options that indicate whether blob read info for deleted/expired blobs are to be returned.
   * @return The blob read info that contains the information for the given key
   * @throws StoreException
   */
  BlobReadOptions getBlobReadInfo(StoreKey id, EnumSet<StoreGetOptions> getOptions) throws StoreException {
    ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = validIndexSegments;
    IndexValue value = findKey(id, null, IndexEntryType.ANY, indexSegments);
    BlobReadOptions readOptions;
    if (value == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.ID_Not_Found);
    } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
      if (!getOptions.contains(StoreGetOptions.Store_Include_Deleted)) {
        throw new StoreException("Id " + id + " has been deleted in index " + dataDir, StoreErrorCodes.ID_Deleted);
      } else {
        readOptions = getDeletedBlobReadOptions(value, id, indexSegments);
      }
    } else if (isExpired(value) && !getOptions.contains(StoreGetOptions.Store_Include_Expired)) {
      throw new StoreException("Id " + id + " has expired ttl in index " + dataDir, StoreErrorCodes.TTL_Expired);
    } else {
      readOptions = new BlobReadOptions(log, value.getOffset(),
          new MessageInfo(id, value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index), value.getExpiresAtMs(),
              journal.getCrcOfKey(id), value.getAccountId(), value.getContainerId(), value.getOperationTimeInMs()));
    }
    return readOptions;
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
    try {
      IndexValue putValue =
          findKey(key, new FileSpan(getStartOffset(indexSegments), value.getOffset()), IndexEntryType.PUT,
              indexSegments);
      if (value.getOriginalMessageOffset() != value.getOffset().getOffset()
          && value.getOriginalMessageOffset() != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET) {
        // PUT record in the same log segment.
        String logSegmentName = value.getOffset().getName();
        // The delete entry in the index might not contain the information about the size of the original blob. So we
        // use the Message format to read and provide the information. The range in log that we provide starts at the
        // original message offset and ends at the delete message's start offset (the original message surely cannot go
        // beyond the start offset of the delete message).
        MessageInfo deletedBlobInfo =
            hardDelete.getMessageInfo(log.getSegment(logSegmentName), value.getOriginalMessageOffset(), factory);
        if (putValue != null && putValue.getOffset().getName().equals(value.getOffset().getName())) {
          if (putValue.getOffset().getOffset() != value.getOriginalMessageOffset()) {
            logger.error(
                "Offset in PUT index entry {} is different from original message offset in delete entry {} for key {}",
                putValue.getOffset().getOffset(), value.getOriginalMessageOffset(), key);
            metrics.putEntryDeletedInfoMismatchCount.inc();
          }
          if (putValue.getSize() != deletedBlobInfo.getSize()) {
            logger.error("Size in PUT index entry {} is different from that in the PUT record {} for ID {}",
                putValue.getSize(), deletedBlobInfo.getSize(), key);
            metrics.putEntryDeletedInfoMismatchCount.inc();
          }
          long putValueExpiresAtMs = Utils.getTimeInMsToTheNearestSec(putValue.getExpiresAtMs());
          long deletedBlobInfoExpiresAtMs = Utils.getTimeInMsToTheNearestSec(deletedBlobInfo.getExpirationTimeInMs());
          if (putValueExpiresAtMs != deletedBlobInfoExpiresAtMs) {
            logger.error("Expire time in PUT index entry {} is different from that in the PUT record {} for ID {}",
                putValueExpiresAtMs, deletedBlobInfoExpiresAtMs, key);
            metrics.putEntryDeletedInfoMismatchCount.inc();
          }
        }
        Offset offset = new Offset(logSegmentName, value.getOriginalMessageOffset());
        readOptions = new BlobReadOptions(log, offset,
            new MessageInfo(deletedBlobInfo.getStoreKey(), deletedBlobInfo.getSize(),
                deletedBlobInfo.getExpirationTimeInMs(), deletedBlobInfo.getAccountId(),
                deletedBlobInfo.getContainerId(), deletedBlobInfo.getOperationTimeMs()));
      } else if (putValue != null) {
        // PUT record in a different log segment.
        readOptions = new BlobReadOptions(log, putValue.getOffset(),
            new MessageInfo(key, putValue.getSize(), putValue.getExpiresAtMs(), putValue.getAccountId(),
                putValue.getContainerId(), putValue.getOperationTimeInMs()));
      } else {
        // PUT record no longer available.
        throw new StoreException("Did not find PUT index entry for key [" + key
            + "] and the the original offset in value of the DELETE entry was [" + value.getOriginalMessageOffset()
            + "]", StoreErrorCodes.ID_Deleted);
      }
    } catch (IOException e) {
      throw new StoreException("IOError when reading delete blob info from the log " + dataDir, e,
          StoreErrorCodes.IOError);
    }
    return readOptions;
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
      storeToken = revalidateStoreFindToken(storeToken, indexSegments);

      List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();
      if (!storeToken.getType().equals(StoreFindToken.Type.IndexBased)) {
        startTimeInMs = time.milliseconds();
        Offset offsetToStart = storeToken.getOffset();
        if (storeToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
          offsetToStart = getStartOffset();
        }
        logger.trace("Index : " + dataDir + " getting entries since " + offsetToStart);
        // check journal
        List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, storeToken.getInclusive());
        logger.trace("Journal based token, Time used to get entries: {}", (time.milliseconds() - startTimeInMs));
        // we deliberately obtain a snapshot of the index segments AFTER fetching from the journal. This ensures that
        // any and all entries returned from the journal are guaranteed to be in the obtained snapshot of indexSegments.
        indexSegments = validIndexSegments;

        // recheck to make sure that offsetToStart hasn't slipped out of the journal. It is possible (but highly
        // improbable) that the offset has slipped out of the journal and has been compacted b/w the time of getting
        // entries from the journal to when another view of the index segments was obtained.
        if (entries != null && offsetToStart.compareTo(journal.getFirstOffset()) >= 0) {
          startTimeInMs = time.milliseconds();
          logger.trace(
              "Index : " + dataDir + " retrieving from journal from offset " + offsetToStart + " total entries "
                  + entries.size());
          Offset offsetEnd = offsetToStart;
          long currentTotalSizeOfEntries = 0;
          for (JournalEntry entry : entries) {
            IndexValue value =
                findKey(entry.getKey(), new FileSpan(entry.getOffset(), getCurrentEndOffset(indexSegments)),
                    IndexEntryType.ANY, indexSegments);
            messageEntries.add(
                new MessageInfo(entry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
                    value.getExpiresAtMs(), value.getAccountId(), value.getContainerId(),
                    value.getOperationTimeInMs()));
            currentTotalSizeOfEntries += value.getSize();
            offsetEnd = entry.getOffset();
            if (currentTotalSizeOfEntries >= maxTotalSizeOfEntries) {
              break;
            }
          }
          logger.trace("Journal based token, Time used to generate message entries: {}",
              (time.milliseconds() - startTimeInMs));

          startTimeInMs = time.milliseconds();
          logger.trace("Index : " + dataDir + " new offset from find info " + offsetEnd);
          eliminateDuplicates(messageEntries);
          logger.trace("Journal based token, Time used to eliminate duplicates: {}",
              (time.milliseconds() - startTimeInMs));

          StoreFindToken storeFindToken = new StoreFindToken(offsetEnd, sessionId, incarnationId, false);
          // use the latest ref of indexSegments
          long bytesRead = getTotalBytesRead(storeFindToken, messageEntries, logEndOffsetBeforeFind, indexSegments);
          storeFindToken.setBytesRead(bytesRead);
          return new FindInfo(messageEntries, storeFindToken);
        } else {
          storeToken = revalidateStoreFindToken(storeToken, indexSegments);
          offsetToStart = storeToken.getType().equals(StoreFindToken.Type.Uninitialized) ? getStartOffset(indexSegments)
              : storeToken.getOffset();
          // Find index segment closest to the token offset.
          // Get entries starting from the first key in this offset.
          Map.Entry<Offset, IndexSegment> entry = indexSegments.floorEntry(offsetToStart);
          StoreFindToken newToken;
          if (entry != null && !entry.getKey().equals(indexSegments.lastKey())) {
            startTimeInMs = time.milliseconds();
            newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries), indexSegments);
            logger.trace("Journal based to segment based token, Time used to find entries: {}",
                (time.milliseconds() - startTimeInMs));

            startTimeInMs = time.milliseconds();
            updateDeleteStateForMessages(messageEntries);
            logger.trace("Journal based to segment based token, Time used to update delete state: {}",
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
        // Find the index segment corresponding to the token indexStartOffset.
        // Get entries starting from the token Key in this index.
        startTimeInMs = time.milliseconds();
        StoreFindToken newToken =
            findEntriesFromSegmentStartOffset(storeToken.getOffset(), storeToken.getStoreKey(), messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries), indexSegments);
        logger.trace("Segment based token, Time used to find entries: {}", (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        updateDeleteStateForMessages(messageEntries);
        logger.trace("Segment based token, Time used to update delete state: {}",
            (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        eliminateDuplicates(messageEntries);
        logger.trace("Segment based token, Time used to eliminate duplicates: {}",
            (time.milliseconds() - startTimeInMs));

        long totalBytesRead = getTotalBytesRead(newToken, messageEntries, logEndOffsetBeforeFind, indexSegments);
        newToken.setBytesRead(totalBytesRead);
        return new FindInfo(messageEntries, newToken);
      }
    } catch (IOException e) {
      throw new StoreException("IOError when finding entries for index " + dataDir, e, StoreErrorCodes.IOError);
    } catch (Exception e) {
      throw new StoreException("Unknown error when finding entries for index " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    }
  }

  /**
   * Validate the {@link StoreFindToken} and reset if required
   * @param storeToken the {@link StoreFindToken} that needs to be validated
   * @return the new {@link StoreFindToken} after validating
   */
  private StoreFindToken resetTokenIfRequired(StoreFindToken storeToken) {
    UUID remoteIncarnationId = storeToken.getIncarnationId();
    // if incarnationId is null, for backwards compatibility purposes, the token is considered as good.
    /// if not null, we check for a match
    if (!storeToken.getType().equals(StoreFindToken.Type.Uninitialized) && remoteIncarnationId != null
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
        if (!storeToken.getType().equals(StoreFindToken.Type.Uninitialized)
            && storeToken.getOffset().compareTo(logEndOffsetOnStartup) > 0) {
          logger.info("Index : " + dataDir + " resetting offset after not clean shutdown " + logEndOffsetOnStartup
              + " before offset " + storeToken.getOffset());
          storeToken = new StoreFindToken(logEndOffsetOnStartup, sessionId, incarnationId, true);
        }
      } else if (!storeToken.getType().equals(StoreFindToken.Type.Uninitialized)
          && storeToken.getOffset().compareTo(logEndOffsetOnStartup) > 0) {
        logger.error(
            "Index : " + dataDir + " invalid token. Provided offset is outside the log range after clean shutdown");
        // if the shutdown was clean, the offset should always be lesser or equal to the logEndOffsetOnStartup
        throw new IllegalArgumentException(
            "Invalid token. Provided offset is outside the log range after clean shutdown");
      }
    }
    return storeToken;
  }

  /**
   * Gets the total number of bytes read from the log at the position of {@code token}. This includes any overhead due
   * to headers and empty space.
   * @param token the point until which the log has been read.
   * @param messageEntries the list of {@link MessageInfo} that were read when producing {@code token}.
   * @param logEndOffsetBeforeFind the end offset of the log before a find was attempted.
   * @param indexSegments the list of index segments to use.
   * @return the total number of bytes read from the log at the position of {@code token}.
   */
  private long getTotalBytesRead(StoreFindToken token, List<MessageInfo> messageEntries, Offset logEndOffsetBeforeFind,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    long bytesRead = 0;
    if (token.getType().equals(StoreFindToken.Type.IndexBased)) {
      bytesRead = getAbsolutePositionInLogForOffset(token.getOffset(), indexSegments);
    } else if (token.getType().equals(StoreFindToken.Type.JournalBased)) {
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
   * @return the number of valid log segments starting from the first segment.
   */
  long getLogSegmentCount() {
    return getLogSegmentToIndexSegmentMapping(validIndexSegments).size();
  }

  /**
   * @return the absolute position represented by {@code offset} in the {@link Log}.
   */
  long getAbsolutePositionInLogForOffset(Offset offset) {
    return getAbsolutePositionInLogForOffset(offset, validIndexSegments);
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
      throw new IllegalArgumentException("Offset is invalid: " + offset);
    }
    int numPrecedingLogSegments = getLogSegmentToIndexSegmentMapping(indexSegments).headMap(offset.getName()).size();
    return numPrecedingLogSegments * log.getSegmentCapacity() + offset.getOffset();
  }

  /**
   * Fetches {@link LogSegment} names whose entries don't over lap with {@link Journal}. Returns {@code null}
   * if there aren't any
   * @return a {@link List<String>} of {@link LogSegment} names whose entries don't overlap with {@link Journal}.
   * {@code null} if there aren't any
   */
  List<String> getLogSegmentsNotInJournal() {
    LogSegment logSegment = log.getFirstSegment();
    Offset firstOffsetInJournal = journal.getFirstOffset();
    List<String> logSegmentNamesToReturn = new ArrayList<>();
    while (firstOffsetInJournal != null && logSegment != null) {
      if (!logSegment.getName().equals(firstOffsetInJournal.getName())) {
        logSegmentNamesToReturn.add(logSegment.getName());
      } else {
        break;
      }
      logSegment = log.getNextSegment(logSegment);
    }
    return logSegmentNamesToReturn.size() > 0 ? logSegmentNamesToReturn : null;
  }

  /**
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return mapping from log segment names to {@link IndexSegment} instances that refer to them.
   */
  private TreeMap<String, List<IndexSegment>> getLogSegmentToIndexSegmentMapping(
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    TreeMap<String, List<IndexSegment>> mapping = new TreeMap<>(LogSegmentNameHelper.COMPARATOR);
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : indexSegments.entrySet()) {
      IndexSegment indexSegment = indexSegmentEntry.getValue();
      String logSegmentName = indexSegment.getLogSegmentName();
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
   * @param key The key representing the position (exclusive) in the segment to start reading entries from. If the key
   *            is null, all the keys will be read.
   * @param messageEntries the list to be populated with the MessageInfo for every entry that is read.
   * @param findEntriesCondition that determines whether to fetch more entries based on a maximum total size of entries
   *                             that needs to be returned and a time that determines the latest segment to be scanned.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   * @return A token representing the position in the segment/journal up to which entries have been read and returned.
   */
  private StoreFindToken findEntriesFromSegmentStartOffset(Offset initialSegmentStartOffset, StoreKey key,
      List<MessageInfo> messageEntries, FindEntriesCondition findEntriesCondition,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) throws IOException, StoreException {
    Offset segmentStartOffset = initialSegmentStartOffset;
    if (segmentStartOffset.equals(indexSegments.lastKey())) {
      // We would never have given away a token with a segmentStartOffset of the latest segment.
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
      if (segmentToProcess.getEntriesSince(key, findEntriesCondition, messageEntries, currentTotalSizeOfEntries)) {
        // if we did fetch entries from this segment, set the new token info accordingly.
        newTokenSegmentStartOffset = segmentStartOffset;
      }
      logger.trace(
          "Index : " + dataDir + " findEntriesFromOffset segment start offset " + segmentStartOffset + " with key "
              + key + " total entries received " + messageEntries.size());
      segmentStartOffset = indexSegments.higherKey(segmentStartOffset);
      segmentToProcess = indexSegments.get(segmentStartOffset);
    }

    while (findEntriesCondition.proceed(currentTotalSizeOfEntries.get(), segmentToProcess.getLastModifiedTimeSecs())) {
      // Check in the journal to see if we are already at an offset in the journal, if so get entries from it.
      Offset journalFirstOffsetBeforeCheck = journal.getFirstOffset();
      Offset journalLastOffsetBeforeCheck = journal.getLastOffset();
      List<JournalEntry> entries = journal.getEntriesSince(segmentStartOffset, true);
      Offset endOffsetOfSnapshot = getCurrentEndOffset(indexSegments);
      if (entries != null) {
        logger.trace("Index : " + dataDir + " findEntriesFromOffset journal offset " + segmentStartOffset
            + " total entries received " + entries.size());
        IndexSegment currentSegment = segmentToProcess;
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
          IndexValue value =
              findKey(entry.getKey(), new FileSpan(entry.getOffset(), endOffsetOfSnapshot), IndexEntryType.ANY,
                  indexSegments);
          messageEntries.add(
              new MessageInfo(entry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
                  value.getExpiresAtMs(), value.getAccountId(), value.getContainerId(), value.getOperationTimeInMs()));
          currentTotalSizeOfEntries.addAndGet(value.getSize());
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntries.get(),
              currentSegment.getLastModifiedTimeSecs())) {
            break;
          }
        }
        break; // we have entered and finished reading from the journal, so we are done.
      }

      if (segmentStartOffset == validIndexSegments.lastKey()) {
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
        if (segmentToProcess.getEntriesSince(null, findEntriesCondition, messageEntries, currentTotalSizeOfEntries)) {
          newTokenSegmentStartOffset = segmentStartOffset;
        }
        logger.trace("Index : " + dataDir + " findEntriesFromOffset segment start offset " + segmentStartOffset
            + " with all the keys, total entries received " + messageEntries.size());
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
      return new StoreFindToken(newTokenOffsetInJournal, sessionId, incarnationId, false);
    } else if (messageEntries.size() == 0 && !findEntriesCondition.hasEndTime()) {
      // If the condition does not have an endtime, then since we have entered a segment, we should return at least one
      // message
      throw new IllegalStateException(
          "Message entries cannot be null. At least one entry should have been returned, start offset: "
              + initialSegmentStartOffset + ", key: " + key + ", findEntriesCondition: " + findEntriesCondition);
    } else {
      // if newTokenSegmentStartOffset is set, then we did fetch entries from that segment, otherwise return an
      // uninitialized token
      return newTokenSegmentStartOffset == null ? new StoreFindToken()
          : new StoreFindToken(messageEntries.get(messageEntries.size() - 1).getStoreKey(), newTokenSegmentStartOffset,
              sessionId, incarnationId);
    }
  }

  /**
   * We can have duplicate entries in the message entries since updates can happen to the same key. For example,
   * insert a key followed by a delete. This would create two entries in the journal or the index. A single findInfo
   * could read both the entries. The findInfo should return as clean information as possible. This method removes
   * the oldest duplicate in the list.
   * @param messageEntries The message entry list where duplicates need to be removed
   */
  private void eliminateDuplicates(List<MessageInfo> messageEntries) {
    Set<StoreKey> setToFindDuplicate = new HashSet<StoreKey>();
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator(messageEntries.size());
    while (messageEntriesIterator.hasPrevious()) {
      MessageInfo messageInfo = messageEntriesIterator.previous();
      if (setToFindDuplicate.contains(messageInfo.getStoreKey())) {
        messageEntriesIterator.remove();
      } else {
        setToFindDuplicate.add(messageInfo.getStoreKey());
      }
    }
  }

  /**
   * Updates the messages with their deleted state. This method can be used when
   * the messages have been retrieved from an old index segment and needs to be updated with the deleted state
   * from the new index segment
   * @param messageEntries The message entries that needs to be updated with the delete state
   */
  private void updateDeleteStateForMessages(List<MessageInfo> messageEntries) throws StoreException {
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator();
    while (messageEntriesIterator.hasNext()) {
      MessageInfo messageInfo = messageEntriesIterator.next();
      if (!messageInfo.isDeleted()) {
        // ok to use most recent ref to filter out deleted records.
        IndexValue indexValue = findKey(messageInfo.getStoreKey());
        messageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(),
            indexValue.isFlagSet(IndexValue.Flags.Delete_Index), messageInfo.getExpirationTimeInMs(),
            indexValue.getAccountId(), indexValue.getContainerId(), indexValue.getOperationTimeInMs());
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
   * @throws StoreException
   */
  void close() throws StoreException {
    long startTimeInMs = time.milliseconds();
    try {
      persistor.write();
      if (hardDeleter != null) {
        try {
          hardDeleter.shutdown();
        } catch (Exception e) {
          logger.error("Index : " + dataDir + " error while persisting cleanup token ", e);
        }
      }
      try {
        cleanShutdownFile.createNewFile();
      } catch (IOException e) {
        logger.error("Index : " + dataDir + " error while creating clean shutdown file ", e);
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
   * Gets the start {@link Offset} of the given instance of {@code indexSegments}.
   * @param indexSegments the map of index segment start {@link Offset} to {@link IndexSegment} instances
   */
  private Offset getCurrentEndOffset(ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    return indexSegments.size() == 0 ? getStartOffset(indexSegments)
        : indexSegments.lastEntry().getValue().getEndOffset();
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
   * where entries needs to be fetched
   * @param token The token that signifies the start position in the index from where deleted entries need to be
   *              retrieved
   * @param maxTotalSizeOfEntries The maximum total size of entries that need to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value.
   * @param endTimeSeconds The (approximate) time of the latest entry to be fetched. This is used at segment granularity.
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  FindInfo findDeletedEntriesSince(FindToken token, long maxTotalSizeOfEntries, long endTimeSeconds)
      throws StoreException {
    try {
      StoreFindToken storeToken = (StoreFindToken) token;
      StoreFindToken newToken;
      List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();

      if (storeToken.getType().equals(StoreFindToken.Type.IndexBased)) {
        // Case 1: index based
        // Find the index segment corresponding to the token indexStartOffset.
        // Get entries starting from the token Key in this index.
        newToken = findEntriesFromSegmentStartOffset(storeToken.getOffset(), storeToken.getStoreKey(), messageEntries,
            new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds), validIndexSegments);
        if (newToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
          newToken = storeToken;
        }
      } else {
        // journal based or empty
        Offset offsetToStart = storeToken.getOffset();
        boolean inclusive = false;
        if (storeToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
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
                    IndexEntryType.ANY, indexSegments);
            if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
              messageEntries.add(
                  new MessageInfo(entry.getKey(), value.getSize(), true, value.getExpiresAtMs(), value.getAccountId(),
                      value.getContainerId(), value.getOperationTimeInMs()));
            }
            offsetEnd = entry.getOffset();
            currentTotalSizeOfEntries += value.getSize();
            if (currentTotalSizeOfEntries >= maxTotalSizeOfEntries) {
              break;
            }
          }
          newToken = new StoreFindToken(offsetEnd, sessionId, incarnationId, false);
        } else {
          // Case 3: offset based, but offset out of journal
          Map.Entry<Offset, IndexSegment> entry = indexSegments.floorEntry(offsetToStart);
          if (entry != null && entry.getKey() != indexSegments.lastKey()) {
            newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds), indexSegments);
            if (newToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
              newToken = storeToken;
            }
          } else {
            newToken = storeToken; //use the same offset as before.
          }
        }
      }
      filterDeleteEntries(messageEntries);
      eliminateDuplicates(messageEntries);
      return new FindInfo(messageEntries, newToken);
    } catch (IOException e) {
      throw new StoreException("IOError when finding entries for index " + dataDir, e, StoreErrorCodes.IOError);
    } catch (Exception e) {
      throw new StoreException("Unknown error when finding entries for index " + dataDir, e,
          StoreErrorCodes.Unknown_Error);
    }
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
   * not, a {@link StoreFindToken.Type#Uninitialized} token is returned.
   * @param token the {@link StoreFindToken} to revalidate.
   * @return {@code token} if is consistent with the current set of index segments, a new
   * {@link StoreFindToken.Type#Uninitialized} token otherwise.
   */
  FindToken revalidateFindToken(FindToken token) {
    return revalidateStoreFindToken((StoreFindToken) token, validIndexSegments);
  }

  /**
   * Re-validates the {@code token} to ensure that it is consistent with the given view of {@code indexSegments}. If it
   * is not, a {@link StoreFindToken.Type#Uninitialized} token is returned.
   * @param token the {@link StoreFindToken} to revalidate.
   * @param indexSegments the view of the index segments to revalidate against.
   * @return {@code token} if is consistent with {@code indexSegments}, a new {@link StoreFindToken.Type#Uninitialized}
   * token otherwise.
   */
  private StoreFindToken revalidateStoreFindToken(StoreFindToken token,
      ConcurrentSkipListMap<Offset, IndexSegment> indexSegments) {
    StoreFindToken revalidatedToken = token;
    Offset offset = token.getOffset();
    switch (token.getType()) {
      case Uninitialized:
        // nothing to do.
        break;
      case JournalBased:
        Offset floorOffset = indexSegments.floorKey(offset);
        if (floorOffset == null || !floorOffset.getName().equals(offset.getName())) {
          revalidatedToken = new StoreFindToken();
          logger.info("Revalidated token {} because it is invalid for the index segment map", token);
        }
        break;
      case IndexBased:
        if (!indexSegments.containsKey(offset)) {
          revalidatedToken = new StoreFindToken();
          logger.info("Revalidated token {} because it is invalid for the index segment map", token);
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized token type: " + token.getType());
    }
    return revalidatedToken;
  }

  /**
   * Gets the list of {@link IndexSegment} files that refer to the log segment with name {@code logSegmentName}.
   * @param dataDir the directory where the index files are.
   * @param logSegmentName the name of the log segment whose index segment files are required.
   * @return the list of {@link IndexSegment} files that refer to the log segment with name {@code logSegmentName}.
   */
  static File[] getIndexSegmentFilesForLogSegment(String dataDir, final String logSegmentName) {
    return new File(dataDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(logSegmentName) && name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX);
      }
    });
  }

  /**
   * Cleans up all files related to index segments that refer to the log segment with name {@code logSegmentName}.
   *  @param dataDir the directory where the index files are.
   * @param logSegmentName the name of the log segment whose index segment related files need to be deleteds.
   * @throws IOException if {@code dataDir} could not be read or if a file could not be deleted.
   */
  static void cleanupIndexSegmentFilesForLogSegment(String dataDir, final String logSegmentName) throws IOException {
    File[] filesToCleanup = new File(dataDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(logSegmentName) && (name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX)
            || name.endsWith(IndexSegment.BLOOM_FILE_NAME_SUFFIX));
      }
    });
    if (filesToCleanup == null) {
      throw new IOException("Failed to list index segment files");
    }
    for (File file : filesToCleanup) {
      if (!file.delete()) {
        throw new IOException("Could not delete file named " + file);
      }
    }
  }

  private class IndexPersistor implements Runnable {

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
                + "currentEndOffSet of index " + indexEndOffsetBeforeFlush, StoreErrorCodes.Illegal_Index_State);
          }

          if (hardDeleter != null) {
            hardDeleter.preLogFlush();
          }

          // flush the log to ensure everything till the fileEndPointerBeforeFlush is flushed
          log.flush();

          if (hardDeleter != null) {
            hardDeleter.postLogFlush();
          }

          Map.Entry<Offset, IndexSegment> prevEntry = indexSegments.lowerEntry(lastEntry.getKey());
          IndexSegment prevInfo = prevEntry != null ? prevEntry.getValue() : null;
          List<IndexSegment> prevInfosToWrite = new ArrayList<>();
          Offset currentLogEndPointer = log.getEndOffset();
          while (prevInfo != null && !prevInfo.isMapped()) {
            if (prevInfo.getEndOffset().compareTo(currentLogEndPointer) > 0) {
              String message = "The read only index cannot have a file end pointer " + prevInfo.getEndOffset()
                  + " greater than the log end offset " + currentLogEndPointer;
              throw new StoreException(message, StoreErrorCodes.IOError);
            }
            prevInfosToWrite.add(prevInfo);
            Map.Entry<Offset, IndexSegment> infoEntry = indexSegments.lowerEntry(prevInfo.getStartOffset());
            prevInfo = infoEntry != null ? infoEntry.getValue() : null;
          }
          for (int i = prevInfosToWrite.size() - 1; i >= 0; i--) {
            IndexSegment toWrite = prevInfosToWrite.get(i);
            logger.trace("Index : {} writing prev index with end offset {}", dataDir, toWrite.getEndOffset());
            toWrite.writeIndexSegmentToFile(toWrite.getEndOffset());
            toWrite.map(true);
          }
          currentInfo.writeIndexSegmentToFile(indexEndOffsetBeforeFlush);
        }
      } catch (IOException e) {
        throw new StoreException("IO error while writing index to file", e, StoreErrorCodes.IOError);
      } finally {
        context.stop();
      }
    }

    @Override
    public void run() {
      try {
        write();
      } catch (Exception e) {
        logger.error("Index : " + dataDir + " error while persisting the index to disk ", e);
      }
    }
  }
}
