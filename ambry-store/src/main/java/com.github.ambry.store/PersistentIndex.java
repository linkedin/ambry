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
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
  private enum IndexEntryType {
    ANY, PUT, DELETE,
  }

  static final short VERSION = 0;
  static final String INDEX_SEGMENT_FILE_NAME_SUFFIX = "index";
  static final String BLOOM_FILE_NAME_SUFFIX = "bloom";
  static final String CLEAN_SHUTDOWN_FILENAME = "cleanshutdown";

  static final Comparator<File> INDEX_FILE_COMPARATOR = new Comparator<File>() {
    @Override
    public int compare(File o1, File o2) {
      if (o1 == null || o2 == null) {
        throw new NullPointerException("arguments to compare two files is null");
      }
      Offset o1Offset = IndexSegment.getIndexSegmentStartOffset(o1.getName());
      return o1Offset.compareTo(IndexSegment.getIndexSegmentStartOffset(o2.getName()));
    }
  };
  static final FilenameFilter INDEX_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(INDEX_SEGMENT_FILE_NAME_SUFFIX);
    }
  };

  final ScheduledExecutorService scheduler;
  final ConcurrentSkipListMap<Offset, IndexSegment> indexes = new ConcurrentSkipListMap<>();
  final Journal journal;
  final HardDeleter hardDeleter;
  final Thread hardDeleteThread;

  private final Log log;
  private final Offset logAbsoluteZeroOffset;
  private final long maxInMemoryIndexSizeInBytes;
  private final int maxInMemoryNumElements;
  private final String dataDir;
  private final MessageStoreHardDelete hardDelete;
  private final StoreKeyFactory factory;
  private final StoreConfig config;
  private final boolean cleanShutdown;
  private final Offset logEndOffsetOnStartup;
  private final StoreMetrics metrics;
  private final UUID incarnationId;
  private final Time time;

  private final UUID sessionId = UUID.randomUUID();
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final IndexPersistor persistor = new IndexPersistor();

  /**
   * Creates a new persistent index
   * @param datadir The directory to use to store the index files
   * @param scheduler The scheduler that runs regular background tasks
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @param hardDelete  The hard delete handle used to perform hard deletes
   * @param metrics the metrics object
   * @param time the time instance to use
   * @param incarnationId to uniquely identify the store's incarnation
   * @throws StoreException
   */
  PersistentIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, StoreMetrics metrics,
      Time time, UUID incarnationId) throws StoreException {
    /*
    If a put and a delete of a key happens within the same segment, the segment will have only one entry for it,
    whereas the journal keeps both. In order to account for this, and to ensure that the journal always has all the
    elements held by the latest segment, the journal needs to be able to hold twice the max number of elements a
    segment can hold.
    */
    this(datadir, scheduler, log, config, factory, recovery, hardDelete, metrics,
        new Journal(datadir, 2 * config.storeIndexMaxNumberOfInmemElements,
            config.storeMaxNumberOfEntriesToReturnFromJournal), time, incarnationId);
  }

  /**
   * Creates a new persistent index
   * @param datadir The directory to use to store the index files
   * @param scheduler The scheduler that runs regular background tasks
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @param hardDelete  The hard delete handle used to perform hard deletes
   * @param metrics the metrics object
   * @param journal the journal to use
   * @param time the time instance to use
   * @param incarnationId to uniquely identify the store's incarnation
   * @throws StoreException
   */
  PersistentIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, StoreMetrics metrics,
      Journal journal, Time time, UUID incarnationId) throws StoreException {
    File[] indexFiles = new File(datadir).listFiles(INDEX_FILE_FILTER);
    if (indexFiles == null) {
      throw new StoreException("Could not read index files from directory [" + datadir + "]",
          StoreErrorCodes.Index_Creation_Failure);
    }
    try {
      this.dataDir = datadir;
      this.log = log;
      this.time = time;
      this.scheduler = scheduler;
      this.metrics = metrics;
      this.factory = factory;
      this.config = config;
      this.hardDelete = hardDelete;
      this.journal = journal;
      this.incarnationId = incarnationId;
      this.maxInMemoryIndexSizeInBytes = config.storeIndexMaxMemorySizeBytes;
      this.maxInMemoryNumElements = config.storeIndexMaxNumberOfInmemElements;

      logAbsoluteZeroOffset = new Offset(log.getFirstSegment().getName(), 0);
      hardDeleter = new HardDeleter(config, metrics, datadir, log, this, hardDelete, factory, time);

      Arrays.sort(indexFiles, INDEX_FILE_COMPARATOR);
      for (int i = 0; i < indexFiles.length; i++) {
        // We map all the indexes except the most recent index segment.
        // The recent index segment would go through recovery after they have been
        // read into memory
        boolean map = i < indexFiles.length - 1;
        IndexSegment info = new IndexSegment(indexFiles[i], map, factory, config, metrics, journal);
        logger.info("Index : {} loaded index segment {} with start offset {} and end offset {} ", datadir,
            indexFiles[i], info.getStartOffset(), info.getEndOffset());
        indexes.put(info.getStartOffset(), info);
      }
      logger.info("Index : " + datadir + " log end offset of index  before recovery " + log.getEndOffset());
      // delete the shutdown file
      File cleanShutdownFile = new File(datadir, CLEAN_SHUTDOWN_FILENAME);
      cleanShutdown = cleanShutdownFile.exists();
      if (cleanShutdown) {
        cleanShutdownFile.delete();
      }
      recover(recovery);
      setEndOffsets();
      log.setActiveSegment(getCurrentEndOffset().getName());
      logEndOffsetOnStartup = log.getEndOffset();

      // After recovering the last messages, and setting the log end offset, let the hard delete thread do its recovery.
      // NOTE: It is safe to do the hard delete recovery after the regular recovery because we ensure that hard deletes
      // never work on the part of the log that is not yet flushed (by ensuring that the message retention
      // period is longer than the log flush time).
      logger.info("Index : " + datadir + " Starting hard delete recovery");
      hardDeleter.performRecovery();
      logger.info("Index : " + datadir + " Finished performing hard delete recovery");

      // start scheduler thread to persist index in the background
      this.scheduler.scheduleAtFixedRate(persistor,
          config.storeDataFlushDelaySeconds + new Random().nextInt(Time.SecsPerMin),
          config.storeDataFlushIntervalSeconds, TimeUnit.SECONDS);
      if (config.storeEnableHardDelete) {
        logger.info("Index : " + datadir + " Starting hard delete thread ");
        hardDeleteThread = Utils.newThread("hard delete thread " + datadir, hardDeleter, true);
        hardDeleteThread.start();
      } else {
        hardDeleteThread = null;
        hardDeleter.close();
      }
      metrics.initializeHardDeleteMetric(hardDeleter, log);
    } catch (StoreException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreException("Unknown error while creating index " + datadir, e,
          StoreErrorCodes.Index_Creation_Failure);
    }
  }

  /**
   * Recovers a segment given the end offset in the log and a recovery handler
   * @param recovery The recovery handler that is used to perform the recovery
   * @throws StoreException
   * @throws IOException
   */
  private void recover(MessageStoreRecovery recovery) throws StoreException, IOException {
    final Timer.Context context = metrics.recoveryTime.time();
    Offset recoveryStartOffset = log.getStartOffset();
    if (indexes.size() > 0) {
      IndexSegment indexSegment = indexes.lastEntry().getValue();
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
          markAsDeleted(info.getStoreKey(), new FileSpan(runningOffset, infoEndOffset));
          logger.info("Index : {} updated message with key {} size {} ttl {} deleted {}", dataDir, info.getStoreKey(),
              value.getSize(), value.getExpiresAtMs(), info.isDeleted());
        } else if (value != null) {
          throw new StoreException("Illegal message state during recovery. Duplicate PUT record",
              StoreErrorCodes.Initialization_Error);
        } else {
          // create a new entry in the index
          IndexValue newValue = new IndexValue(info.getSize(), runningOffset, info.getExpirationTimeInMs());
          addToIndex(new IndexEntry(info.getStoreKey(), newValue), new FileSpan(runningOffset, infoEndOffset));
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
   * @throws IOException if an end offset could not be set due to I/O error.
   */
  private void setEndOffsets() throws IOException {
    for (Offset segmentStartOffset : indexes.keySet()) {
      Offset nextIndexSegmentStartOffset = indexes.higherKey(segmentStartOffset);
      if (nextIndexSegmentStartOffset == null || !segmentStartOffset.getName()
          .equals(nextIndexSegmentStartOffset.getName())) {
        // this is the last index segment for the log segment it refers to.
        IndexSegment indexSegment = indexes.get(segmentStartOffset);
        LogSegment logSegment = log.getSegment(segmentStartOffset.getName());
        logSegment.setEndOffset(indexSegment.getEndOffset().getOffset());
      }
    }
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
      IndexSegment info = new IndexSegment(dataDir, entry.getValue().getOffset(), factory, entry.getKey().sizeInBytes(),
          entry.getValue().getBytes().capacity(), config, metrics);
      info.addEntry(entry, fileSpan.getEndOffset());
      indexes.put(info.getStartOffset(), info);
    } else {
      indexes.lastEntry().getValue().addEntry(entry, fileSpan.getEndOffset());
    }
    journal.addEntry(entry.getValue().getOffset(), entry.getKey());
  }

  /**
   * Adds a set of entries to the index
   * @param entries The entries to be added to the index
   * @param fileSpan The file span that the entries represent in the log
   * @throws StoreException
   */
  void addToIndex(ArrayList<IndexEntry> entries, FileSpan fileSpan) throws StoreException {
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
    if (indexes.size() == 0) {
      logger.info("Creating first segment");
      return true;
    }

    IndexSegment lastSegment = indexes.lastEntry().getValue();
    if (lastSegment.getSizeWritten() >= maxInMemoryIndexSizeInBytes) {
      logger.info("Index: {} Rolling over because the size written {} >= maxInMemoryIndexSizeInBytes {}", dataDir,
          lastSegment.getSizeWritten(), maxInMemoryIndexSizeInBytes);
      return true;
    }
    if (lastSegment.getNumberOfItems() >= maxInMemoryNumElements) {
      logger.info("Index: {} Rolling over because the number of items in the last segment: {} >= "
          + "maxInMemoryNumElements {}", dataDir, lastSegment.getNumberOfItems(), maxInMemoryNumElements);
      return true;
    }
    if (lastSegment.getKeySize() != entry.getKey().sizeInBytes()) {
      logger.info("Index: {} Rolling over because the segment keySize: {} != entry's keysize {}", dataDir,
          lastSegment.getKeySize(), entry.getKey().sizeInBytes());
      return true;
    }
    if (lastSegment.getValueSize() != entry.getValue().getBytes().capacity()) {
      logger.info("Index: {} Rolling over because the segment value size: {} != current entry value size: {}", dataDir,
          lastSegment.getValueSize(), entry.getValue().getBytes().capacity());
      return true;
    }
    return !entry.getValue().getOffset().getName().equals(lastSegment.getLogSegmentName());
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
    return findKey(key, fileSpan, IndexEntryType.ANY);
  }

  /**
   * Finds the {@link IndexValue} of type {@code type} associated with the {@code key} if it is present in the index
   * within the given {@code fileSpan}.
   * @param key the {@link StoreKey} whose {@link IndexValue} is required.
   * @param fileSpan {@link FileSpan} which specifies the range within which search should be made
   * @param type the {@link IndexEntryType} desired.
   * @return The associated {@link IndexValue} of the type {@code type} if it exists within the {@code fileSpan},
   * {@code null} otherwise.
   * @throws StoreException
   */
  private IndexValue findKey(StoreKey key, FileSpan fileSpan, IndexEntryType type) throws StoreException {
    IndexValue retValue = null;
    final Timer.Context context = metrics.findTime.time();
    try {
      ConcurrentNavigableMap<Offset, IndexSegment> segmentsMapToSearch;
      if (fileSpan == null) {
        logger.trace("Searching for " + key + " in the entire index");
        segmentsMapToSearch = indexes.descendingMap();
      } else {
        logger.trace(
            "Searching for " + key + " in index with filespan ranging from " + fileSpan.getStartOffset() + " to "
                + fileSpan.getEndOffset());
        segmentsMapToSearch =
            indexes.subMap(indexes.floorKey(fileSpan.getStartOffset()), true, indexes.floorKey(fileSpan.getEndOffset()),
                true).descendingMap();
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
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file span represented by this entry in the log
   * @throws StoreException
   */
  void markAsDeleted(StoreKey id, FileSpan fileSpan) throws StoreException {
    validateFileSpan(fileSpan, true);
    IndexValue value = findKey(id);
    if (value == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.ID_Not_Found);
    } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
      throw new StoreException("Id " + id + " already deleted in index " + dataDir, StoreErrorCodes.ID_Deleted);
    }

    IndexValue newValue = new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(fileSpan.getStartOffset());
    newValue.setNewSize(fileSpan.getEndOffset().getOffset() - fileSpan.getStartOffset().getOffset());
    addToIndex(new IndexEntry(id, newValue), fileSpan);
  }

  /**
   * Returns the blob read info for a given key
   * @param id The id of the entry whose info is required
   * @param getOptions the get options that indicate whether blob read info for deleted/expired blobs are to be returned.
   * @return The blob read info that contains the information for the given key
   * @throws StoreException
   */
  BlobReadOptions getBlobReadInfo(StoreKey id, EnumSet<StoreGetOptions> getOptions) throws StoreException {
    IndexValue value = findKey(id);
    BlobReadOptions readOptions;
    if (value == null) {
      throw new StoreException("Id " + id + " not present in index " + dataDir, StoreErrorCodes.ID_Not_Found);
    } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
      if (!getOptions.contains(StoreGetOptions.Store_Include_Deleted)) {
        throw new StoreException("Id " + id + " has been deleted in index " + dataDir, StoreErrorCodes.ID_Deleted);
      } else {
        readOptions = getDeletedBlobReadOptions(value, id);
      }
    } else if (isExpired(value) && !getOptions.contains(StoreGetOptions.Store_Include_Expired)) {
      throw new StoreException("Id " + id + " has expired ttl in index " + dataDir, StoreErrorCodes.TTL_Expired);
    } else {
      readOptions = new BlobReadOptions(log, value.getOffset(), value.getSize(), value.getExpiresAtMs(), id);
    }
    return readOptions;
  }

  /**
   * Gets {@link BlobReadOptions} for a deleted blob.
   * @param value the {@link IndexValue} of the delete index entry for the blob.
   * @param key the {@link StoreKey} for which {@code value} is the delete {@link IndexValue}
   * @return the {@link BlobReadOptions} that contains the information for the given {@code id}
   * @throws StoreException
   */
  private BlobReadOptions getDeletedBlobReadOptions(IndexValue value, StoreKey key) throws StoreException {
    BlobReadOptions readOptions;
    try {
      IndexValue putValue =
          findKey(key, new FileSpan(indexes.firstEntry().getValue().getStartOffset(), value.getOffset()),
              IndexEntryType.PUT);
      if (value.getOriginalMessageOffset() != -1) {
        // PUT record in the same log segment.
        String logSegmentName = value.getOffset().getName();
        // The delete entry in the index might not contain the information about the size of the original blob. So we
        // use the Message format to read and provide the information. The range in log that we provide starts at the
        // original message offset and ends at the delete message's start offset (the original message surely cannot go
        // beyond the start offset of the delete message).
        MessageInfo deletedBlobInfo =
            hardDelete.getMessageInfo(log.getSegment(logSegmentName), value.getOriginalMessageOffset(), factory);
        if (putValue != null) {
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
          if (putValue.getExpiresAtMs() != deletedBlobInfo.getExpirationTimeInMs()) {
            logger.error("Expire time in PUT index entry {} is different from that in the PUT record {} for ID {}",
                putValue.getExpiresAtMs(), deletedBlobInfo.getExpirationTimeInMs(), key);
            metrics.putEntryDeletedInfoMismatchCount.inc();
          }
        }
        Offset offset = new Offset(logSegmentName, value.getOriginalMessageOffset());
        readOptions =
            new BlobReadOptions(log, offset, deletedBlobInfo.getSize(), deletedBlobInfo.getExpirationTimeInMs(),
                deletedBlobInfo.getStoreKey());
      } else if (putValue != null) {
        // PUT record in a different log segment.
        readOptions =
            new BlobReadOptions(log, putValue.getOffset(), putValue.getSize(), putValue.getExpiresAtMs(), key);
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

  private boolean isExpired(IndexValue value) {
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

      List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();
      if (!storeToken.getType().equals(StoreFindToken.Type.IndexBased)) {
        startTimeInMs = time.milliseconds();
        Offset offsetToStart = storeToken.getOffset();
        if (storeToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
          offsetToStart = log.getStartOffset();
        }
        logger.trace("Index : " + dataDir + " getting entries since " + offsetToStart);
        // check journal
        List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, storeToken.getInclusive());
        logger.trace("Journal based token, Time used to get entries: {}", (time.milliseconds() - startTimeInMs));

        if (entries != null) {
          startTimeInMs = time.milliseconds();
          logger.trace(
              "Index : " + dataDir + " retrieving from journal from offset " + offsetToStart + " total entries "
                  + entries.size());
          Offset offsetEnd = offsetToStart;
          long currentTotalSizeOfEntries = 0;
          for (JournalEntry entry : entries) {
            IndexValue value = findKey(entry.getKey());
            messageEntries.add(
                new MessageInfo(entry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
                    value.getExpiresAtMs()));
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
          long bytesRead = getTotalBytesRead(storeFindToken, messageEntries, logEndOffsetBeforeFind);
          storeFindToken.setBytesRead(bytesRead);
          return new FindInfo(messageEntries, storeFindToken);
        } else {
          // Find index segment closest to the token offset.
          // Get entries starting from the first key in this offset.
          Map.Entry<Offset, IndexSegment> entry = indexes.floorEntry(offsetToStart);
          StoreFindToken newToken;
          if (entry != null && !entry.getKey().equals(indexes.lastKey())) {
            startTimeInMs = time.milliseconds();
            newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries));
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
          long totalBytesRead = getTotalBytesRead(newToken, messageEntries, logEndOffsetBeforeFind);
          newToken.setBytesRead(totalBytesRead);
          return new FindInfo(messageEntries, newToken);
        }
      } else {
        // Find the index segment corresponding to the token indexStartOffset.
        // Get entries starting from the token Key in this index.
        startTimeInMs = time.milliseconds();
        StoreFindToken newToken =
            findEntriesFromSegmentStartOffset(storeToken.getOffset(), storeToken.getStoreKey(), messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries));
        logger.trace("Segment based token, Time used to find entries: {}", (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        updateDeleteStateForMessages(messageEntries);
        logger.trace("Segment based token, Time used to update delete state: {}",
            (time.milliseconds() - startTimeInMs));

        startTimeInMs = time.milliseconds();
        eliminateDuplicates(messageEntries);
        logger.trace("Segment based token, Time used to eliminate duplicates: {}",
            (time.milliseconds() - startTimeInMs));

        long totalBytesRead = getTotalBytesRead(newToken, messageEntries, logEndOffsetBeforeFind);
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

  private long getTotalBytesRead(StoreFindToken newToken, List<MessageInfo> messageEntries,
      Offset logEndOffsetBeforeFind) {
    long bytesRead = 0;
    if (newToken.getType().equals(StoreFindToken.Type.IndexBased)) {
      bytesRead = log.getDifference(newToken.getOffset(), logAbsoluteZeroOffset);
    } else if (newToken.getType().equals(StoreFindToken.Type.JournalBased)) {
      if (messageEntries.size() > 0) {
        MessageInfo lastMsgInfo = messageEntries.get(messageEntries.size() - 1);
        Offset offsetInToken = newToken.getOffset();
        Offset endOffset = new Offset(offsetInToken.getName(), offsetInToken.getOffset() + lastMsgInfo.getSize());
        bytesRead = log.getDifference(endOffset, logAbsoluteZeroOffset);
      } else {
        bytesRead = log.getDifference(logEndOffsetBeforeFind, logAbsoluteZeroOffset);
      }
    }
    return bytesRead;
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
   * @return A token representing the position in the segment/journal up to which entries have been read and returned.
   */
  private StoreFindToken findEntriesFromSegmentStartOffset(Offset initialSegmentStartOffset, StoreKey key,
      List<MessageInfo> messageEntries, FindEntriesCondition findEntriesCondition) throws IOException, StoreException {
    Offset segmentStartOffset = initialSegmentStartOffset;
    if (segmentStartOffset.equals(indexes.lastKey())) {
      // We would never have given away a token with a segmentStartOffset of the latest segment.
      throw new IllegalArgumentException(
          "Index : " + dataDir + " findEntriesFromOffset segment start offset " + segmentStartOffset
              + " is of the last segment");
    }

    Offset newTokenSegmentStartOffset = null;
    Offset newTokenOffsetInJournal = null;

    IndexSegment segmentToProcess = indexes.get(segmentStartOffset);
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
      segmentStartOffset = indexes.higherKey(segmentStartOffset);
      segmentToProcess = indexes.get(segmentStartOffset);
    }

    while (findEntriesCondition.proceed(currentTotalSizeOfEntries.get(), segmentToProcess.getLastModifiedTime())) {
      // Check in the journal to see if we are already at an offset in the journal, if so get entries from it.
      Offset journalFirstOffsetBeforeCheck = journal.getFirstOffset();
      Offset journalLastOffsetBeforeCheck = journal.getLastOffset();
      List<JournalEntry> entries = journal.getEntriesSince(segmentStartOffset, true);
      if (entries != null) {
        logger.trace("Index : " + dataDir + " findEntriesFromOffset journal offset " + segmentStartOffset
            + " total entries received " + entries.size());
        IndexSegment currentSegment = segmentToProcess;
        for (JournalEntry entry : entries) {
          if (entry.getOffset().compareTo(currentSegment.getEndOffset()) > 0) {
            /* The offset is of the next segment. If the next segment's last modified time makes
            it ineligible, skip */
            Offset nextSegmentStartOffset = indexes.higherKey(currentSegment.getStartOffset());
            currentSegment = indexes.get(nextSegmentStartOffset);
            if (!findEntriesCondition.proceed(currentTotalSizeOfEntries.get(), currentSegment.getLastModifiedTime())) {
              break;
            }
          }
          newTokenOffsetInJournal = entry.getOffset();
          IndexValue value = findKey(entry.getKey());
          messageEntries.add(
              new MessageInfo(entry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
                  value.getExpiresAtMs()));
          currentTotalSizeOfEntries.addAndGet(value.getSize());
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntries.get(), currentSegment.getLastModifiedTime())) {
            break;
          }
        }
        break; // we have entered and finished reading from the journal, so we are done.
      }

      if (segmentStartOffset == indexes.lastKey()) {
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
        segmentStartOffset = indexes.higherKey(segmentStartOffset);
        segmentToProcess = indexes.get(segmentStartOffset);
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
        IndexValue indexValue = findKey(messageInfo.getStoreKey());
        messageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(),
            indexValue.isFlagSet(IndexValue.Flags.Delete_Index), messageInfo.getExpirationTimeInMs());
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
    persistor.write();
    try {
      hardDeleter.shutdown();
    } catch (Exception e) {
      logger.error("Index : " + dataDir + " error while persisting cleanup token ", e);
    }
    File cleanShutdownFile = new File(dataDir, CLEAN_SHUTDOWN_FILENAME);
    try {
      cleanShutdownFile.createNewFile();
    } catch (IOException e) {
      logger.error("Index : " + dataDir + " error while creating clean shutdown file ", e);
    }
  }

  /**
   * Returns the current end offset that the index represents in the log
   * @return The end offset in the log that this index currently represents
   */
  Offset getCurrentEndOffset() {
    return indexes.size() == 0 ? log.getStartOffset() : indexes.lastEntry().getValue().getEndOffset();
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
            new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds));
        if (newToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
          newToken = storeToken;
        }
      } else {
        // journal based or empty
        Offset offsetToStart = storeToken.getOffset();
        boolean inclusive = false;
        if (storeToken.getType().equals(StoreFindToken.Type.Uninitialized)) {
          offsetToStart = log.getStartOffset();
          inclusive = true;
        }
        List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, inclusive);

        Offset offsetEnd = offsetToStart;
        if (entries != null) {
          // Case 2: offset based, and offset still in journal
          IndexSegment currentSegment = indexes.floorEntry(offsetToStart).getValue();
          long currentTotalSizeOfEntries = 0;
          for (JournalEntry entry : entries) {
            if (entry.getOffset().compareTo(currentSegment.getEndOffset()) > 0) {
              Offset nextSegmentStartOffset = indexes.higherKey(currentSegment.getStartOffset());
              currentSegment = indexes.get(nextSegmentStartOffset);
            }
            if (endTimeSeconds < currentSegment.getLastModifiedTime()) {
              break;
            }

            IndexValue value = findKey(entry.getKey());
            if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
              messageEntries.add(new MessageInfo(entry.getKey(), value.getSize(), true, value.getExpiresAtMs()));
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
          Map.Entry<Offset, IndexSegment> entry = indexes.floorEntry(offsetToStart);
          if (entry != null && entry.getKey() != indexes.lastKey()) {
            newToken = findEntriesFromSegmentStartOffset(entry.getKey(), null, messageEntries,
                new FindEntriesCondition(maxTotalSizeOfEntries, endTimeSeconds));
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
        if (indexes.size() > 0) {
          // before iterating the map, get the current file end pointer
          Map.Entry<Offset, IndexSegment> lastEntry = indexes.lastEntry();
          IndexSegment currentInfo = lastEntry.getValue();
          Offset indexEndOffsetBeforeFlush = currentInfo.getEndOffset();
          Offset logEndOffsetBeforeFlush = log.getEndOffset();
          if (logEndOffsetBeforeFlush.compareTo(indexEndOffsetBeforeFlush) < 0) {
            throw new StoreException("LogEndOffset " + logEndOffsetBeforeFlush + " before flush cannot be less than "
                + "currentEndOffSet of index " + indexEndOffsetBeforeFlush, StoreErrorCodes.Illegal_Index_State);
          }

          hardDeleter.preLogFlush();

          // flush the log to ensure everything till the fileEndPointerBeforeFlush is flushed
          log.flush();

          hardDeleter.postLogFlush();

          IndexSegment prevInfo = indexes.size() > 1 ? indexes.lowerEntry(lastEntry.getKey()).getValue() : null;
          Offset currentLogEndPointer = log.getEndOffset();
          while (prevInfo != null && !prevInfo.isMapped()) {
            if (prevInfo.getEndOffset().compareTo(currentLogEndPointer) > 0) {
              String message = "The read only index cannot have a file end pointer " + prevInfo.getEndOffset()
                  + " greater than the log end offset " + currentLogEndPointer;
              throw new StoreException(message, StoreErrorCodes.IOError);
            }
            logger.trace("Index : " + dataDir + " writing prev index with end offset " + prevInfo.getEndOffset());
            prevInfo.writeIndexSegmentToFile(prevInfo.getEndOffset());
            prevInfo.map(true);
            Map.Entry<Offset, IndexSegment> infoEntry = indexes.lowerEntry(prevInfo.getStartOffset());
            prevInfo = infoEntry != null ? infoEntry.getValue() : null;
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

