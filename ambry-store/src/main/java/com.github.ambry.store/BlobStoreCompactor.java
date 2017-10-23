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
package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Component that removes the "dead" data from the set of provided log segments and reclaims space.
 * <p/>
 * It does this by copying the valid data into swap spaces and atomically replacing the segments being compacted with
 * the swap spaces.
 */
class BlobStoreCompactor {
  static final String INDEX_SEGMENT_READ_JOB_NAME = "blob_store_compactor_index_segment_read";
  static final String TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME = "compactor_clean_shutdown";
  static final String TEMP_LOG_SEGMENT_NAME_SUFFIX = BlobStore.SEPARATOR + "temp";
  static final FilenameFilter TEMP_LOG_SEGMENTS_FILTER = new FilenameFilter() {
    private final String SUFFIX = LogSegmentNameHelper.SUFFIX + TEMP_LOG_SEGMENT_NAME_SUFFIX;

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(SUFFIX);
    }
  };

  private static final long WAIT_TIME_FOR_CLEANUP_MS = 5 * Time.MsPerSec;

  private final File dataDir;
  private final String storeId;
  private final StoreKeyFactory storeKeyFactory;
  private final StoreConfig config;
  private final StoreMetrics srcMetrics;
  private final StoreMetrics tgtMetrics;
  private final Log srcLog;
  private final DiskIOScheduler diskIOScheduler;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final Time time;
  private final UUID sessionId;
  private final UUID incarnationId;
  private final AtomicBoolean compactionInProgress = new AtomicBoolean(false);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private volatile boolean isActive = false;
  private PersistentIndex srcIndex;

  private Log tgtLog;
  private PersistentIndex tgtIndex;
  private long numSwapsUsed;
  private StoreFindToken recoveryStartToken = null;
  private CompactionLog compactionLog;
  private volatile CountDownLatch runningLatch = new CountDownLatch(0);

  /**
   * Constructs the compactor component.
   * @param dataDir the directory where all compactions need to be run.
   * @param storeId the unique ID of the store.
   * @param storeKeyFactory the {@link StoreKeyFactory} to generate {@link StoreKey} instances.
   * @param config the {@link StoreConfig} that defines configuration parameters.
   * @param srcMetrics the {@link StoreMetrics} to use to record metrics for the compactor.
   * @param tgtMetrics the {@link StoreMetrics} to use to record metrics for the temporarily created log and index.
   * @param diskIOScheduler the {@link DiskIOScheduler} to schedule I/O.
   * @param srcLog the {@link Log} to copy data from.
   * @param time the {@link Time} instance to use.
   * @param sessionId the sessionID of the store.
   * @param incarnationId the incarnation ID of the store.
   * @throws IOException if the {@link CompactionLog} could not be created or if commit/cleanup failed during recovery.
   * @throws StoreException if the commit failed during recovery.
   */
  BlobStoreCompactor(String dataDir, String storeId, StoreKeyFactory storeKeyFactory, StoreConfig config,
      StoreMetrics srcMetrics, StoreMetrics tgtMetrics, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, Log srcLog, Time time, UUID sessionId, UUID incarnationId)
      throws IOException, StoreException {
    this.dataDir = new File(dataDir);
    this.storeId = storeId;
    this.storeKeyFactory = storeKeyFactory;
    this.config = config;
    this.srcMetrics = srcMetrics;
    this.tgtMetrics = tgtMetrics;
    this.srcLog = srcLog;
    this.diskIOScheduler = diskIOScheduler;
    this.diskSpaceAllocator = diskSpaceAllocator;
    this.time = time;
    this.sessionId = sessionId;
    this.incarnationId = incarnationId;
    fixStateIfRequired();
    logger.trace("Constructed BlobStoreCompactor for {}", dataDir);
  }

  /**
   * Initializes the compactor and sets the {@link PersistentIndex} to copy data from.
   * @param srcIndex the {@link PersistentIndex} to copy data from.
   */
  void initialize(PersistentIndex srcIndex) {
    this.srcIndex = srcIndex;
    if (compactionLog == null && srcIndex.hardDeleter != null && srcIndex.hardDeleter.isPaused()) {
      logger.debug("Resuming hard delete in {} during compactor initialize because there is no compaction in progress",
          storeId);
      srcIndex.hardDeleter.resume();
    }
    isActive = true;
    srcMetrics.initializeCompactorGauges(storeId, compactionInProgress);
    logger.trace("Initialized BlobStoreCompactor for {}", storeId);
  }

  /**
   * Closes the compactor and waits for {@code waitTimeSecs} for the close to complete.
   * @param waitTimeSecs the number of seconds to wait for close to complete.
   * @throws InterruptedException if the wait for close was interrupted.
   */
  void close(long waitTimeSecs) throws InterruptedException {
    isActive = false;
    if (waitTimeSecs > 0 && !runningLatch.await(waitTimeSecs, TimeUnit.SECONDS)) {
      logger.error("Compactor did not shutdown within {} seconds for {}", waitTimeSecs, storeId);
    }
    logger.trace("Closed BlobStoreCompactor for {}", storeId);
  }

  /**
   * Compacts the store by copying valid data from the log segments in {@code details} to swap spaces and switching
   * the compacted segments out for the swap spaces. Returns once the compaction is complete.
   * @param details the {@link CompactionDetails} for the compaction.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
   * in the segments to compact are in the journal.
   * @throws IllegalStateException if the compactor has not been initialized.
   * @throws IOException if there were I/O errors creating the {@link CompactionLog}
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  void compact(CompactionDetails details) throws IOException, StoreException {
    if (srcIndex == null) {
      throw new IllegalStateException("Compactor has not been initialized");
    } else if (compactionLog != null) {
      throw new IllegalStateException("There is already a compaction in progress");
    }
    checkSanity(details);
    logger.info("Compaction of {} started with details {}", storeId, details);
    compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, time, details);
    resumeCompaction();
  }

  /**
   * Resumes compaction from where it was left off.
   * @throws IllegalStateException if the compactor has not been initialized or if there is no compaction to resume.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  void resumeCompaction() throws StoreException {
    if (srcIndex == null) {
      throw new IllegalStateException("Compactor has not been initialized");
    } else if (compactionLog == null) {
      throw new IllegalStateException("There is no compaction to resume");
    }
    /*
    A single compaction job could be performed across multiple compaction cycles (if there aren't enough swap spaces to
    complete the job in one cycle). Therefore, we loop and perform PREPARE, COPY, COMMIT and CLEANUP until the
    CompactionLog reports the job as DONE.
    Short description of each of the phases:
    1. PREPARE - Initial state of a compaction cycle. Signifies that the cycle has just started.
    2. COPY - Valid data from the segments under compaction is copied over to the swap spaces. If all the
    data cannot be copied, it auto splits the compaction job into two cycles.
    3. COMMIT - Adds the new log segments to the application log and atomically updates the set of index segments
    4. CLEANUP - Cleans up the old log segments and their index files
     */

    logger.trace("resumeCompaction() started for {}", storeId);
    runningLatch = new CountDownLatch(1);
    compactionInProgress.set(true);
    try {
      while (isActive && !compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
        CompactionLog.Phase phase = compactionLog.getCompactionPhase();
        logger.debug("Starting cycle to compact {} in {}", compactionLog.getCompactionDetails(), storeId);
        switch (phase) {
          case PREPARE:
            logger.debug("Compaction PREPARE started for {} in {}", compactionLog.getCompactionDetails(), storeId);
            compactionLog.markCopyStart();
            // fall through to COPY
          case COPY:
            logger.debug("Compaction COPY started for {} in {}", compactionLog.getCompactionDetails(), storeId);
            copy();
            if (isActive) {
              compactionLog.markCommitStart();
              logger.debug("Compaction COMMIT started for {} in {}", compactionLog.getCompactionDetails(), storeId);
              commit(false);
              compactionLog.markCleanupStart();
              logger.debug("Compaction CLEANUP started for {} in {}", compactionLog.getCompactionDetails(), storeId);
              waitTillRefCountZero();
              logger.debug("Wait for ref count to go to 0 has been finished {} in {}",
                  compactionLog.getCompactionDetails(), storeId);
              cleanup(false);
              logger.debug("Completing for {} in {}", compactionLog.getCompactionDetails(), storeId);
              compactionLog.markCycleComplete();
            }
            break;
          default:
            throw new IllegalStateException("Illegal compaction phase: " + phase);
        }
      }
      endCompaction();
    } catch (InterruptedException | IOException e) {
      throw new StoreException("Exception during compaction", e, StoreErrorCodes.Unknown_Error);
    } finally {
      compactionInProgress.set(false);
      runningLatch.countDown();
      logger.trace("resumeCompaction() ended for {}", storeId);
    }
  }

  /**
   * @return the number of temporary log segment files this compactor is currently using.
   */
  int getSwapSegmentsInUse() throws StoreException {
    String[] tempSegments = dataDir.list(TEMP_LOG_SEGMENTS_FILTER);
    if (tempSegments == null) {
      throw new StoreException("Error occured while listing files in data dir:" + dataDir.getAbsolutePath(),
          StoreErrorCodes.IOError);
    }
    return tempSegments.length;
  }

  /**
   * If a compaction was in progress during a crash/shutdown, fixes the state so that the store is loaded correctly
   * and compaction can resume smoothly on a call to {@link #resumeCompaction()}. Expected to be called before the
   * {@link PersistentIndex} is instantiated in the {@link BlobStore}.
   * @throws IOException if the {@link CompactionLog} could not be created or if commit or cleanup failed.
   * @throws StoreException if the commit failed.
   */
  private void fixStateIfRequired() throws IOException, StoreException {
    if (CompactionLog.isCompactionInProgress(dataDir.getAbsolutePath(), storeId)) {
      compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, storeKeyFactory, time);
      CompactionLog.Phase phase = compactionLog.getCompactionPhase();
      logger.info("Fixing compaction state for {} (phase is {})", storeId, phase);
      switch (phase) {
        case COMMIT:
          commit(true);
          compactionLog.markCleanupStart();
          logger.info("Finished compaction COMMIT for {} in {}", compactionLog.getCompactionDetails(), storeId);
          // fall through to CLEANUP
        case CLEANUP:
          cleanup(true);
          logger.info("Completing compaction cycle for {} in {}", compactionLog.getCompactionDetails(), storeId);
          compactionLog.markCycleComplete();
          srcMetrics.compactionFixStateCount.inc();
          break;
        case COPY:
          srcMetrics.compactionFixStateCount.inc();
          recoveryStartToken = compactionLog.getSafeToken();
          logger.info("Compaction recovery start token for {} in {} set to {}", compactionLog.getCompactionDetails(),
              storeId, recoveryStartToken);
          // fall through to the break.
        case PREPARE:
        case DONE:
          break;
        default:
          throw new IllegalStateException("Unrecognized compaction phase: " + phase);
      }
      if (compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
        endCompaction();
      }
    }
  }

  /**
   * Checks the sanity of the provided {@code details}
   * @param details the {@link CompactionDetails} to use for compaction.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
   * in the segments to compact are in the journal.
   */
  private void checkSanity(CompactionDetails details) {
    List<String> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    // all segments should be available and should not have anything in the journal
    // segments should be in order
    String prevSegmentName = null;
    for (String segmentName : segmentsUnderCompaction) {
      LogSegment segment = srcLog.getSegment(segmentName);
      if (segment == null) {
        throw new IllegalArgumentException(segmentName + " does not exist in the log");
      }
      if (prevSegmentName != null && LogSegmentNameHelper.COMPARATOR.compare(prevSegmentName, segmentName) >= 0) {
        throw new IllegalArgumentException("Ordering of " + segmentsUnderCompaction + " is incorrect");
      }
      // should be outside the range of the journal
      Offset segmentEndOffset = new Offset(segment.getName(), segment.getEndOffset());
      if (segmentEndOffset.compareTo(srcIndex.journal.getFirstOffset()) >= 0) {
        throw new IllegalArgumentException("Some of the offsets provided for compaction are within the journal");
      }
      prevSegmentName = segment.getName();
    }
  }

  /**
   * Copies valid data from the source log segments into the swap spaces.
   * <p/>
   * Splits the compaction into two cycles if there aren't enough swap spaces and completes the copy for the current
   * cycle.
   * @throws InterruptedException if the compaction was interrupted
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void copy() throws InterruptedException, IOException, StoreException {
    setupState();
    List<String> logSegmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
    FileSpan duplicateSearchSpan = null;
    if (compactionLog.getCurrentIdx() > 0) {
      // only records in the the very first log segment in the cycle could have been copied in a previous cycle
      LogSegment firstLogSegment = srcLog.getSegment(logSegmentsUnderCompaction.get(0));
      LogSegment prevSegment = srcLog.getPrevSegment(firstLogSegment);
      if (prevSegment != null) {
        // duplicate data, if it exists, can only be in the log segment just before the first log segment in the cycle.
        Offset startOffset = new Offset(prevSegment.getName(), prevSegment.getStartOffset());
        Offset endOffset = new Offset(prevSegment.getName(), prevSegment.getEndOffset());
        duplicateSearchSpan = new FileSpan(startOffset, endOffset);
      }
      logger.trace("Duplicate search span is {} for {}", duplicateSearchSpan, storeId);
    }

    for (String logSegmentName : logSegmentsUnderCompaction) {
      logger.debug("Processing {} in {}", logSegmentName, storeId);
      LogSegment srcLogSegment = srcLog.getSegment(logSegmentName);
      Offset logSegmentEndOffset = new Offset(srcLogSegment.getName(), srcLogSegment.getEndOffset());
      if (needsCopying(logSegmentEndOffset) && !copyDataByLogSegment(srcLogSegment, duplicateSearchSpan)) {
        if (isActive) {
          // split the cycle only if there is no shutdown in progress
          logger.debug("Splitting current cycle with segments {} at {} for {}", logSegmentsUnderCompaction,
              logSegmentName, storeId);
          compactionLog.splitCurrentCycle(logSegmentName);
        }
        break;
      }
      duplicateSearchSpan = null;
    }

    numSwapsUsed = tgtIndex.getLogSegmentCount();
    logger.debug("Swaps used to copy {} is {} for {}", compactionLog.getCompactionDetails(), numSwapsUsed, storeId);
    if (isActive) {
      // it is possible to double count based on the time at which shutdown occurs (if it occurs b/w this statement
      // and before the subsequent commit can take effect)
      long segmentCountDiff =
          compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction().size() - numSwapsUsed;
      long savedBytes = srcLog.getSegmentCapacity() * segmentCountDiff;
      srcMetrics.compactionBytesReclaimedCount.inc(savedBytes);
    }
    tgtIndex.close();
    tgtLog.close();
    // persist the bloom of the "latest" index segment if it exists
    if (numSwapsUsed > 0) {
      tgtIndex.getIndexSegments().lastEntry().getValue().map(true);
    } else {
      // there were no valid entries copied, return any temp segments back to the pool
      logger.trace("Cleaning up temp segments in {} because no swap spaces were used", storeId);
      cleanupUnusedTempSegments();
    }
  }

  /**
   * Commits the changes made during {@link CompactionLog.Phase#COPY}. Commit involves
   * 1. Renaming temporary log segments. This constitutes the on-disk commit. If the server were to crash after this
   * step, the restart will remove all trace of the old log and index segments.
   * 2. Adding them to the log segments maintained by the application log.
   * 3. Atomically switching the old set of index segments for the new ones (if not recovering).
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were I/O errors during committing.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void commit(boolean recovering) throws IOException, StoreException {
    List<String> logSegmentNames = getTargetLogSegmentNames();
    logger.debug("Target log segments are {} for {}", logSegmentNames, storeId);
    renameLogSegments(logSegmentNames);
    addNewLogSegmentsToSrcLog(logSegmentNames, recovering);
    if (!recovering) {
      updateSrcIndex(logSegmentNames);
    }
  }

  /**
   * Cleans up after the commit is complete. Cleaning up involves:
   * 1. Dropping the log segments that are no longer relevant.
   * 2. Deleting all associated index segment and bloom files.
   * 3. Deleting the clean shutdown file associated with the index of the swap spaces.
   * 4. Resetting state.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were I/O errors during cleanup.
   */
  private void cleanup(boolean recovering) throws IOException {
    cleanupLogAndIndexSegments(recovering);
    File cleanShutdownFile = new File(dataDir, TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME);
    if (cleanShutdownFile.exists() && !cleanShutdownFile.delete()) {
      logger.warn("Could not delete the clean shutdown file {}", cleanShutdownFile);
    }
    resetStructures();
  }

  // copy() helpers

  /**
   * Sets up the state required for copying data by populating all the data structures, creating a log and index that
   * wraps the swap spaces and pausing hard delete in the application log.
   * @throws InterruptedException if hard delete could not be paused.
   * @throws IOException if logs and indexes could not be set up.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void setupState() throws InterruptedException, IOException, StoreException {
    CompactionDetails details = compactionLog.getCompactionDetails();
    long highestGeneration = 0;
    for (String segmentName : details.getLogSegmentsUnderCompaction()) {
      highestGeneration = Math.max(highestGeneration, LogSegmentNameHelper.getGeneration(segmentName));
    }
    logger.debug("Generation of target segments will be {} for {} in {}", highestGeneration + 1, details, storeId);
    List<Pair<String, String>> targetSegmentNamesAndFilenames = new ArrayList<>();
    List<LogSegment> existingTargetLogSegments = new ArrayList<>();
    for (String segmentName : details.getLogSegmentsUnderCompaction()) {
      long pos = LogSegmentNameHelper.getPosition(segmentName);
      String targetSegmentName = LogSegmentNameHelper.getName(pos, highestGeneration + 1);
      String targetSegmentFileName =
          LogSegmentNameHelper.nameToFilename(targetSegmentName) + TEMP_LOG_SEGMENT_NAME_SUFFIX;
      File targetSegmentFile = new File(dataDir, targetSegmentFileName);
      if (targetSegmentFile.exists()) {
        existingTargetLogSegments.add(new LogSegment(targetSegmentName, targetSegmentFile, tgtMetrics));
      } else {
        targetSegmentNamesAndFilenames.add(new Pair<>(targetSegmentName, targetSegmentFileName));
      }
    }
    // TODO: available swap space count should be obtained from DiskManager. For now, assumed to be 1.
    long targetLogTotalCapacity = srcLog.getSegmentCapacity();
    logger.debug("Target log capacity is {} for {}. Existing log segments are {}. Future names and files are {}",
        targetLogTotalCapacity, storeId, existingTargetLogSegments, targetSegmentNamesAndFilenames);
    tgtLog = new Log(dataDir.getAbsolutePath(), targetLogTotalCapacity, srcLog.getSegmentCapacity(), diskSpaceAllocator,
        tgtMetrics, true, existingTargetLogSegments, targetSegmentNamesAndFilenames.iterator());
    Journal journal = new Journal(dataDir.getAbsolutePath(), 2 * config.storeIndexMaxNumberOfInmemElements,
        config.storeMaxNumberOfEntriesToReturnFromJournal);
    tgtIndex =
        new PersistentIndex(dataDir.getAbsolutePath(), storeId, null, tgtLog, config, storeKeyFactory, null, null,
            diskIOScheduler, tgtMetrics, journal, time, sessionId, incarnationId,
            TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME);
    if (srcIndex.hardDeleter != null && !srcIndex.hardDeleter.isPaused()) {
      logger.debug("Pausing hard delete for {}", storeId);
      srcIndex.hardDeleter.pause();
    }
  }

  /**
   * Determines if the offset has been copied or requires copying.
   * @param offset the {@link Offset} to test at.
   * @return {@code true} if copying is required. {@code false} otherwise.
   */
  private boolean needsCopying(Offset offset) {
    return recoveryStartToken == null || recoveryStartToken.getOffset().compareTo(offset) < 0;
  }

  /**
   * @return the start {@link Offset} of the index segment up until which delete records are considered applicable. Any
   * delete records of blobs in subsequent index segments do not count and the blob is considered as not deleted.
   * <p/>
   * Returns {@code null} if none of the delete records are considered applicable.
   */
  private Offset getStartOffsetOfLastIndexSegmentForDeleteCheck() {
    // TODO: move this to BlobStoreStats
    Offset cutoffOffset = compactionLog.getStartOffsetOfLastIndexSegmentForDeleteCheck();
    if (cutoffOffset == null || !srcIndex.getIndexSegments().containsKey(cutoffOffset)) {
      long referenceTimeMs = compactionLog.getCompactionDetails().getReferenceTimeMs();
      for (IndexSegment indexSegment : srcIndex.getIndexSegments().descendingMap().values()) {
        if (indexSegment.getLastModifiedTimeMs() < referenceTimeMs) {
          // NOTE: using start offset here because of the way FileSpan is treated in PersistentIndex.findKey().
          // using this as the end offset for delete includes the whole index segment in the search.
          cutoffOffset = indexSegment.getStartOffset();
          break;
        }
      }
      if (cutoffOffset != null) {
        compactionLog.setStartOffsetOfLastIndexSegmentForDeleteCheck(cutoffOffset);
        logger.info("Start offset of last index segment for delete check is {} for {}", cutoffOffset, storeId);
      }
    }
    return cutoffOffset;
  }

  /**
   * Copies data from the provided log segment into the target log (swap spaces).
   * @param logSegmentToCopy the {@link LogSegment} to copy from.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates.
   * @return {@code true} if all the records in the log segment were copied. {@code false} if some records were not
   * copied either because there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyDataByLogSegment(LogSegment logSegmentToCopy, FileSpan duplicateSearchSpan)
      throws IOException, StoreException {
    logger.info("Copying data from {}", logSegmentToCopy);
    for (Offset indexSegmentStartOffset : getIndexSegmentDetails(logSegmentToCopy.getName()).keySet()) {
      IndexSegment indexSegmentToCopy = srcIndex.getIndexSegments().get(indexSegmentStartOffset);
      logger.info("Processing index segment {}", indexSegmentToCopy.getFile());
      if (needsCopying(indexSegmentToCopy.getEndOffset()) && !copyDataByIndexSegment(logSegmentToCopy,
          indexSegmentToCopy, duplicateSearchSpan)) {
        // there is a shutdown in progress or there was no space to copy all entries.
        logger.info("Did not copy all entries in {} (either because there is no space or there is a shutdown)",
            indexSegmentToCopy.getFile());
        return false;
      }
    }
    return true;
  }

  /**
   * Copies data in the provided {@code indexSegmentToCopy} into the target log (swap spaces).
   * @param logSegmentToCopy the {@link LogSegment} to copy from.
   * @param indexSegmentToCopy the {@link IndexSegment} that contains the entries that need to be copied.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates.
   * @return {@code true} if all the records in the index segment were copied. {@code false} if some records were not
   * copied either because there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyDataByIndexSegment(LogSegment logSegmentToCopy, IndexSegment indexSegmentToCopy,
      FileSpan duplicateSearchSpan) throws IOException, StoreException {
    logger.debug("Copying data from {}", indexSegmentToCopy.getFile());
    List<IndexEntry> allIndexEntries = new ArrayList<>();
    // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
    diskIOScheduler.getSlice(INDEX_SEGMENT_READ_JOB_NAME, INDEX_SEGMENT_READ_JOB_NAME, 1);
    // get all entries
    indexSegmentToCopy.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), allIndexEntries,
        new AtomicLong(0));

    // save a token for restart (the key gets ignored but is required to be non null for construction)
    StoreFindToken safeToken =
        new StoreFindToken(allIndexEntries.get(0).getKey(), indexSegmentToCopy.getStartOffset(), sessionId,
            incarnationId);
    compactionLog.setSafeToken(safeToken);
    logger.debug("Set safe token for compaction in {} to {}", storeId, safeToken);

    boolean checkAlreadyCopied =
        recoveryStartToken != null && recoveryStartToken.getOffset().equals(indexSegmentToCopy.getStartOffset());
    logger.trace("Should check already copied for {}: {} ", indexSegmentToCopy.getFile(), checkAlreadyCopied);

    // filter deleted/expired entries and get index entries for all the PUT and DELETE records
    List<IndexEntry> indexEntriesToCopy =
        getIndexEntriesToCopy(allIndexEntries, duplicateSearchSpan, indexSegmentToCopy.getStartOffset(),
            checkAlreadyCopied);
    logger.debug("{} entries need to be copied in {}", indexEntriesToCopy.size(), indexSegmentToCopy.getFile());

    // Copy these over
    boolean copiedAll = copyRecords(logSegmentToCopy, indexEntriesToCopy, indexSegmentToCopy.getLastModifiedTimeSecs());
    // persist
    tgtIndex.persistIndex();
    return copiedAll;
  }

  /**
   * Gets all the index entries that need to be copied. The index entries that will be copied are:
   * 1. Records that are not duplicates.
   * 2. Records that are not expired or deleted.
   * 3. Delete records.
   * Records will be excluded because they are expired/deleted or because they are duplicates.
   * @param allIndexEntries the {@link MessageInfo} of all the entries in the index segment.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates
   * @param indexSegmentStartOffset the start {@link Offset} of the index segment under consideration.
   * @param checkAlreadyCopied {@code true} if a check for existence in the swap spaces has to be executed (due to
   *                                       crash/shutdown), {@code false} otherwise.
   * @return the list of valid {@link IndexEntry} sorted by their offset.
   * @throws IllegalStateException if there is a mismatch b/w the index value expected and the index value obtained.
   * @throws StoreException if there is any problem with using the index.
   */
  private List<IndexEntry> getIndexEntriesToCopy(List<IndexEntry> allIndexEntries, FileSpan duplicateSearchSpan,
      Offset indexSegmentStartOffset, boolean checkAlreadyCopied) throws StoreException {
    List<IndexEntry> indexEntriesToCopy = new ArrayList<>();
    List<IndexEntry> copyCandidates = getValidIndexEntries(indexSegmentStartOffset, allIndexEntries);
    for (IndexEntry copyCandidate : copyCandidates) {
      IndexValue copyCandidateValue = copyCandidate.getValue();
      // search for duplicates in srcIndex if required
      if (duplicateSearchSpan != null) {
        IndexValue possibleDuplicate = srcIndex.findKey(copyCandidate.getKey(), duplicateSearchSpan);
        if (possibleDuplicate != null) {
          if (possibleDuplicate.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // copyCandidate is surely a duplicate because srcIndex contains a DELETE index entry
            logger.trace("Found a duplicate IndexValue {} for {} in segment with start offset {} in {}",
                possibleDuplicate, copyCandidateValue, indexSegmentStartOffset, storeId);
            continue;
          } else if (!copyCandidateValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // copyCandidate is a duplicate because it is a PUT entry and so is the possible duplicate.
            logger.trace("Found a duplicate IndexValue {} for {} in segment with start offset {} in {}",
                possibleDuplicate, copyCandidateValue, indexSegmentStartOffset, storeId);
            continue;
          }
        }
      }
      // search for duplicates in tgtIndex if required
      if (!checkAlreadyCopied || !alreadyExistsInTgt(copyCandidate.getKey(), copyCandidateValue)) {
        logger.trace(
            "Adding index entry {} in index segment with start offset {} in {} because it is valid and does not already"
                + " exist", copyCandidate, indexSegmentStartOffset, storeId);
        indexEntriesToCopy.add(copyCandidate);
      }
    }
    // order by offset in log.
    Collections.sort(indexEntriesToCopy, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    logger.debug("Out of {} entries, {} are valid and {} will be copied in this round", allIndexEntries.size(),
        copyCandidates.size(), indexEntriesToCopy.size());
    logger.trace(
        "For index segment with start offset {} in {} - Total index entries: {}. Valid index entries: {}. Entries to "
            + "copy: {}", indexSegmentStartOffset, storeId, allIndexEntries, copyCandidates, indexEntriesToCopy);
    return indexEntriesToCopy;
  }

  /**
   * Gets all the valid index entries in the given list of index entries.
   * @param indexSegmentStartOffset the start {@link Offset} of the {@link IndexSegment} that {@code allIndexEntries} is
   *                                from.
   * @param allIndexEntries the list of {@link IndexEntry} instances from which the valid entries have to be chosen.
   * @return the list of valid entries picked from {@code allIndexEntries}.
   * @throws StoreException if {@link BlobReadOptions} could not be obtained from the store for deleted blobs.
   */
  private List<IndexEntry> getValidIndexEntries(Offset indexSegmentStartOffset, List<IndexEntry> allIndexEntries)
      throws StoreException {
    // TODO: move this blob store stats
    Offset startOffsetOfLastIndexSegmentForDeleteCheck = getStartOffsetOfLastIndexSegmentForDeleteCheck();
    boolean deletesInEffect = startOffsetOfLastIndexSegmentForDeleteCheck != null
        && indexSegmentStartOffset.compareTo(startOffsetOfLastIndexSegmentForDeleteCheck) <= 0;
    logger.trace("Deletes in effect is {} for index segment with start offset {} in {}", deletesInEffect,
        indexSegmentStartOffset, storeId);
    List<IndexEntry> validEntries = new ArrayList<>();
    for (IndexEntry indexEntry : allIndexEntries) {
      IndexValue value = indexEntry.getValue();
      if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // DELETE entry. Always valid.
        validEntries.add(indexEntry);
        // if this delete cannot be counted and there is a corresponding unexpired PUT entry in the same index segment,
        // we will need to add it.
        // NOTE: In PersistentIndex.markAsDeleted(), the expiry time of the put value is copied into the delete value.
        // So it is safe to check for isExpired() on the delete value.
        if (!deletesInEffect && !srcIndex.isExpired(value)) {
          logger.trace("Fetching the PUT entry of a deleted blob with entry {} in index segment with start offset {} in"
              + " {} because it needs to be retained", indexEntry, indexSegmentStartOffset, storeId);
          long putRecordOffset = value.getOriginalMessageOffset();
          if (putRecordOffset != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET && putRecordOffset != value.getOffset()
              .getOffset() && indexSegmentStartOffset.getOffset() <= putRecordOffset) {
            try (BlobReadOptions options = srcIndex.getBlobReadInfo(indexEntry.getKey(),
                EnumSet.allOf(StoreGetOptions.class))) {
              Offset offset = new Offset(indexSegmentStartOffset.getName(), options.getOffset());
              MessageInfo info = options.getMessageInfo();
              IndexValue putValue =
                  new IndexValue(info.getSize(), offset, info.getExpirationTimeInMs(), info.getOperationTimeMs(),
                      info.getAccountId(), info.getContainerId());
              validEntries.add(new IndexEntry(indexEntry.getKey(), putValue));
            } catch (StoreException e) {
              logger.error("Fetching PUT index entry of {} in {} failed", indexEntry.getKey(), indexSegmentStartOffset);
            }
          }
        }
      } else if (!srcIndex.isExpired(value)) {
        // unexpired PUT entry.
        if (deletesInEffect) {
          FileSpan deleteSearchSpan =
              new FileSpan(indexSegmentStartOffset, startOffsetOfLastIndexSegmentForDeleteCheck);
          IndexValue searchedValue = srcIndex.findKey(indexEntry.getKey(), deleteSearchSpan);
          if (!searchedValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // PUT entry that has not expired and is not considered deleted.
            validEntries.add(indexEntry);
          } else {
            logger.trace("Skipping {} in index segment with start offset {} in {} because it is a deleted PUT",
                indexEntry, indexSegmentStartOffset, storeId);
          }
        } else {
          // valid PUT entry
          validEntries.add(indexEntry);
        }
      } else {
        logger.trace("Skipping {} in index segment with start offset {} in {} because it is an expired PUT", indexEntry,
            indexSegmentStartOffset, storeId);
      }
    }
    return validEntries;
  }

  /**
   * Checks if a record already exists in the target log (swap spaces).
   * @param key the {@link StoreKey} of the record.
   * @param srcValue the {@link IndexValue} obtained from the application index.
   * @return {@code true} if the record already exists in the target log, {@code false} otherwise.
   * @throws StoreException if there is any problem with using the index.
   */
  private boolean alreadyExistsInTgt(StoreKey key, IndexValue srcValue) throws StoreException {
    IndexValue tgtValue = tgtIndex.findKey(key);
    return tgtValue != null && (tgtValue.isFlagSet(IndexValue.Flags.Delete_Index) || !srcValue.isFlagSet(
        IndexValue.Flags.Delete_Index));
  }

  /**
   * Copies the given {@code srcIndexEntries} from the given log segment into the swap spaces.
   * @param logSegmentToCopy the {@link LogSegment} to copy from.
   * @param srcIndexEntries the {@link IndexEntry}s to copy.
   * @param lastModifiedTimeSecs the last modified time of the source index segment.
   * @return @code true} if all the records  were copied. {@code false} if some records were not copied either because
   * there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyRecords(LogSegment logSegmentToCopy, List<IndexEntry> srcIndexEntries, long lastModifiedTimeSecs)
      throws IOException, StoreException {
    boolean copiedAll = true;
    long totalCapacity = tgtLog.getCapacityInBytes();
    long writtenLastTime = 0;
    try (FileChannel fileChannel = Utils.openChannel(logSegmentToCopy.getView().getFirst(), false)) {
      for (IndexEntry srcIndexEntry : srcIndexEntries) {
        IndexValue srcValue = srcIndexEntry.getValue();
        long usedCapacity = tgtIndex.getLogUsedCapacity();
        if (isActive && (tgtLog.getCapacityInBytes() - usedCapacity >= srcValue.getSize())) {
          fileChannel.position(srcValue.getOffset().getOffset());
          Offset endOffsetOfLastMessage = tgtLog.getEndOffset();
          // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
          diskIOScheduler.getSlice(DiskManager.CLEANUP_OPS_JOB_NAME, DiskManager.CLEANUP_OPS_JOB_NAME, writtenLastTime);
          tgtLog.appendFrom(fileChannel, srcValue.getSize());
          FileSpan fileSpan = tgtLog.getFileSpanForMessage(endOffsetOfLastMessage, srcValue.getSize());
          if (srcValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
            IndexValue putValue = tgtIndex.findKey(srcIndexEntry.getKey());
            if (putValue != null) {
              tgtIndex.markAsDeleted(srcIndexEntry.getKey(), fileSpan, srcValue.getOperationTimeInMs());
            } else {
              IndexValue tgtValue =
                  new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getExpiresAtMs(),
                      srcValue.getOperationTimeInMs(), srcValue.getAccountId(), srcValue.getContainerId());
              tgtValue.setFlag(IndexValue.Flags.Delete_Index);
              tgtValue.clearOriginalMessageOffset();
              tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
            }
          } else {
            IndexValue tgtValue =
                new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getExpiresAtMs(),
                    srcValue.getOperationTimeInMs(), srcValue.getAccountId(), srcValue.getContainerId());
            tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
          }
          long lastModifiedTimeSecsToSet =
              srcValue.getOperationTimeInMs() != Utils.Infinite_Time ? srcValue.getOperationTimeInMs() / Time.MsPerSec
                  : lastModifiedTimeSecs;
          tgtIndex.getIndexSegments().lastEntry().getValue().setLastModifiedTimeSecs(lastModifiedTimeSecsToSet);
          writtenLastTime = srcValue.getSize();
          srcMetrics.compactionCopyRateInBytes.mark(srcValue.getSize());
        } else if (!isActive) {
          logger.info("Stopping copying in {} because shutdown is in progress", storeId);
          copiedAll = false;
          break;
        } else {
          // this is the extra segment, so it is ok to run out of space.
          logger.info(
              "There is no more capacity in the destination log in {}. Total capacity is {}. Used capacity is {}."
                  + " Segment that was being copied is {}", storeId, totalCapacity, usedCapacity,
              logSegmentToCopy.getName());
          copiedAll = false;
          break;
        }
      }
    } finally {
      logSegmentToCopy.closeView();
    }
    return copiedAll;
  }

  /**
   * Cleans up any unused temporary segments. Can happen only if there were no entries to be copied and all the segments
   * under compaction can be just dropped.
   * @throws StoreException if the directory listing did not succeed or if any of the files could not be deleted.
   */
  private void cleanupUnusedTempSegments() throws StoreException {
    File[] files = dataDir.listFiles(TEMP_LOG_SEGMENTS_FILTER);
    if (files != null) {
      logger.debug("Cleaning up {}", (Object[]) files);
      for (File file : files) {
        // TODO (DiskManager changes): This will actually return the segment to the DiskManager pool.
        if (!file.delete()) {
          throw new StoreException("Could not delete segment file: " + file.getAbsolutePath(),
              StoreErrorCodes.Unknown_Error);
        }
      }
    } else {
      throw new StoreException("Could not list temp files in directory: " + dataDir, StoreErrorCodes.Unknown_Error);
    }
  }

  // commit() helpers

  /**
   * Gets the names of the target log segments that were generated (the swap spaces). Note that this function returns
   * those log segments that already exist and is not useful to determine what should exist and it only returns the ones
   * that are still in their temporary uncommitted state.
   * @return the list of target log segments that exist on the disk.
   */
  private List<String> getTargetLogSegmentNames() {
    List<String> filenames = Arrays.asList(dataDir.list(TEMP_LOG_SEGMENTS_FILTER));
    List<String> names = new ArrayList<>(filenames.size());
    for (String filename : filenames) {
      String transformed = filename.substring(0, filename.length() - TEMP_LOG_SEGMENT_NAME_SUFFIX.length());
      names.add(LogSegmentNameHelper.nameFromFilename(transformed));
    }
    Collections.sort(names, LogSegmentNameHelper.COMPARATOR);
    return names;
  }

  /**
   * Renames the temporary log segment files to remove the temporary file name suffix and effectively commits them on
   * disk.
   * @param logSegmentNames the names of the log segments whose backing files need to be renamed.
   * @throws StoreException if the rename fails.
   */
  private void renameLogSegments(List<String> logSegmentNames) throws StoreException {
    for (String segmentName : logSegmentNames) {
      String filename = LogSegmentNameHelper.nameToFilename(segmentName);
      File from = new File(dataDir, filename + TEMP_LOG_SEGMENT_NAME_SUFFIX);
      File to = new File(dataDir, filename);
      logger.debug("Renaming {} to {}", from, to);
      if (!from.renameTo(to)) {
        // TODO: change error code
        throw new StoreException("Failed to rename " + from + " to " + to, StoreErrorCodes.Unknown_Error);
      }
    }
  }

  /**
   * Adds the log segments with names {@code logSegmentNames} to the application log instance.
   * @param logSegmentNames the names of the log segments to commit.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were any I/O errors creating a log segment
   */
  private void addNewLogSegmentsToSrcLog(List<String> logSegmentNames, boolean recovering) throws IOException {
    logger.debug("Adding {} in {} to the application log", logSegmentNames, storeId);
    for (String logSegmentName : logSegmentNames) {
      File segmentFile = new File(dataDir, LogSegmentNameHelper.nameToFilename(logSegmentName));
      LogSegment segment = new LogSegment(logSegmentName, segmentFile, srcMetrics);
      srcLog.addSegment(segment, recovering);
    }
  }

  /**
   * Adds all the index segments that refer to log segments in {@code logSegmentNames} to the application index and
   * removes all the index segments that refer to the log segments that will be cleaned up from the application index.
   * The change is atomic with the use of {@link PersistentIndex#changeIndexSegments(List, Set)}.
   * @param logSegmentNames the names of the log segments whose index segments need to be committed.
   * @throws IOException if there were any I/O errors in setting up the index segment.
   * @throws StoreException if there were any problems committing the changed index segments.
   */
  private void updateSrcIndex(List<String> logSegmentNames) throws IOException, StoreException {
    Set<Offset> indexSegmentsToRemove = new HashSet<>();
    for (String logSegmentName : compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction()) {
      indexSegmentsToRemove.addAll(getIndexSegmentDetails(logSegmentName).keySet());
    }

    List<File> indexSegmentFilesToAdd = new ArrayList<>();
    Map<String, SortedMap<Offset, File>> newLogSegmentsToIndexSegmentDetails =
        getLogSegmentsToIndexSegmentsMap(logSegmentNames);
    for (Map.Entry<String, SortedMap<Offset, File>> entry : newLogSegmentsToIndexSegmentDetails.entrySet()) {
      LogSegment logSegment = srcLog.getSegment(entry.getKey());
      SortedMap<Offset, File> indexSegmentDetails = entry.getValue();
      long endOffset = logSegment.getStartOffset();
      for (Map.Entry<Offset, File> indexSegmentEntry : indexSegmentDetails.entrySet()) {
        File indexSegmentFile = indexSegmentEntry.getValue();
        IndexSegment indexSegment =
            new IndexSegment(indexSegmentFile, true, storeKeyFactory, config, tgtMetrics, null, time);
        endOffset = indexSegment.getEndOffset().getOffset();
        indexSegmentFilesToAdd.add(indexSegmentFile);
      }
      logSegment.setEndOffset(endOffset);
    }
    logger.debug("Adding {} to the application index and removing {} from the application index in {}",
        indexSegmentFilesToAdd, indexSegmentsToRemove, storeId);
    srcIndex.changeIndexSegments(indexSegmentFilesToAdd, indexSegmentsToRemove);
  }

  /**
   * Gets a map of all the index segment files that cover the given {@code logSegmentNames}.
   * @param logSegmentNames the names of log segments whose index segment files are required.
   * @return a map keyed on log segment name and containing details about all the index segment files that refer to it.
   */
  private Map<String, SortedMap<Offset, File>> getLogSegmentsToIndexSegmentsMap(List<String> logSegmentNames) {
    Map<String, SortedMap<Offset, File>> logSegmentsToIndexSegmentDetails = new HashMap<>();
    for (String logSegmentName : logSegmentNames) {
      logSegmentsToIndexSegmentDetails.put(logSegmentName, getIndexSegmentDetails(logSegmentName));
    }
    return logSegmentsToIndexSegmentDetails;
  }

  // cleanup() helpers

  /**
   * Waits until the ref counts of the all the log segments that need to be cleaned up reach 0.
   * @throws InterruptedException if the wait is interrupted.
   */
  private void waitTillRefCountZero() throws InterruptedException {
    if (isActive) {
      long singleWaitMs = Math.min(500, WAIT_TIME_FOR_CLEANUP_MS);
      long waitSoFar = 0;
      List<String> segmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
      for (String segmentName : segmentsUnderCompaction) {
        while (srcLog.getSegment(segmentName).refCount() > 0 && waitSoFar < WAIT_TIME_FOR_CLEANUP_MS) {
          time.sleep(singleWaitMs);
          waitSoFar += singleWaitMs;
        }
        if (srcLog.getSegment(segmentName).refCount() > 0) {
          logger.debug("Ref count of {} in {} did not go down to 0", segmentName, storeId);
        }
      }
    }
  }

  /**
   * Cleans up all the log and index segments. This involves:
   * 1. Dropping the log segments that are no longer relevant.
   * 2. Deleting all associated index segment and bloom files.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException
   */
  private void cleanupLogAndIndexSegments(boolean recovering) throws IOException {
    CompactionDetails details = compactionLog.getCompactionDetails();
    List<String> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    logger.debug("Cleaning up {} (and related index segments) in {}", segmentsUnderCompaction, storeId);
    for (int i = 0; i < segmentsUnderCompaction.size(); i++) {
      String segmentName = segmentsUnderCompaction.get(i);
      if (srcLog.getSegment(segmentName) != null) {
        PersistentIndex.cleanupIndexSegmentFilesForLogSegment(dataDir.getAbsolutePath(), segmentName);
        srcLog.dropSegment(segmentName, recovering || i >= numSwapsUsed);
      }
    }
  }

  /**
   * Resets all the internal data structures and wipes the slate clean for the next compaction.
   */
  private void resetStructures() {
    tgtLog = null;
    tgtIndex = null;
    numSwapsUsed = 0;
    recoveryStartToken = null;
  }

  /**
   * Ends compaction by closing the compaction log if it exists.
   */
  private void endCompaction() {
    if (compactionLog != null) {
      if (compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
        logger.info("Compaction of {} finished", storeId);
        if (srcIndex != null && srcIndex.hardDeleter != null) {
          srcIndex.hardDeleter.resume();
        }
      } else {
        logger.info("Compaction of {} suspended", storeId);
      }
      compactionLog.close();
      compactionLog = null;
    }
  }

  // general helpers

  /**
   * Gets all the index segment files that cover the given {@code logSegmentName}.
   * @param logSegmentName the name of the log segment whose index segment files are required.
   * @return a map that contains all the index segment files of the log segment - keyed on the start offset.
   */
  private SortedMap<Offset, File> getIndexSegmentDetails(String logSegmentName) {
    SortedMap<Offset, File> indexSegmentStartOffsetToFile = new TreeMap<>();
    File[] indexSegmentFiles =
        PersistentIndex.getIndexSegmentFilesForLogSegment(dataDir.getAbsolutePath(), logSegmentName);
    for (File indexSegmentFile : indexSegmentFiles) {
      indexSegmentStartOffsetToFile.put(IndexSegment.getIndexSegmentStartOffset(indexSegmentFile.getName()),
          indexSegmentFile);
    }
    return indexSegmentStartOffsetToFile;
  }
}
