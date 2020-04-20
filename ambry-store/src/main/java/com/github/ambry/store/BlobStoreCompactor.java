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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
  static final String COMPACTION_CLEANUP_JOB_NAME = "blob_store_compactor_cleanup";
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
  private final IndexSegmentValidEntryFilter validEntryFilter;

  private volatile boolean isActive = false;
  private PersistentIndex srcIndex;

  private Log tgtLog;
  private PersistentIndex tgtIndex;
  private long numSwapsUsed;
  private StoreFindToken recoveryStartToken = null;
  private CompactionLog compactionLog;
  private volatile CountDownLatch runningLatch = new CountDownLatch(0);
  private byte[] bundleReadBuffer;
  private final boolean useDirectIO;

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
    this.useDirectIO = Utils.isLinux() && config.storeCompactionEnableDirectIO;
    if (config.storeCompactionFilter.equals(IndexSegmentValidEntryFilterWithoutUndelete.class.getSimpleName())) {
      validEntryFilter = new IndexSegmentValidEntryFilterWithoutUndelete();
    } else {
      validEntryFilter = new IndexSegmentValidEntryFilterWithUndelete();
    }
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
    logger.info("Direct IO config: {}, OS: {}, availability: {}", config.storeCompactionEnableDirectIO,
        System.getProperty("os.name"), useDirectIO);
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
   * @param bundleReadBuffer the preAllocated buffer for bundle read in compaction copy phase.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
   * in the segments to compact are in the journal.
   * @throws IllegalStateException if the compactor has not been initialized.
   * @throws IOException if there were I/O errors creating the {@link CompactionLog}
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  void compact(CompactionDetails details, byte[] bundleReadBuffer) throws IOException, StoreException {
    if (srcIndex == null) {
      throw new IllegalStateException("Compactor has not been initialized");
    } else if (compactionLog != null) {
      throw new IllegalStateException("There is already a compaction in progress");
    }
    checkSanity(details);
    logger.info("Compaction of {} started with details {}", storeId, details);
    compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, time, details, config);
    resumeCompaction(bundleReadBuffer);
  }

  /**
   * Resumes compaction from where it was left off.
   * @throws IllegalStateException if the compactor has not been initialized or if there is no compaction to resume.
   * @throws StoreException if there were exceptions reading to writing to store components.
   * @param bundleReadBuffer the preAllocated buffer for bundle read in compaction copy phase.
   */
  void resumeCompaction(byte[] bundleReadBuffer) throws StoreException {
    this.bundleReadBuffer = bundleReadBuffer;
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
    this.bundleReadBuffer = null;
  }

  /**
   * @return an array of temporary log segment files this compactor is currently using.
   */
  String[] getSwapSegmentsInUse() throws StoreException {
    String[] tempSegments = dataDir.list(TEMP_LOG_SEGMENTS_FILTER);
    if (tempSegments == null) {
      throw new StoreException("Error occurred while listing files in data dir:" + dataDir.getAbsolutePath(),
          StoreErrorCodes.IOError);
    }
    return tempSegments;
  }

  /**
   * If a compaction was in progress during a crash/shutdown, fixes the state so that the store is loaded correctly
   * and compaction can resume smoothly on a call to {@link #resumeCompaction(byte[])}. Expected to be called before the
   * {@link PersistentIndex} is instantiated in the {@link BlobStore}.
   * @throws IOException if the {@link CompactionLog} could not be created or if commit or cleanup failed.
   * @throws StoreException if the commit failed.
   */
  private void fixStateIfRequired() throws IOException, StoreException {
    if (CompactionLog.isCompactionInProgress(dataDir.getAbsolutePath(), storeId)) {
      compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, storeKeyFactory, time, config);
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
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void copy() throws IOException, StoreException {
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
    tgtIndex.close(false);
    tgtLog.close(false);
    // persist the bloom of the "latest" index segment if it exists
    if (numSwapsUsed > 0) {
      tgtIndex.getIndexSegments().lastEntry().getValue().seal();
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
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void commit(boolean recovering) throws StoreException {
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
   * @throws StoreException if there were store exception during cleanup.
   */
  private void cleanup(boolean recovering) throws StoreException {
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
   * @throws IOException if logs and indexes could not be set up.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void setupState() throws IOException, StoreException {
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
        existingTargetLogSegments.add(new LogSegment(targetSegmentName, targetSegmentFile, config, tgtMetrics));
      } else {
        targetSegmentNamesAndFilenames.add(new Pair<>(targetSegmentName, targetSegmentFileName));
      }
    }
    // TODO: available swap space count should be obtained from DiskManager. For now, assumed to be 1.
    long targetLogTotalCapacity = srcLog.getSegmentCapacity();
    logger.debug("Target log capacity is {} for {}. Existing log segments are {}. Future names and files are {}",
        targetLogTotalCapacity, storeId, existingTargetLogSegments, targetSegmentNamesAndFilenames);
    tgtLog = new Log(dataDir.getAbsolutePath(), targetLogTotalCapacity, diskSpaceAllocator, config, tgtMetrics, true,
        existingTargetLogSegments, targetSegmentNamesAndFilenames.iterator());
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
    // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
    diskIOScheduler.getSlice(INDEX_SEGMENT_READ_JOB_NAME, INDEX_SEGMENT_READ_JOB_NAME, 1);
    boolean checkAlreadyCopied =
        recoveryStartToken != null && recoveryStartToken.getOffset().equals(indexSegmentToCopy.getStartOffset());
    logger.trace("Should check already copied for {}: {} ", indexSegmentToCopy.getFile(), checkAlreadyCopied);

    List<IndexEntry> indexEntriesToCopy =
        validEntryFilter.getValidEntry(indexSegmentToCopy, duplicateSearchSpan, checkAlreadyCopied);
    logger.debug("{} entries need to be copied in {}", indexEntriesToCopy.size(), indexSegmentToCopy.getFile());

    // Copy these over
    boolean copiedAll = copyRecords(logSegmentToCopy, indexEntriesToCopy, indexSegmentToCopy.getLastModifiedTimeSecs());
    // persist
    tgtIndex.persistIndex();
    return copiedAll;
  }

  /**
   * Determines if {@code copyCandidate} is a duplicate.
   * @param copyCandidate the {@link IndexEntry} to check
   * @param duplicateSearchSpan the search span for duplicates in the source index
   * @param indexSegmentStartOffset the start offset of the {@link IndexSegment} being processed
   * @param checkAlreadyCopied if {@code true}, checks if {@code copyCandidate} has already been copied.
   * @return {@code true} if {@code copyCandidate} is a duplicate of something that exists or has been copied
   */
  private boolean isDuplicate(IndexEntry copyCandidate, FileSpan duplicateSearchSpan, Offset indexSegmentStartOffset,
      boolean checkAlreadyCopied) {
    // Duplicates can exist because of two reasons
    // 1. Duplicates in src: These can occur because compaction creates temporary duplicates
    // Consider the following situation
    // Segments 0_0, 1_0 and 2_0 are being compacted. All the data in 0_0 and half the data in 1_0 fit in 0_1 and the
    // rest in 1_1. When 0_1 is full and is put into the "main" log, there will be some duplicated data in 0_1 and 1_0.
    // If a shutdown occurs after this point, then the code has to make sure not to copy any of the data already copied
    // 2. Duplicates in tgt: the checkpointing logic during copying works at the IndexSegment level
    // This means that a crash/shutdown could occur in the middle of copying the data represented by an IndexSegment and
    // the incremental progress inside an IndexSegment is not logged. After restart, compaction is resumed from the
    // checkpoint and will have to detect data that it has already copied
    try {
      boolean isDuplicate = false;
      IndexValue copyCandidateValue = copyCandidate.getValue();
      if (duplicateSearchSpan != null && validEntryFilter.alreadyExists(srcIndex, duplicateSearchSpan,
          copyCandidate.getKey(), copyCandidateValue)) {
        // is a duplicate because it already is in the store
        logger.trace("{} in segment with start offset {} in {} is a duplicate because it already exists in store",
            copyCandidate, indexSegmentStartOffset, storeId);
        isDuplicate = true;
      } else if (checkAlreadyCopied && validEntryFilter.alreadyExists(tgtIndex, null, copyCandidate.getKey(),
          copyCandidateValue)) {
        // is a duplicate because it has already been copied
        logger.trace("{} in segment with start offset {} in {} is a duplicate because it has already been copied",
            copyCandidate, indexSegmentStartOffset, storeId);
        isDuplicate = true;
      } else {
        // not a duplicate
        logger.trace("{} in index segment with start offset {} in {} is not a duplicate", copyCandidate,
            indexSegmentStartOffset, storeId);
      }
      return isDuplicate;
    } catch (StoreException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @param offset the {@link Offset} to check
   * @return {@code true} if the offset is being compacted in the current cycle.
   */
  private boolean isOffsetUnderCompaction(Offset offset) {
    List<String> segmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
    LogSegment first = srcLog.getSegment(segmentsUnderCompaction.get(0));
    LogSegment last = srcLog.getSegment(segmentsUnderCompaction.get(segmentsUnderCompaction.size() - 1));
    Offset start = new Offset(first.getName(), first.getStartOffset());
    Offset end = new Offset(last.getName(), last.getEndOffset());
    return new FileSpan(start, end).inSpan(offset);
  }

  /**
   * Calculate the farthest index that can be used for a bundle IO read based on start index and bundleReadBuffer capacity.
   * @param sortedSrcIndexEntries all available entries, which are ordered by offset.
   * @param start the starting index for the given list.
   * @return the farthest index, inclusively, which can be used for a bundle read.
   */
  private int getBundleReadEndIndex(List<IndexEntry> sortedSrcIndexEntries, int start) {
    long startOffset = sortedSrcIndexEntries.get(start).getValue().getOffset().getOffset();
    int end;
    for (end = start; end < sortedSrcIndexEntries.size(); end++) {
      long currentSize =
          sortedSrcIndexEntries.get(end).getValue().getOffset().getOffset() + sortedSrcIndexEntries.get(end)
              .getValue()
              .getSize() - startOffset;
      if (currentSize > bundleReadBuffer.length) {
        break;
      }
    }
    return end - 1;
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
      // byte[] for both general IO and direct IO.
      byte[] byteArrayToUse;
      // ByteBuffer for general IO
      ByteBuffer bufferToUse = null;
      // the index number of the list to start/end with.
      int start = 0, end;
      // the size (in bytes) from start to end for read.
      int readSize;
      while (start < srcIndexEntries.size()) {
        // try to do a bundle of read to reduce disk IO
        long startOffset = srcIndexEntries.get(start).getValue().getOffset().getOffset();
        if (bundleReadBuffer == null || srcIndexEntries.get(start).getValue().getSize() > bundleReadBuffer.length) {
          end = start;
          readSize = (int) srcIndexEntries.get(start).getValue().getSize();
          byteArrayToUse = new byte[readSize];
          srcMetrics.compactionBundleReadBufferNotFitIn.inc();
          logger.trace("Record size greater than bundleReadBuffer capacity, key: {} size: {}",
              srcIndexEntries.get(start).getKey(), srcIndexEntries.get(start).getValue().getSize());
        } else {
          end = getBundleReadEndIndex(srcIndexEntries, start);
          readSize = (int) (srcIndexEntries.get(end).getValue().getOffset().getOffset() + srcIndexEntries.get(end)
              .getValue()
              .getSize() - startOffset);
          byteArrayToUse = bundleReadBuffer;
          srcMetrics.compactionBundleReadBufferUsed.inc();
        }
        if (useDirectIO) {
          // do direct IO read
          logSegmentToCopy.readIntoDirectly(byteArrayToUse, startOffset, readSize);
        } else {
          // do general IO read
          bufferToUse = ByteBuffer.wrap(byteArrayToUse);
          bufferToUse.position(0);
          bufferToUse.limit(readSize);
          int ioCount = Utils.readFileToByteBuffer(fileChannel, startOffset, bufferToUse);
          srcMetrics.compactionBundleReadBufferIoCount.inc(ioCount);
        }

        // copy from buffer to tgtLog
        for (int i = start; i <= end; i++) {
          IndexEntry srcIndexEntry = srcIndexEntries.get(i);
          IndexValue srcValue = srcIndexEntry.getValue();
          long usedCapacity = tgtIndex.getLogUsedCapacity();
          if (isActive && (tgtLog.getCapacityInBytes() - usedCapacity >= srcValue.getSize())) {
            Offset endOffsetOfLastMessage = tgtLog.getEndOffset();
            // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
            diskIOScheduler.getSlice(COMPACTION_CLEANUP_JOB_NAME, COMPACTION_CLEANUP_JOB_NAME, writtenLastTime);
            if (useDirectIO) {
              // do direct IO write
              tgtLog.appendFromDirectly(byteArrayToUse, (int) (srcValue.getOffset().getOffset() - startOffset),
                  (int) srcIndexEntry.getValue().getSize());
            } else {
              // do general write
              long bufferPosition = srcValue.getOffset().getOffset() - startOffset;
              bufferToUse.limit((int) (bufferPosition + srcIndexEntry.getValue().getSize()));
              bufferToUse.position((int) (bufferPosition));
              tgtLog.appendFrom(bufferToUse);
            }
            FileSpan fileSpan = tgtLog.getFileSpanForMessage(endOffsetOfLastMessage, srcValue.getSize());
            IndexValue valueFromTgtIdx = tgtIndex.findKey(srcIndexEntry.getKey());
            if (srcValue.isDelete()) {
              if (valueFromTgtIdx != null) {
                tgtIndex.markAsDeleted(srcIndexEntry.getKey(), fileSpan, null, srcValue.getOperationTimeInMs(),
                    srcValue.getLifeVersion());
              } else {
                IndexValue tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getFlags(),
                    srcValue.getExpiresAtMs(), srcValue.getOperationTimeInMs(), srcValue.getAccountId(),
                    srcValue.getContainerId(), srcValue.getLifeVersion());
                tgtValue.setFlag(IndexValue.Flags.Delete_Index);
                tgtValue.clearOriginalMessageOffset();
                tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
              }
            } else if (srcValue.isUndelete()) {
              if (valueFromTgtIdx != null) {
                tgtIndex.markAsUndeleted(srcIndexEntry.getKey(), fileSpan, srcValue.getOperationTimeInMs(),
                    srcValue.getLifeVersion());
              } else {
                IndexValue tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getFlags(),
                    srcValue.getExpiresAtMs(), srcValue.getOperationTimeInMs(), srcValue.getAccountId(),
                    srcValue.getContainerId(), srcValue.getLifeVersion());
                tgtValue.setFlag(IndexValue.Flags.Undelete_Index);
                tgtValue.clearOriginalMessageOffset();
                tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
              }
            } else if (srcValue.isTtlUpdate()) {
              if (valueFromTgtIdx != null) {
                tgtIndex.markAsPermanent(srcIndexEntry.getKey(), fileSpan, null, srcValue.getOperationTimeInMs(),
                    srcValue.getLifeVersion());
              } else {
                IndexValue tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getFlags(),
                    srcValue.getExpiresAtMs(), srcValue.getOperationTimeInMs(), srcValue.getAccountId(),
                    srcValue.getContainerId(), srcValue.getLifeVersion());
                tgtValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
                tgtValue.clearOriginalMessageOffset();
                tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
              }
            } else if (valueFromTgtIdx != null) {
              throw new StoreException("Cannot insert duplicate PUT entry for " + srcIndexEntry.getKey(),
                  StoreErrorCodes.Unknown_Error);
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
        if (!copiedAll) {
          // break outer while loop
          break;
        }
        tgtLog.flush();
        start = end + 1;
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
   * @throws StoreException if there were any store exception creating a log segment
   */
  private void addNewLogSegmentsToSrcLog(List<String> logSegmentNames, boolean recovering) throws StoreException {
    logger.debug("Adding {} in {} to the application log", logSegmentNames, storeId);
    for (String logSegmentName : logSegmentNames) {
      File segmentFile = new File(dataDir, LogSegmentNameHelper.nameToFilename(logSegmentName));
      LogSegment segment = new LogSegment(logSegmentName, segmentFile, config, srcMetrics);
      srcLog.addSegment(segment, recovering);
    }
  }

  /**
   * Adds all the index segments that refer to log segments in {@code logSegmentNames} to the application index and
   * removes all the index segments that refer to the log segments that will be cleaned up from the application index.
   * The change is atomic with the use of {@link PersistentIndex#changeIndexSegments(List, Set)}.
   * @param logSegmentNames the names of the log segments whose index segments need to be committed.
   * @throws StoreException if there were any problems committing the changed index segments.
   */
  private void updateSrcIndex(List<String> logSegmentNames) throws StoreException {
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
  private void cleanupLogAndIndexSegments(boolean recovering) throws StoreException {
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

  /**
   * IndexSegmentValidEntryFilter without undelete log records.
   */
  class IndexSegmentValidEntryFilterWithoutUndelete implements IndexSegmentValidEntryFilter {

    @Override
    public List<IndexEntry> getValidEntry(IndexSegment indexSegment, FileSpan duplicateSearchSpan,
        boolean checkAlreadyCopied) throws StoreException {
      List<IndexEntry> allIndexEntries = new ArrayList<>();
      // get all entries. We get one entry per key
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), allIndexEntries,
          new AtomicLong(0), true);
      // save a token for restart (the key gets ignored but is required to be non null for construction)
      StoreFindToken safeToken =
          new StoreFindToken(allIndexEntries.get(0).getKey(), indexSegment.getStartOffset(), sessionId, incarnationId);
      compactionLog.setSafeToken(safeToken);
      logger.debug("Set safe token for compaction in {} to {}", storeId, safeToken);

      List<IndexEntry> copyCandidates = getValidIndexEntries(indexSegment, allIndexEntries);
      int validEntriesSize = copyCandidates.size();
      copyCandidates.removeIf(
          copyCandidate -> isDuplicate(copyCandidate, duplicateSearchSpan, indexSegment.getStartOffset(),
              checkAlreadyCopied));
      // order by offset in log.
      copyCandidates.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
      logger.debug("Out of {} entries, {} are valid and {} will be copied in this round", allIndexEntries.size(),
          validEntriesSize, copyCandidates.size());
      logger.trace("For index segment with start offset {} in {} - Total index entries: {}. Entries to copy: {}",
          indexSegment.getStartOffset(), storeId, allIndexEntries, copyCandidates);
      return copyCandidates;
    }

    /**
     * Gets all the valid index entries in the given list of index entries.
     * @param indexSegment the {@link IndexSegment} that {@code allIndexEntries} are from.
     * @param allIndexEntries the list of {@link IndexEntry} instances from which the valid entries have to be chosen.
     *                        There should be only one entry per key where a DELETE is preferred over all other entries
     *                        and PUT is preferred over a TTL update entry
     * @return the list of valid entries generated from {@code allIndexEntries}. May contain entries not in
     * {@code allIndexEntries}.
     * @throws StoreException if {@link BlobReadOptions} could not be obtained from the store for deleted blobs.
     */
    private List<IndexEntry> getValidIndexEntries(IndexSegment indexSegment, List<IndexEntry> allIndexEntries)
        throws StoreException {
      // Assumed preference order from IndexSegment (current impl)
      // (Legend: entry/entries in segment -> output from IndexSegment#getIndexEntriesSince())
      // PUT entry only -> PUT entry
      // TTL update entry only -> TTL update entry
      // DELETE entry only -> DELETE entry
      // PUT + DELETE -> DELETE
      // TTL update + DELETE -> DELETE
      // PUT + TTL update -> PUT (the one relevant to this comment)
      // PUT + TTL update + DELETE -> DELETE
      // TODO: move this blob store stats
      Offset startOffsetOfLastIndexSegmentForDeleteCheck = getStartOffsetOfLastIndexSegmentForDeleteCheck();
      // deletes are in effect if this index segment does not have any deletes that are less than
      // StoreConfig#storeDeletedMessageRetentionDays days old. If there are such deletes, then they are not counted as
      // deletes and the PUT records are still valid as far as compaction is concerned
      boolean deletesInEffect = startOffsetOfLastIndexSegmentForDeleteCheck != null
          && indexSegment.getStartOffset().compareTo(startOffsetOfLastIndexSegmentForDeleteCheck) <= 0;
      logger.trace("Deletes in effect is {} for index segment with start offset {} in {}", deletesInEffect,
          indexSegment.getStartOffset(), storeId);
      List<IndexEntry> validEntries = new ArrayList<>();
      for (IndexEntry indexEntry : allIndexEntries) {
        IndexValue value = indexEntry.getValue();
        if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
          IndexValue putValue = getPutValueFromSrc(indexEntry.getKey(), value, indexSegment);
          if (putValue != null) {
            // In PersistentIndex.markAsDeleted(), the expiry time of the put/ttl update value is copied into the
            // delete value. So it is safe to check for isExpired() on the delete value.
            if (deletesInEffect || srcIndex.isExpired(value)) {
              // still have to evaluate whether the TTL update has to be copied
              NavigableSet<IndexValue> values = indexSegment.find(indexEntry.getKey());
              IndexValue secondVal = values.lower(values.last());
              if (secondVal != null && secondVal.isFlagSet(IndexValue.Flags.Ttl_Update_Index) && isTtlUpdateEntryValid(
                  indexEntry.getKey(), indexSegment.getStartOffset())) {
                validEntries.add(new IndexEntry(indexEntry.getKey(), secondVal));
              }
              // DELETE entry. Always valid.
              validEntries.add(indexEntry);
            } else {
              // if this delete cannot be counted and there is a corresponding unexpired PUT/TTL update entry in the same
              // index segment, we will need to add it.
              addAllEntriesForKeyInSegment(validEntries, indexSegment, indexEntry);
            }
          } else {
            // DELETE entry. Always valid.
            validEntries.add(indexEntry);
          }
        } else if (value.isFlagSet(IndexValue.Flags.Ttl_Update_Index)) {
          // if IndexSegment::getIndexEntriesSince() returns a TTL update entry, it is because it is the ONLY entry i.e.
          // no PUT or DELETE in the same index segment.
          if (isTtlUpdateEntryValid(indexEntry.getKey(), indexSegment.getStartOffset())) {
            validEntries.add(indexEntry);
          }
        } else {
          IndexValue valueFromIdx = srcIndex.findKey(indexEntry.getKey());
          // Doesn't matter whether we get the PUT or DELETE entry for the expiry test
          if (!srcIndex.isExpired(valueFromIdx)) {
            // unexpired PUT entry.
            if (deletesInEffect) {
              if (!hasDeleteEntryInSpan(indexEntry.getKey(), indexSegment.getStartOffset(),
                  startOffsetOfLastIndexSegmentForDeleteCheck)) {
                // PUT entry that has not expired and is not considered deleted.
                // Add all values in this index segment (to account for the presence of TTL updates)
                addAllEntriesForKeyInSegment(validEntries, indexSegment, indexEntry);
              } else {
                logger.trace("{} in index segment with start offset {} in {} is not valid because it is a deleted PUT",
                    indexEntry, indexSegment.getStartOffset(), storeId);
              }
            } else {
              // valid PUT entry
              // Add all values in this index segment (to account for the presence of TTL updates)
              addAllEntriesForKeyInSegment(validEntries, indexSegment, indexEntry);
            }
          } else {
            logger.trace("{} in index segment with start offset {} in {} is not valid because it is an expired PUT",
                indexEntry, indexSegment.getStartOffset(), storeId);
          }
        }
      }
      return validEntries;
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
     * Gets the {@link IndexValue} for the PUT from the {@link #srcIndex} (if it exists)
     * @param key the {@link StoreKey} whose PUT is required
     * @param updateValue the update (TTL update/delete) {@link IndexValue} associated with the same {@code key}
     * @param indexSegmentOfUpdateValue the {@link IndexSegment} that {@code updateValue} belongs to
     * @return the {@link IndexValue} for the PUT in the {@link #srcIndex} (if it exists)
     * @throws StoreException if there are problems with the index
     */
    private IndexValue getPutValueFromSrc(StoreKey key, IndexValue updateValue, IndexSegment indexSegmentOfUpdateValue)
        throws StoreException {
      IndexValue putValue = srcIndex.findKey(key, new FileSpan(srcIndex.getStartOffset(), updateValue.getOffset()),
          EnumSet.of(PersistentIndex.IndexEntryType.PUT));
      // in a non multi valued segment, if putValue is not found directly from the index, check if the PUT and DELETE
      // are the same segment so that the PUT entry can be constructed from the DELETE entry
      if (putValue == null && updateValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        putValue = getPutValueFromDeleteEntry(key, updateValue, indexSegmentOfUpdateValue);
      }
      return putValue;
    }

    /**
     * Gets the {@link IndexValue} for the PUT using info in the {@code deleteValue)
     * @param key the {@link StoreKey} whose PUT is required
     * @param deleteValue the delete {@link IndexValue} associated with the same {@code key}
     * @param indexSegmentOfUpdateValue the {@link IndexSegment} that {@code deleteValue} belongs to
     * @return the {@link IndexValue} for the PUT in the {@link #srcIndex} (if it exists)
     */
    private IndexValue getPutValueFromDeleteEntry(StoreKey key, IndexValue deleteValue,
        IndexSegment indexSegmentOfUpdateValue) {
      // TODO: find a way to test this?
      IndexValue putValue = null;
      long putRecordOffset = deleteValue.getOriginalMessageOffset();
      if (putRecordOffset != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET && putRecordOffset != deleteValue.getOffset()
          .getOffset() && indexSegmentOfUpdateValue.getStartOffset().getOffset() <= putRecordOffset) {
        try (BlobReadOptions options = srcIndex.getBlobReadInfo(key, EnumSet.allOf(StoreGetOptions.class))) {
          Offset offset = new Offset(indexSegmentOfUpdateValue.getStartOffset().getName(), options.getOffset());
          MessageInfo info = options.getMessageInfo();
          putValue = new IndexValue(info.getSize(), offset, info.getExpirationTimeInMs(), info.getOperationTimeMs(),
              info.getAccountId(), info.getContainerId());
        } catch (StoreException e) {
          logger.error("Fetching PUT index entry of {} in {} failed", key, indexSegmentOfUpdateValue.getStartOffset());
        }
      }
      return putValue;
    }

    /**
     * Determines whether a TTL update entry is valid. A TTL update entry is valid as long as the associated PUT record
     * is still present in the store (the validity of the PUT record does not matter - only its presence/absence does).
     * @param key the {@link StoreKey} being examined
     * @param indexSegmentStartOffset the start offset of the {@link IndexSegment} that the TTL update record is in
     * @return {@code true} if the TTL update entry is valid
     * @throws StoreException if there are problems reading the index
     */
    private boolean isTtlUpdateEntryValid(StoreKey key, Offset indexSegmentStartOffset) throws StoreException {
      boolean valid = false;
      //  A TTL update entry is "valid" if the corresponding PUT is still alive
      // The PUT entry, if it exists, must be "before" this TTL update entry.
      FileSpan srcSearchSpan = new FileSpan(srcIndex.getStartOffset(), indexSegmentStartOffset);
      IndexValue srcValue = srcIndex.findKey(key, srcSearchSpan, EnumSet.of(PersistentIndex.IndexEntryType.PUT));
      if (srcValue == null) {
        // PUT is not in the source - therefore can't be in target. This TTL update can be cleaned up
        logger.trace("TTL update of {} in segment with start offset {} in {} is not valid the corresponding PUT entry "
            + "does not exist anymore", key, indexSegmentStartOffset, storeId);
      } else {
        // exists in source - now we need to check if it exists in the target
        IndexValue tgtValue = tgtIndex.findKey(key, null, EnumSet.of(PersistentIndex.IndexEntryType.PUT));
        if (tgtValue == null && isOffsetUnderCompaction(srcValue.getOffset())) {
          // exists in src but not in tgt. This can happen either because
          // 1. The FileSpan to which srcValue belongs is not under compaction (so there is no reason for tgt to have it)
          // 2. srcValue will be compacted in this cycle (because it has been determined that the PUT is not valid. Since
          // the PUT is going away in this cycle, it is safe to remove the TTL update also)
          logger.trace(
              "TTL update of {} in segment with start offset {} in {} is not valid because the corresponding PUT entry"
                  + " {} will be compacted in this cycle ({} are being compacted in this cycle)", key,
              indexSegmentStartOffset, storeId, srcValue,
              compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction());
        } else {
          // PUT entry exists in both the src and tgt,
          // OR PUT entry exists in source and the offset of the source entry is not under compaction in this cycle
          // therefore this TTL update entry cannot be compacted
          valid = true;
        }
      }
      return valid;
    }

    /**
     * Adds entries related to {@code entry} that are in the same {@code indexSegment} including {@code entry}
     * @param entries the list of {@link IndexEntry} to add to.
     * @param indexSegment the {@link IndexSegment} to fetch values from.
     * @param entry the {@link IndexEntry} that is under processing
     * @throws StoreException if there are problems using the index
     */
    private void addAllEntriesForKeyInSegment(List<IndexEntry> entries, IndexSegment indexSegment, IndexEntry entry)
        throws StoreException {
      logger.trace("Fetching related entries of a blob with entry {} in index segment with start offset {} in {} "
          + "because they need to be retained", entry, indexSegment.getStartOffset(), storeId);
      NavigableSet<IndexValue> values = indexSegment.find(entry.getKey());
      if (values.size() > 1) {
        // we are using a multivalued index segment. Any related values will be in this set
        values.forEach(valueFromSeg -> entries.add(new IndexEntry(entry.getKey(), valueFromSeg)));
      } else {
        // in a non multi valued segment, there can only be PUTs and DELETEs in the same segment
        entries.add(entry);
        if (entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
          IndexValue putValue = getPutValueFromDeleteEntry(entry.getKey(), entry.getValue(), indexSegment);
          if (putValue != null) {
            entries.add(new IndexEntry(entry.getKey(), putValue));
          }
        }
      }
    }

    /**
     * @param key the {@link StoreKey} to check
     * @param searchStartOffset the start offset of the search for delete entry
     * @param searchEndOffset the end offset of the search for delete entry
     * @return {@code true} if the key has a delete entry in the given search span.
     * @throws StoreException if there are any problems using the index
     */
    private boolean hasDeleteEntryInSpan(StoreKey key, Offset searchStartOffset, Offset searchEndOffset)
        throws StoreException {
      FileSpan deleteSearchSpan = new FileSpan(searchStartOffset, searchEndOffset);
      return srcIndex.findKey(key, deleteSearchSpan, EnumSet.of(PersistentIndex.IndexEntryType.DELETE)) != null;
    }

    @Override
    public boolean alreadyExists(PersistentIndex idx, FileSpan searchSpan, StoreKey key, IndexValue srcValue)
        throws StoreException {
      IndexValue value = idx.findKey(key, searchSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
      boolean exists = false;
      if (value != null) {
        if (value.isDelete()) {
          exists = true;
        } else if (value.isTtlUpdate()) {
          // if srcValue is not a delete, it is a duplicate.
          exists = !srcValue.isDelete();
        } else {
          // value is a PUT without a TTL update or a DELETE
          exists = !srcValue.isDelete() && !srcValue.isTtlUpdate();
        }
      }
      return exists;
    }
  }

  /**
   * IndexSegmentValidEntryFilter with undelete log records.
   */
  class IndexSegmentValidEntryFilterWithUndelete implements IndexSegmentValidEntryFilter {
    @Override
    public List<IndexEntry> getValidEntry(IndexSegment indexSegment, FileSpan duplicateSearchSpan,
        boolean checkAlreadyCopied) throws StoreException {
      List<IndexEntry> copyCandidates = getValidIndexEntries(indexSegment);
      int validEntriesSize = copyCandidates.size();
      copyCandidates.removeIf(
          copyCandidate -> isDuplicate(copyCandidate, duplicateSearchSpan, indexSegment.getStartOffset(),
              checkAlreadyCopied));
      // order by offset in log.
      copyCandidates.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
      logger.debug("Out of entries, {} are valid and {} will be copied in this round", validEntriesSize,
          copyCandidates.size());
      logger.trace("For index segment with start offset {} in {} - Entries to copy: {}", indexSegment.getStartOffset(),
          storeId, copyCandidates);
      return copyCandidates;
    }

    /**
     * Gets all the valid index entries in the given list of index entries.
     * @param indexSegment the {@link IndexSegment} that {@code allIndexEntries} are from.
     * @return the list of valid entries generated from {@code allIndexEntries}. May contain entries not in
     * {@code allIndexEntries}.
     * @throws StoreException if {@link BlobReadOptions} could not be obtained from the store for deleted blobs.
     */
    private List<IndexEntry> getValidIndexEntries(IndexSegment indexSegment) throws StoreException {
      // Validity of a IndexValue is determined by itself and it's latest state. For example, if the current IndexValue
      // is a Put, and the latest IndexValue for this blob is a Delete, and the operation is done out of retention duration,
      // then this Put IndexValue is considered as invalid.
      //
      // There is one exception, TtlUpdate IndexValue. A TtlUpdate IndexValue's validity is not only depends on the latest
      // state of the blob, it's also affected by the Put IndexValue. If the latest IndexValue for this blob is a Delete,
      // and it's operation time is out of the retention duration, this TtlUpdate record would be considered as invalid.
      // However if the Put IndexValue exists for this TtlUpdate and is not under compaction, then we will keep this TtlUpdate
      // anyway.
      //
      // This is a table that shows how the validity of the current IndexValue is determined.
      // ----------------------------------------------------------------------------------------
      // Current IndexValue  | Latest IndexValue | Is Valid                                     |
      // --------------------+-------------------+----------------------------------------------|
      // Put(verion c)       | Put(version f)    | isExpired(Pc)?false:true                     |
      //                     | Delete(f)         | reachRetention(Df)||isExpired(Df)?false:true |
      //                     | Undelete(f)       | isExpired(Uf)?false:true                     |
      // --------------------+-------------------+----------------------------------------------|
      // TtlUpdate(c)        | Put(f)            | Exception                                    |
      //                     | Delete(f)         | reachRetention(Df)?false:true                |
      //                     | Undelete(f)       | true                                         |
      // --------------------+-------------------+----------------------------------------------|
      // Delete(c)           | Put(f)            | Exception                                    |
      //                     | Delete(f)         | c==f?true:false                              |
      //                     | Undelete(f)       | false                                        |
      // --------------------+-------------------+----------------------------------------------|
      // Undelete(c)         | Put(f)            | Exception                                    |
      //                     | Delete(f)         | false                                        |
      //                     | Undelete(f)       | c==f&&!isExpired(Uc)?true:false              |
      // ----------------------------------------------------------------------------------------
      List<IndexEntry> validEntries = new ArrayList<>();
      Iterator<IndexEntry> iterator = indexSegment.getIterator();
      StoreKey previousKey = null;
      IndexValue previousLatestState = null;
      while (iterator.hasNext()) {
        IndexEntry entry = iterator.next();
        StoreKey currentKey = entry.getKey();
        IndexValue currentValue = entry.getValue();
        IndexValue currentLatestState;

        if (previousKey == null) {
          // save a token for restart (the key gets ignored but is required to be non null for construction)
          StoreFindToken safeToken =
              new StoreFindToken(currentKey, indexSegment.getStartOffset(), sessionId, incarnationId);
          compactionLog.setSafeToken(safeToken);
          logger.debug("Set safe token for compaction in {} to {}", storeId, safeToken);
        }
        // If an IndexSegment contains more than one IndexValue for the same StoreKey, then they must follow each other
        // since IndexSegment store IndexValues based on StoreKey. If the current key equals to the previous key, then
        // we don't have to query the latest state again.
        if (currentKey.equals(previousKey)) {
          currentLatestState = previousLatestState;
        } else {
          previousKey = currentKey;
          previousLatestState = currentLatestState = srcIndex.findKey(currentKey);
        }

        if (currentValue.isUndelete()) {
          if (currentLatestState.isPut()) {
            throw new IllegalStateException(
                "Undelete's latest state can't be put for key" + currentKey + " in store " + dataDir);
          }
          if (currentLatestState.isUndelete() && currentLatestState.getLifeVersion() == currentValue.getLifeVersion()
              && !srcIndex.isExpired(currentValue)) {
            validEntries.add(entry);
          }
        } else if (currentValue.isDelete()) {
          if (currentLatestState.isPut()) {
            throw new IllegalStateException(
                "Delete's latest state can't be put for key" + currentKey + " in store " + dataDir);
          }
          if (currentLatestState.isDelete() && currentLatestState.getLifeVersion() == currentValue.getLifeVersion()) {
            validEntries.add(entry);
          }
        } else if (currentValue.isTtlUpdate()) {
          if (currentLatestState.isPut()) {
            // If isPut returns true, when the latest state doesn't carry ttl_update flag, this is wrong
            throw new IllegalStateException(
                "TtlUpdate's latest state can't be put for key" + currentKey + " in store " + dataDir);
          }
          // This is a TTL_UPDATE record, then the blob can't be expired. Only check if it's deleted or not.
          if (currentLatestState.isDelete()) {
            if (currentLatestState.getOperationTimeInMs() >= compactionLog.getCompactionDetails()
                .getReferenceTimeMs()) {
              validEntries.add(entry);
            } else if (isTtlUpdateEntryValidWhenLatestStateIsDeleteAndRetention(currentKey,
                indexSegment.getStartOffset())) {
              validEntries.add(entry);
            }
          } else {
            validEntries.add(entry);
          }
        } else {
          if (srcIndex.isExpired(currentLatestState)) {
            logger.trace("{} in index segment with start offset {} in {} is not valid because it is an expired PUT",
                entry, indexSegment.getStartOffset(), storeId);
            continue;
          }
          if (currentLatestState.isPut()) {
            if (currentLatestState.getLifeVersion() != currentValue.getLifeVersion()) {
              throw new IllegalStateException(
                  "Two different lifeVersions  for puts key" + currentKey + " in store " + dataDir);
            }
            validEntries.add(entry);
          } else if (currentLatestState.isDelete()) {
            if (currentLatestState.getOperationTimeInMs() >= compactionLog.getCompactionDetails()
                .getReferenceTimeMs()) {
              validEntries.add(entry);
            } else {
              logger.trace("{} in index segment with start offset {} in {} is not valid because it is a deleted PUT",
                  entry, indexSegment.getStartOffset(), storeId);
            }
          } else {
            validEntries.add(entry);
          }
        }
      }
      return validEntries;
    }

    /**
     * Determines whether a TTL update entry is valid. A TTL update entry is valid as long as the associated PUT record
     * is still present in the store (the validity of the PUT record does not matter - only its presence/absence does).
     * @param key the {@link StoreKey} being examined
     * @param indexSegmentStartOffset the start offset of the {@link IndexSegment} that the TTL update record is in
     * @return {@code true} if the TTL update entry is valid
     * @throws StoreException if there are problems reading the index
     */
    private boolean isTtlUpdateEntryValidWhenLatestStateIsDeleteAndRetention(StoreKey key,
        Offset indexSegmentStartOffset) throws StoreException {
      boolean valid = false;
      //  A TTL update entry is "valid" if the corresponding PUT is still alive
      // The PUT entry, if it exists, must be "before" this TTL update entry.
      FileSpan srcSearchSpan = new FileSpan(srcIndex.getStartOffset(), indexSegmentStartOffset);
      IndexValue srcValue = srcIndex.findKey(key, srcSearchSpan, EnumSet.of(PersistentIndex.IndexEntryType.PUT));
      if (srcValue == null) {
        // PUT is not in the source - it might be compacted
        logger.trace("TTL update of {} in segment with start offset {} in {} is not valid the corresponding PUT entry "
            + "does not exist anymore", key, indexSegmentStartOffset, storeId);
      } else {
        // PUT exists in the source index even when the latest state for this StoreKey is delete and delete already reaches
        // retention date. This can happen either because
        // 1. The log segment to which srcValue(PUT) belongs is not under compaction, thus PUT is not compacted.
        // 2. srcValue(PUT) will be compacted in this cycle (because it has been determined that the PUT is not valid. Since
        // the PUT is going away in this cycle, it is safe to remove the TTL update also)
        //
        // For condition one, we will keep TTL_UPDATE, for condition 2, we will remove TTL_UPDATE.
        if (isOffsetUnderCompaction(srcValue.getOffset())) {
          logger.trace(
              "TTL update of {} in segment with start offset {} in {} is not valid because the corresponding PUT entry"
                  + " {} will be compacted in this cycle ({} are being compacted in this cycle)", key,
              indexSegmentStartOffset, storeId, srcValue,
              compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction());
        } else {
          // PUT entry exists in both the src and tgt,
          // OR PUT entry exists in source and the offset of the source entry is not under compaction in this cycle
          // therefore this TTL update entry cannot be compacted
          valid = true;
        }
      }
      return valid;
    }

    @Override
    public boolean alreadyExists(PersistentIndex idx, FileSpan searchSpan, StoreKey key, IndexValue srcValue)
        throws StoreException {
      IndexValue value = idx.findKey(key, searchSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
      if (value == null) {
        return false;
      }
      if (value.getLifeVersion() > srcValue.getLifeVersion()) {
        // If the IndexValue in the previous index has a lifeVersion higher than source value, then this
        // must be a duplicate.
        return true;
      } else if (value.getLifeVersion() < srcValue.getLifeVersion()) {
        // If the IndexValue in the previous index has a lifeVersion lower than source value, then this
        // is not a duplicate.
        return false;
      } else {
        // When the lifeVersions are the same, the order of the record are **FIX** as
        // P/U -> T -> D
        if (value.isDelete()) {
          return true;
        } else if (value.isTtlUpdate()) {
          // if srcValue is not a delete, it is a duplicate.
          return !srcValue.isDelete();
        } else if (value.isUndelete()) {
          if (srcValue.isPut()) {
            throw new IllegalStateException(
                "An Undelete[" + value + "] and a Put[" + srcValue + "] can't be at the same lifeVersion at store "
                    + dataDir);
          }
          // value is a UNDELETE without a TTL update or
          return srcValue.isUndelete();
        } else {
          if (srcValue.isUndelete()) {
            throw new IllegalStateException(
                "A Put[" + value + "] and an Undelete[" + srcValue + "] can't be at the same lifeVersion at store "
                    + dataDir);
          }
          // value is a PUT without a TTL update
          return srcValue.isPut();
        }
      }
    }
  }
}
