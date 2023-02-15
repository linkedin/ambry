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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountUtils;
import com.github.ambry.account.Container;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.StoreFindToken.*;


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
    private final String SUFFIX = LogSegmentName.SUFFIX + TEMP_LOG_SEGMENT_NAME_SUFFIX;

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(SUFFIX);
    }
  };

  private static final long WAIT_TIME_FOR_CLEANUP_MS = 5 * Time.MsPerSec;
  private static final Logger logger = LoggerFactory.getLogger(BlobStoreCompactor.class);
  private final File dataDir;
  private final String storeId;
  private final DiskMetrics diskMetrics;
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
  private final IndexSegmentValidEntryFilter validEntryFilter;
  private final AccountService accountService;
  private final Set<Pair<Short, Short>> deprecatedContainers;
  private final RemoteTokenTracker remoteTokenTracker;
  private final boolean useDirectIO;
  private volatile boolean isActive = false;
  private PersistentIndex srcIndex;
  private Log tgtLog;
  private PersistentIndex tgtIndex;
  private long numSwapsUsed;
  private StoreFindToken recoveryStartToken = null;
  private CompactionLog compactionLog;
  private volatile CountDownLatch runningLatch = new CountDownLatch(0);
  private byte[] bundleReadBuffer;
  private final AtomicReference<CompactionDetails> currentCompactionDetails = new AtomicReference();
  private final AtomicInteger compactedLogCount = new AtomicInteger(0);
  private final AtomicInteger logSegmentCount = new AtomicInteger(0);
  private volatile boolean shouldPersistIndexSegmentOffsets = false;

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
   * @param accountService the {@link AccountService} instance to use.
   * @param remoteTokenTracker the {@link RemoteTokenTracker} that tracks tokens from all peer replicas.
   * @param diskMetrics the {@link DiskMetrics}
   * @throws IOException if the {@link CompactionLog} could not be created or if commit/cleanup failed during recovery.
   * @throws StoreException if the commit failed during recovery.
   */
  BlobStoreCompactor(String dataDir, String storeId, StoreKeyFactory storeKeyFactory, StoreConfig config,
      StoreMetrics srcMetrics, StoreMetrics tgtMetrics, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, Log srcLog, Time time, UUID sessionId, UUID incarnationId,
      AccountService accountService, RemoteTokenTracker remoteTokenTracker, DiskMetrics diskMetrics)
      throws IOException, StoreException {
    this.dataDir = new File(dataDir);
    this.diskMetrics = diskMetrics;
    this.storeId = storeId;
    this.storeKeyFactory = storeKeyFactory;
    this.config = config;
    this.srcMetrics = srcMetrics;
    this.tgtMetrics = tgtMetrics;
    this.srcLog = srcLog;
    this.diskIOScheduler = diskIOScheduler;
    this.diskSpaceAllocator = diskSpaceAllocator;
    this.accountService = accountService;
    this.time = time;
    this.sessionId = sessionId;
    this.incarnationId = incarnationId;
    this.useDirectIO = Utils.isLinux() && config.storeCompactionEnableDirectIO;
    this.deprecatedContainers = new HashSet<>();
    this.remoteTokenTracker = remoteTokenTracker;
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
  void initialize(PersistentIndex srcIndex) throws StoreException {
    this.srcIndex = srcIndex;
    if (compactionLog == null && srcIndex.hardDeleter != null && srcIndex.hardDeleter.isPaused()) {
      logger.debug("Resuming hard delete in {} during compactor initialize because there is no compaction in progress",
          storeId);
      srcIndex.hardDeleter.resume();
    }
    isActive = true;
    logger.info("Direct IO config: {}, OS: {}, availability: {}", config.storeCompactionEnableDirectIO,
        System.getProperty("os.name"), useDirectIO);
    srcMetrics.initializeCompactorGauges(storeId, compactionInProgress, currentCompactionDetails, compactedLogCount,
        logSegmentCount);
    logger.trace("Initialized BlobStoreCompactor for {}", storeId);
  }

  /**
   * Return the {@link #tgtMetrics}. Only used in tests.
   * @return the {@link #tgtMetrics}.
   */
  StoreMetrics getTgtMetrics() {
    return tgtMetrics;
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
    currentCompactionDetails.set(details);
    short version = shouldPersistIndexSegmentOffsets ? CompactionLog.CURRENT_VERSION : CompactionLog.VERSION_1;
    compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, version, time, details, config);
    resumeCompaction(bundleReadBuffer);
  }

  /**
   * Closes the last log segment periodically if replica is in sealed status.
   * @throws StoreException if any store exception occurred as part of ensuring capacity.
   */
  boolean closeLastLogSegmentIfQualified() throws StoreException {
    return srcLog.autoCloseLastLogSegmentIfQualified();
  }

  /**
   * Enable persisting index segment offsets in CompactionLog
   */
  void enablePersistIndexSegmentOffsets() {
    shouldPersistIndexSegmentOffsets = true;
    logger.info("Store {} enables persisting index segment offsets", storeId);
  }

  /**
   * Filters deprecated {@link Container}s for compaction purpose. Deprecated containers include DELETE_IN_PROGRESS
   * containers met with retention time and all INACTIVE containers.
   */
  private void getDeprecatedContainers() {
    deprecatedContainers.clear();
    if (accountService != null) {
      Set<Container> containers =
          AccountUtils.getDeprecatedContainers(accountService, config.storeContainerDeletionRetentionDays);
      deprecatedContainers.addAll(containers.stream()
          .map(container -> new Pair<>(container.getParentAccountId(), container.getId()))
          .collect(Collectors.toSet()));
      //TODO: Filter out the INACTIVE containers from deprecatedContainers set if it's already been compacted.
    }
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
    if (config.storeContainerDeletionEnabled) {
      getDeprecatedContainers();
      logger.info("Deprecated containers are {} for {}", deprecatedContainers, storeId);
    }
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
      if (e instanceof IOException) {
        diskMetrics.diskCompactionErrorDueToDiskFailureCount.inc();
        logger.error("Compaction of store {} failed due to disk issue.", dataDir);
      }
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
    List<LogSegmentName> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    // 1. all segments should be available
    // 2. all segments should be in order
    // 3. all segments should be a list of continuous log segments without skipping anyone in the middle
    // 4. all segments should not have anything in the journal
    LogSegmentName prevSegmentName = null;
    for (LogSegmentName segmentName : segmentsUnderCompaction) {
      LogSegment segment = srcLog.getSegment(segmentName);
      if (segment == null) {
        throw new IllegalArgumentException(segmentName + " does not exist in the log");
      }
      if (prevSegmentName != null && prevSegmentName.compareTo(segmentName) >= 0) {
        throw new IllegalArgumentException("Ordering of " + segmentsUnderCompaction + " is incorrect");
      }
      if (prevSegmentName != null && !prevSegmentName.equals(srcLog.getPrevSegment(segment).getName())) {
        throw new IllegalArgumentException(
            segmentsUnderCompaction + " is incorrect because it's not continuous. " + segment.getName()
                + "'s previous log segment should be " + srcLog.getPrevSegment(segment).getName() + " not "
                + prevSegmentName);
      }
      // should be outside the range of the journal
      Offset segmentEndOffset = new Offset(segment.getName(), segment.getEndOffset());
      Offset journalFirstOffset = srcIndex.journal.getFirstOffset();
      if (journalFirstOffset != null && segmentEndOffset.compareTo(journalFirstOffset) >= 0) {
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
    List<LogSegmentName> logSegmentsUnderCompaction =
        compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
    FileSpan duplicateSearchSpan = null;
    if (compactionLog.getCurrentIdx() > 0) {
      // only records in the very first log segment in the cycle could have been copied in a previous cycle
      logger.info("Continue compaction at cycle {} with {}", compactionLog.getCurrentIdx(), storeId);
      LogSegment firstLogSegment = srcLog.getSegment(logSegmentsUnderCompaction.get(0));
      LogSegment prevSegment = srcLog.getPrevSegment(firstLogSegment);
      logger.info("First log segment at this cycle is {}, previous log segment is {} with {}",
          firstLogSegment.getName(), prevSegment.getName(), storeId);
      if (prevSegment != null) {
        // duplicate data, if it exists, can only be in the log segment just before the first log segment in the cycle.
        Offset startOffset = new Offset(prevSegment.getName(), prevSegment.getStartOffset());
        Offset endOffset = new Offset(prevSegment.getName(), prevSegment.getEndOffset());
        duplicateSearchSpan = new FileSpan(startOffset, endOffset);
      }
      logger.info("Duplicate search span is {} for {}", duplicateSearchSpan, storeId);
    }

    for (LogSegmentName logSegmentName : logSegmentsUnderCompaction) {
      logger.info("Processing {} in {}", logSegmentName, storeId);
      LogSegment srcLogSegment = srcLog.getSegment(logSegmentName);
      Offset logSegmentEndOffset = new Offset(srcLogSegment.getName(), srcLogSegment.getEndOffset());
      if (needsCopying(logSegmentEndOffset) && !copyDataByLogSegment(srcLogSegment, duplicateSearchSpan)) {
        if (isActive) {
          // split the cycle only if there is no shutdown in progress
          logger.info("Splitting current cycle with segments {} at {} for {}", logSegmentsUnderCompaction,
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
      fixAfterOffsetsInCompactionLogWhenTargetIndexIsEmpty();
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
    List<LogSegmentName> logSegmentNames = getTargetLogSegmentNames();
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
    for (LogSegmentName segmentName : details.getLogSegmentsUnderCompaction()) {
      highestGeneration = Math.max(highestGeneration, segmentName.getGeneration());
    }
    logger.debug("Generation of target segments will be {} for {} in {}", highestGeneration + 1, details, storeId);
    List<Pair<LogSegmentName, String>> targetSegmentNamesAndFilenames = new ArrayList<>();
    List<LogSegment> existingTargetLogSegments = new ArrayList<>();
    for (LogSegmentName segmentName : details.getLogSegmentsUnderCompaction()) {
      long pos = segmentName.getPosition();
      LogSegmentName targetSegmentName = LogSegmentName.fromPositionAndGeneration(pos, highestGeneration + 1);
      String targetSegmentFileName = targetSegmentName.toFilename() + TEMP_LOG_SEGMENT_NAME_SUFFIX;
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
        existingTargetLogSegments, targetSegmentNamesAndFilenames.iterator(), diskMetrics);
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
    if (!compactionLog.isIndexSegmentOffsetsPersisted()) {
      // If index segment offsets are not saved within compaction log, then just use recovery start token to decide if
      // we should copy this index segment.
      return recoveryStartToken == null || recoveryStartToken.getOffset().compareTo(offset) < 0;
    }
    // We have index segment offsets from compaction log, every index segment whose offset exists in the compaction log
    // already has its content copied, with the last index segment as exception since it might be halfway through copying.
    NavigableMap<Offset, Offset> indexSegmentOffsets = compactionLog.getBeforeAndAfterIndexSegmentOffsets();
    Map.Entry<Offset, Offset> lastEntry = indexSegmentOffsets.lastEntry();
    return lastEntry == null || lastEntry.getKey().compareTo(offset) < 0;
  }

  /**
   * Return true if the index segment is under copy to target persistent index before crashing. This is only used
   * to check if there were a crash happened before compaction can finish.
   * @param indexSegmentStartOffset The start {@link Offset} of the {@link IndexSegment}.
   * @return {@code True} if the index segment is under copy to the target persistent index.
   */
  private boolean isIndexSegmentUnderCopy(Offset indexSegmentStartOffset) {
    return recoveryStartToken != null && recoveryStartToken.getOffset().equals(indexSegmentStartOffset);
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
    logger.debug("Copying data from {}", logSegmentToCopy);
    long logSegmentStartTime = time.milliseconds();
    for (Offset indexSegmentStartOffset : getIndexSegmentDetails(logSegmentToCopy.getName()).keySet()) {
      IndexSegment indexSegmentToCopy = srcIndex.getIndexSegments().get(indexSegmentStartOffset);
      logger.info("Processing index segment {} with {}", indexSegmentToCopy.getFile(), storeId);
      long startTime = SystemTime.getInstance().milliseconds();
      if (needsCopying(indexSegmentToCopy.getEndOffset()) && !copyDataByIndexSegment(logSegmentToCopy,
          indexSegmentToCopy, duplicateSearchSpan)) {
        // there is a shutdown in progress or there was no space to copy all entries.
        logger.info("Did not copy all entries in {} with {} (either because there is no space or there is a shutdown)",
            indexSegmentToCopy.getFile(), storeId);
        return false;
      }
      srcMetrics.compactionCopyDataByIndexSegmentTimeInMs.update(SystemTime.getInstance().milliseconds() - startTime,
          TimeUnit.MILLISECONDS);
    }
    srcMetrics.compactionCopyDataByLogSegmentTimeInMs.update(
        SystemTime.getInstance().milliseconds() - logSegmentStartTime, TimeUnit.MILLISECONDS);
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

    // Here we add the pair of before and after index segment offsets to the compaction log. This pair would be useful
    // in replication thread. In replication thread, when a remote peer sends a replication token to resume replication,
    // the token would have an offset to indicate which index segment this token is pointing to if it's index based token.
    // However, this index segment would be removed if the log segment is under compaction. Here we save the pair of start
    // offsets of index segments before and after compaction so in replication thread, we can just reset the token back to
    // the after compaction index segment start offset when the before compaction index segment is removed.
    // We are adding the offsets without knowing if any of the content from the current index segment would be copied to the
    // target. Even if there is no content from this index segment that would be preserved after compaction, we still want to
    // save this pair of index segment offsets since replication might be working on this index segment. And it also
    // indicates that last index segment finishes copying.
    compactionLog.addBeforeAndAfterIndexSegmentOffsetPair(indexSegmentToCopy.getStartOffset(),
        tgtIndex.getActiveIndexSegmentOffset());

    // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
    diskIOScheduler.getSlice(INDEX_SEGMENT_READ_JOB_NAME, INDEX_SEGMENT_READ_JOB_NAME, 1);
    boolean checkAlreadyCopied = isIndexSegmentUnderCopy(indexSegmentToCopy.getStartOffset());
    logger.trace("Should check already copied for {}: {} ", indexSegmentToCopy.getFile(), checkAlreadyCopied);

    List<IndexEntry> indexEntriesToCopy =
        validEntryFilter.getValidEntry(indexSegmentToCopy, duplicateSearchSpan, checkAlreadyCopied);
    long dataSize = indexEntriesToCopy.stream().mapToLong(entry -> entry.getValue().getSize()).sum();
    logger.trace("{} entries/{} bytes need to be copied in {} with {}", indexEntriesToCopy.size(), dataSize,
        indexSegmentToCopy.getFile(), storeId);

    if (indexEntriesToCopy.isEmpty()) {
      return true;
    }
    // Copy these over
    long startTime = SystemTime.getInstance().milliseconds();
    boolean copiedAll = copyRecords(logSegmentToCopy, indexEntriesToCopy, indexSegmentToCopy.getLastModifiedTimeSecs());
    srcMetrics.compactionCopyRecordTimeInMs.update(SystemTime.getInstance().milliseconds() - startTime,
        TimeUnit.MILLISECONDS);
    // persist
    tgtIndex.persistIndex();
    return copiedAll;
  }

  /**
   * Determines if {@code copyCandidate} container in the status of DELETED_IN_PROGRESS or INACTIVE.
   * @param copyCandidate the {@link IndexEntry} to check
   */
  private boolean isFromDeprecatedContainer(IndexEntry copyCandidate) {
    IndexValue copyCandidateValue = copyCandidate.getValue();
    boolean isFromDeprecatedContainer = false;
    if (deprecatedContainers.contains(
        new Pair<>(copyCandidateValue.getAccountId(), copyCandidateValue.getContainerId()))) {
      logger.trace("{} in store {} is from deprecated container.", copyCandidate, storeId);
      isFromDeprecatedContainer = true;
    }
    return isFromDeprecatedContainer;
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
    // rest in 1_1. When 0_1 is full and put into the "main" log, there will be some duplicated data in 0_1 and 1_0.
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
    List<LogSegmentName> segmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
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
   * Calculate the desired rate based on current disk latency and threshold.
   * @param diskTimePerMbInMs current disk latency per MB in ms.
   * @param latencyThreshold the threshold.
   * @return the adjusted compaction speed.
   */
  int getDesiredSpeedPerSecond(double diskTimePerMbInMs, int latencyThreshold) {
    int currentLatency = diskTimePerMbInMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) diskTimePerMbInMs;
    int desiredRatePerSec = config.storeCompactionOperationsBytesPerSec;
    if (currentLatency > latencyThreshold) {
      desiredRatePerSec = (int) (config.storeCompactionMinOperationsBytesPerSec +
          (config.storeCompactionOperationsBytesPerSec - config.storeCompactionMinOperationsBytesPerSec)
              * latencyThreshold / currentLatency / config.storeCompactionOperationsAdjustK);
      if (desiredRatePerSec < config.storeCompactionMinOperationsBytesPerSec) {
        desiredRatePerSec = config.storeCompactionMinOperationsBytesPerSec;
      }
      if (desiredRatePerSec > config.storeCompactionOperationsBytesPerSec) {
        desiredRatePerSec = config.storeCompactionOperationsBytesPerSec;
      }
    }
    return desiredRatePerSec;
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
            if (config.storeCompactionMinOperationsBytesPerSec < config.storeCompactionOperationsBytesPerSec
                && diskMetrics != null) {
              // Enable dynamic desired rate based on disk latency.
              double currentReadTimePerMbInMs = diskMetrics.diskReadTimePerMbInMs.getSnapshot().get95thPercentile();
              int desiredReadPerSecond = getDesiredSpeedPerSecond(currentReadTimePerMbInMs,
                  config.storeCompactionIoPerMbReadLatencyThresholdMs);
              double currentWriteTimePerMbInMs = diskMetrics.diskWriteTimePerMbInMs.getSnapshot().get95thPercentile();
              int desiredWritePerSecond = getDesiredSpeedPerSecond(currentWriteTimePerMbInMs,
                  config.storeCompactionIoPerMbWriteLatencyThresholdMs);
              if (currentReadTimePerMbInMs > config.storeCompactionIoPerMbReadLatencyThresholdMs
                  || currentWriteTimePerMbInMs > config.storeCompactionIoPerMbWriteLatencyThresholdMs) {
                // Only log when compaction copy rate is impacted.
                logger.debug(
                    "Current disk read per MB: {}ms, current disk write per MB: {} ms, Desired compaction copy rate(bytes/seconds): read: {} write: {}",
                    currentReadTimePerMbInMs, currentWriteTimePerMbInMs, desiredReadPerSecond, desiredWritePerSecond);
              }
              int desiredRate = Math.min(desiredReadPerSecond, desiredWritePerSecond);
              double currentRate = diskMetrics.diskCompactionCopyRateInBytes.getOneMinuteRate();
              if (currentRate > desiredRate) {
                // If last one minute rate is too high, need to slow down(by sleep) to desired rate.
                // 100 -> 50 : sleep 2-1 time unit
                // 100 -> 25 : sleep 4-1 time unit
                // 100 -> 90: sleep (100/90 - 1) time unit
                long sleepInMs = (long) ((currentRate / desiredRate - 1) * Time.MsPerSec);
                long actualSleepTimeInMs = Math.min(sleepInMs, 10 * Time.MsPerSec);
                logger.debug(
                    "Current rate: {}, desired rate: {}. Calculated sleep time: {} ms. Actual sleep time: {} ms",
                    currentRate, desiredRate, sleepInMs, actualSleepTimeInMs);
                try {
                  Thread.sleep(actualSleepTimeInMs);
                } catch (InterruptedException e) {
                  logger.warn("Compaction throttling sleep is interrupted", e);
                }
              }
            } else {
              logger.debug(
                  "Adaptive compaction is not enabled: storeCompactionMinOperationsBytesPerSec: {}, storeCompactionOperationsBytesPerSec: {}",
                  config.storeCompactionMinOperationsBytesPerSec, config.storeCompactionOperationsBytesPerSec);
              // call into diskIOScheduler to make sure we can proceed.
              diskIOScheduler.getSlice(COMPACTION_CLEANUP_JOB_NAME, COMPACTION_CLEANUP_JOB_NAME, writtenLastTime);
            }

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
                // Here don't use markAsUndelete method. In markAsUndelete, multiple sanity check would be applied. But
                // target index might only contain incomplete blob history, which would fail sanity check.
                // Here we do a different check to make sure we don't insert undelete with lower lifeVersion.
                if (valueFromTgtIdx.isUndelete()) {
                  throw new StoreException(
                      "Id " + srcIndexEntry.getKey() + " already has undelete " + valueFromTgtIdx + " in index "
                          + dataDir, StoreErrorCodes.ID_Undeleted);
                }
                // Undelete would increase the life version
                if (valueFromTgtIdx.getLifeVersion() >= srcValue.getLifeVersion()) {
                  throw new StoreException(
                      "Id " + srcIndexEntry.getKey() + " has bad lifeversion " + valueFromTgtIdx + " in index "
                          + dataDir, StoreErrorCodes.Life_Version_Conflict);
                }
              }
              IndexValue tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getFlags(),
                  srcValue.getExpiresAtMs(), srcValue.getOperationTimeInMs(), srcValue.getAccountId(),
                  srcValue.getContainerId(), srcValue.getLifeVersion());
              tgtValue.setFlag(IndexValue.Flags.Undelete_Index);
              tgtValue.clearOriginalMessageOffset();
              tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
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
              // Add a PUT IndexValue with the same info (especially same lifeVersion).
              IndexValue tgtValue =
                  new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE,
                      srcValue.getExpiresAtMs(), fileSpan.getStartOffset().getOffset(), srcValue.getOperationTimeInMs(),
                      srcValue.getAccountId(), srcValue.getContainerId(), srcValue.getLifeVersion());
              tgtIndex.addToIndex(new IndexEntry(srcIndexEntry.getKey(), tgtValue), fileSpan);
            }
            long lastModifiedTimeSecsToSet =
                srcValue.getOperationTimeInMs() != Utils.Infinite_Time ? srcValue.getOperationTimeInMs() / Time.MsPerSec
                    : lastModifiedTimeSecs;
            tgtIndex.getIndexSegments().lastEntry().getValue().setLastModifiedTimeSecs(lastModifiedTimeSecsToSet);
            writtenLastTime = srcValue.getSize();
            srcMetrics.compactionCopyRateInBytes.mark(srcValue.getSize());
            if (diskMetrics != null) {
              diskMetrics.diskCompactionCopyRateInBytes.mark(srcValue.getSize());
            }
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
      logger.debug("Cleaning up {}", files);
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

  /**
   * Fix the after offsets in the compaction log when there is no valid data being copied to the target index, therefore
   * the target index is empty.
   */
  private void fixAfterOffsetsInCompactionLogWhenTargetIndexIsEmpty() {
    // If we are here, there are several things we can be sure with current implementation.
    // 1. There is no split cycle for this compaction.
    // 2. All the log segments under compaction have no valid data to copy.
    // The reason to create a split cycle is that the log segments under compaction has too much data to write to current
    // target log segment. If there is a split cycle, the target log segment in next cycle would definitely have some
    // leftover data from the previous cycle to write.
    // And since there is no split cycle, this is the only cycle for this compaction. And there is no valid data. So
    // all log segments have no valid data.
    //
    // This means there were no active index segment for target index, therefore the before and after pairs we put in the
    // compaction log are not valid anymore since all the after offsets would be the start offset of the target log, which
    // doesn't exist. In this case, we have to fix it.
    Set<Offset> values = new HashSet<>(compactionLog.getBeforeAndAfterIndexSegmentOffsets().values());
    Offset startOffset = tgtLog.getEndOffset();
    if (values.size() != 1 || !values.contains(startOffset)) {
      logger.error("{}: Expecting one value {} in the compaction log for index segment offsets, but {}", storeId,
          startOffset, values);
    }
    // We have to find a valid index segment and change all the after offsets to be this index segment's start offset.
    // The last index segment of the closest log segment that are not under compaction should be the valid.
    LogSegmentName logSegmentName = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction().get(0);
    Offset validOffset = srcIndex.getIndexSegments().lowerKey(new Offset(logSegmentName, 0));
    // The valid offset should be the last index segment of the previous log segment. But if it's null, then we would just
    // clear the map.
    compactionLog.updateBeforeAndAfterIndexSegmentOffsetsWithValidOffset(validOffset);
  }

  // commit() helpers

  /**
   * Gets the names of the target log segments that were generated (the swap spaces). Note that this function returns
   * those log segments that already exist and is not useful to determine what should exist and it only returns the ones
   * that are still in their temporary uncommitted state.
   * @return the list of target log segments that exist on the disk.
   */
  private List<LogSegmentName> getTargetLogSegmentNames() {
    List<String> filenames = Arrays.asList(dataDir.list(TEMP_LOG_SEGMENTS_FILTER));
    List<LogSegmentName> names = new ArrayList<>(filenames.size());
    for (String filename : filenames) {
      String transformed = filename.substring(0, filename.length() - TEMP_LOG_SEGMENT_NAME_SUFFIX.length());
      names.add(LogSegmentName.fromFilename(transformed));
    }
    Collections.sort(names);
    return names;
  }

  /**
   * Renames the temporary log segment files to remove the temporary file name suffix and effectively commits them on
   * disk.
   * @param logSegmentNames the names of the log segments whose backing files need to be renamed.
   * @throws StoreException if the rename fails.
   */
  private void renameLogSegments(List<LogSegmentName> logSegmentNames) throws StoreException {
    for (LogSegmentName segmentName : logSegmentNames) {
      String filename = segmentName.toFilename();
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
  private void addNewLogSegmentsToSrcLog(List<LogSegmentName> logSegmentNames, boolean recovering)
      throws StoreException {
    logger.debug("Adding {} in {} to the application log", logSegmentNames, storeId);
    for (LogSegmentName logSegmentName : logSegmentNames) {
      File segmentFile = new File(dataDir, logSegmentName.toFilename());
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
  private void updateSrcIndex(List<LogSegmentName> logSegmentNames) throws StoreException {
    Set<Offset> indexSegmentsToRemove = new HashSet<>();
    for (LogSegmentName logSegmentName : compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction()) {
      indexSegmentsToRemove.addAll(getIndexSegmentDetails(logSegmentName).keySet());
    }

    List<File> indexSegmentFilesToAdd = new ArrayList<>();
    Map<LogSegmentName, SortedMap<Offset, File>> newLogSegmentsToIndexSegmentDetails =
        getLogSegmentsToIndexSegmentsMap(logSegmentNames);
    for (Map.Entry<LogSegmentName, SortedMap<Offset, File>> entry : newLogSegmentsToIndexSegmentDetails.entrySet()) {
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

    if (compactionLog.isIndexSegmentOffsetsPersisted()) {
      srcIndex.updateBeforeAndAfterCompactionIndexSegmentOffsets(compactionLog.getStartTime(),
          compactionLog.getBeforeAndAfterIndexSegmentOffsetsForCurrentCycle(), true);
    }
    // Update the index segments, this would remove some index segments and add others. We have to update the before and
    // after compaction index segment offsets before this method.
    srcIndex.changeIndexSegments(indexSegmentFilesToAdd, indexSegmentsToRemove);
    srcIndex.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
  }

  /**
   * Gets a map of all the index segment files that cover the given {@code logSegmentNames}.
   * @param logSegmentNames the names of log segments whose index segment files are required.
   * @return a map keyed on log segment name and containing details about all the index segment files that refer to it.
   */
  private Map<LogSegmentName, SortedMap<Offset, File>> getLogSegmentsToIndexSegmentsMap(
      List<LogSegmentName> logSegmentNames) {
    Map<LogSegmentName, SortedMap<Offset, File>> logSegmentsToIndexSegmentDetails = new HashMap<>();
    for (LogSegmentName logSegmentName : logSegmentNames) {
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
      List<LogSegmentName> segmentsUnderCompaction =
          compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
      for (LogSegmentName segmentName : segmentsUnderCompaction) {
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
    List<LogSegmentName> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    logger.debug("Cleaning up {} (and related index segments) in {}", segmentsUnderCompaction, storeId);
    for (int i = 0; i < segmentsUnderCompaction.size(); i++) {
      LogSegmentName segmentName = segmentsUnderCompaction.get(i);
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
  private void endCompaction() throws StoreException {
    if (compactionLog != null) {
      if (compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
        logger.info("Compaction of {} finished", storeId);
        if (srcIndex != null && !srcIndex.isEmpty() && !compactionLog.cycleLogs.isEmpty()) {
          // The log segment positions after compaction
          Set<Long> logSegmentPositionsAfterCompaction = srcIndex.getLogSegments()
              .stream()
              .map(LogSegment::getName)
              .map(LogSegmentName::getPosition)
              .collect(Collectors.toSet());
          logSegmentCount.set(logSegmentPositionsAfterCompaction.size());
          // The log segment positions under compaction
          Set<Long> logSegmentPositionsUnderCompaction = new HashSet<>();
          for (CompactionLog.CycleLog clog : compactionLog.cycleLogs) {
            logSegmentPositionsUnderCompaction.addAll(clog.compactionDetails.getLogSegmentsUnderCompaction()
                .stream()
                .map(LogSegmentName::getPosition)
                .collect(Collectors.toSet()));
          }
          // If the position under compaction doesn't exist in the set after compaction, then this log segment is compacted
          // eg: log segment under compaction [0_23, 130_16, 144_3]
          //     log segment after compaction [0_24, 130_24, 155_0, 166_0]
          // log segment 144_3's position doesn't exist in the compaction log, so we have one log segment compacted.
          compactedLogCount.set((int) logSegmentPositionsUnderCompaction.stream()
              .filter(p -> !logSegmentPositionsAfterCompaction.contains(p))
              .count());
        }

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
  SortedMap<Offset, File> getIndexSegmentDetails(LogSegmentName logSegmentName) {
    SortedMap<Offset, File> indexSegmentStartOffsetToFile = new TreeMap<>();
    File[] indexSegmentFiles =
        PersistentIndex.getIndexSegmentFilesForLogSegment(dataDir.getAbsolutePath(), logSegmentName);
    for (File indexSegmentFile : indexSegmentFiles) {
      Offset indexOffset = IndexSegment.getIndexSegmentStartOffset(indexSegmentFile.getName());
      // We don't have to check if the IndexSegment's log segment name is the same as the input log segment name
      // since now getIndexSegmentFilesForLogSegment would match exactly the log segment name we provided so that
      // when given 0_1, 0_10's index segment files will never get matched.
      // But leave this here for sanity check.
      if (indexOffset.getName().equals(logSegmentName)) {
        indexSegmentStartOffsetToFile.put(indexOffset, indexSegmentFile);
      }
    }
    return indexSegmentStartOffsetToFile;
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
    if (putValue == null && updateValue.isDelete()) {
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
   * Check if given delete tombstone is removable. There are two cases where delete tombstone can be safely removed:
   * 1. delete record has finite expiration time and it has expired already;
   * 2. all peer replica tokens have passed position of this delete (That is, they have replicated this delete already)
   * @param deleteIndexEntry the {@link IndexEntry} associated with delete tombstone
   * @param currentIndexSegment the {@link IndexSegment} delete tombstone comes from.
   * @return {@code true} if this delete tombstone can be safely removed. {@code false} otherwise.
   */
  private boolean isDeleteTombstoneRemovable(IndexEntry deleteIndexEntry, IndexSegment currentIndexSegment)
      throws StoreException {
    if (srcIndex.isExpired(deleteIndexEntry.getValue())) {
      return true;
    }
    if (remoteTokenTracker == null) {
      return false;
    }
    for (Map.Entry<String, Pair<Long, FindToken>> entry : remoteTokenTracker.getPeerReplicaAndToken().entrySet()) {
      Pair<Long, FindToken> pair = entry.getValue();
      FindToken token = srcIndex.resetTokenIfRequired((StoreFindToken) pair.getSecond());
      if (!token.equals(pair.getSecond())) {
        // incarnation id has changed or there is unclean shutdown
        return false;
      }
      token = srcIndex.revalidateFindToken(pair.getSecond());
      if (!token.equals(pair.getSecond())) {
        // the log segment (token refers to) has been compacted already
        return false;
      }
      switch (token.getType()) {
        case Uninitialized:
          return false;
        case JournalBased:
          // if code reaches here, it means the journal-based token is valid (didn't get reset). Since compaction always
          // performs on segments out of journal, this journal-based token must be past the delete tombstone.
          break;
        case IndexBased:
          // if code reaches here, the index-based token is valid (didn't get reset). We check two following rules:
          // 1. token's index segment is behind delete tombstone's index segment
          // 2. if they are in the same segment, compare the store key (sealed index segment is sorted based on key)
          StoreFindToken indexBasedToken = (StoreFindToken) token;
          if (indexBasedToken.getOffset().compareTo(currentIndexSegment.getStartOffset()) < 0) {
            // index-based token is ahead of current index segment (hasn't reached this delete tombstone)
            return false;
          }
          if (indexBasedToken.getOffset().compareTo(currentIndexSegment.getStartOffset()) == 0) {
            // index-based token refers to current index segment, we need to compare the key
            if (indexBasedToken.getStoreKey().compareTo(deleteIndexEntry.getKey()) <= 0) {
              return false;
            }
          }
          // if tokens start offset > current index segment start offset, then token is obviously past tombstone
          break;
        default:
          throw new IllegalArgumentException("Unsupported token type in compaction: " + token.getType());
      }
    }
    srcMetrics.permanentDeleteTombstonePurgeCount.inc();
    return true;
  }

  /**
   * IndexSegmentValidEntryFilter without undelete log records.
   */
  class IndexSegmentValidEntryFilterWithoutUndelete implements IndexSegmentValidEntryFilter {

    private int numKeysFoundInDuplicateChecking;

    @Override
    public List<IndexEntry> getValidEntry(IndexSegment indexSegment, FileSpan duplicateSearchSpan,
        boolean checkAlreadyCopied) throws StoreException {
      numKeysFoundInDuplicateChecking = 0;
      List<IndexEntry> allIndexEntries = new ArrayList<>();
      // get all entries. We get one entry per key
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), allIndexEntries,
          new AtomicLong(0), true, false);
      // save a token for restart (the key gets ignored but is required to be non null for construction)
      StoreFindToken safeToken =
          new StoreFindToken(allIndexEntries.get(0).getKey(), indexSegment.getStartOffset(), sessionId, incarnationId,
              null, null, UNINITIALIZED_RESET_KEY_VERSION);
      compactionLog.setSafeToken(safeToken);
      logger.debug("Set safe token for compaction in {} to {}", storeId, safeToken);

      List<IndexEntry> copyCandidates = getValidIndexEntries(indexSegment, allIndexEntries);
      int validEntriesSize = copyCandidates.size();
      copyCandidates.removeIf(copyCandidate ->
          isDuplicate(copyCandidate, duplicateSearchSpan, indexSegment.getStartOffset(), checkAlreadyCopied) || (
              config.storeContainerDeletionEnabled && isFromDeprecatedContainer(copyCandidate)));
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
      long deleteReferenceTime = compactionLog.getCompactionDetails().getReferenceTimeMs();
      List<IndexEntry> validEntries = new ArrayList<>();
      for (IndexEntry indexEntry : allIndexEntries) {
        IndexValue value = indexEntry.getValue();
        if (value.isDelete()) {
          IndexValue putValue = getPutValueFromSrc(indexEntry.getKey(), value, indexSegment);
          if (putValue != null) {
            // In PersistentIndex.markAsDeleted(), the expiry time of the put/ttl update value is copied into the
            // delete value. So it is safe to check for isExpired() on the delete value.
            if (value.getOperationTimeInMs() < deleteReferenceTime || srcIndex.isExpired(value)) {
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
            // DELETE entry without corresponding PUT (may be left by previous compaction). Check if this delete
            // tombstone is removable.
            if (config.storeCompactionPurgeDeleteTombstone && isDeleteTombstoneRemovable(indexEntry, indexSegment)) {
              logger.debug(
                  "Delete tombstone of {} (with expiration time {} ms) is removable and won't be copied to target log segment",
                  indexEntry.getKey(), value.getExpiresAtMs());
            } else {
              validEntries.add(indexEntry);
            }
          }
        } else if (value.isTtlUpdate()) {
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
            if (!hasDeleteEntryOutOfRetention(indexEntry.getKey(), indexSegment.getStartOffset(),
                deleteReferenceTime)) {
              // PUT entry that has not expired and is not considered deleted.
              // Add all values in this index segment (to account for the presence of TTL updates)
              addAllEntriesForKeyInSegment(validEntries, indexSegment, indexEntry);
            } else {
              logger.trace("{} in index segment with start offset {} in {} is not valid because it is a deleted PUT",
                  indexEntry, indexSegment.getStartOffset(), storeId);
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
     * @param deleteReferenceTime the timestamp to reference when deciding a delete is out of retention
     * @return {@code true} if the key has a delete entry that is out of retention
     * @throws StoreException if there are any problems using the index
     */
    private boolean hasDeleteEntryOutOfRetention(StoreKey key, Offset searchStartOffset, long deleteReferenceTime)
        throws StoreException {
      FileSpan deleteSearchSpan = new FileSpan(searchStartOffset, srcIndex.getCurrentEndOffset());
      IndexValue deleteValue =
          srcIndex.findKey(key, deleteSearchSpan, EnumSet.of(PersistentIndex.IndexEntryType.DELETE));
      return deleteValue != null && deleteValue.getOperationTimeInMs() < deleteReferenceTime;
    }

    @Override
    public boolean alreadyExists(PersistentIndex idx, FileSpan searchSpan, StoreKey key, IndexValue srcValue)
        throws StoreException {
      IndexValue value = idx.findKey(key, searchSpan, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
      boolean exists = false;
      if (value != null) {
        numKeysFoundInDuplicateChecking++;
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
      copyCandidates.removeIf(copyCandidate ->
          isDuplicate(copyCandidate, duplicateSearchSpan, indexSegment.getStartOffset(), checkAlreadyCopied) || (
              config.storeContainerDeletionEnabled && isFromDeprecatedContainer(copyCandidate)));
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
     * @param indexSegment the {@link IndexSegment} that index entries are from.
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
      // Put(version c)      | Put(version f)    | isExpired(Pc)?false:true                     |
      //                     | Delete(f)         | reachRetention(Df)||isExpired(Df)?false:true |
      //                     | Undelete(f)       | isExpired(Uf)?false:true                     |
      // --------------------+-------------------+----------------------------------------------|
      // TtlUpdate(c)        | Put(f)            | Exception                                    |
      //                     | Delete(f)         | reachRetention(Df)?false:true                |
      //                     | Undelete(f)       | true                                         |
      // --------------------+-------------------+----------------------------------------------|
      // Delete(c)           | Put(f)            | Exception                                    |
      //                     | Delete(f)         | c==f && !isRemovable(Df) ? true : false      |
      //                     | Undelete(f)       | false                                        |
      // --------------------+-------------------+----------------------------------------------|
      // Undelete(c)         | Put(f)            | Exception                                    |
      //                     | Delete(f)         | false                                        |
      //                     | Undelete(f)       | c==f&&!isExpired(Uf)?true:false              |
      // ----------------------------------------------------------------------------------------
      List<IndexEntry> validEntries = new ArrayList<>();
      StoreKey previousKey = null;
      IndexValue previousLatestState = null;
      for (IndexEntry entry : indexSegment) {
        StoreKey currentKey = entry.getKey();
        IndexValue currentValue = entry.getValue();
        IndexValue currentLatestState;

        if (previousKey == null) {
          // save a token for restart (the key gets ignored but is required to be non null for construction)
          StoreFindToken safeToken =
              new StoreFindToken(currentKey, indexSegment.getStartOffset(), sessionId, incarnationId, null, null,
                  UNINITIALIZED_RESET_KEY_VERSION);
          compactionLog.setSafeToken(safeToken);
          logger.debug("Set safe token for compaction in {} to {}", storeId, safeToken);
        }
        // If an IndexSegment contains more than one IndexValue for the same StoreKey, then they must follow each other
        // since IndexSegment stores IndexValues based on StoreKey. If the current key equals to the previous key, then
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
              && !srcIndex.isExpired(currentLatestState)) {
            validEntries.add(entry);
          }
        } else if (currentValue.isDelete()) {
          if (currentLatestState.isPut()) {
            throw new IllegalStateException(
                "Delete's latest state can't be put for key" + currentKey + " in store " + dataDir);
          }
          if (currentLatestState.isDelete() && currentLatestState.getLifeVersion() == currentValue.getLifeVersion()) {
            // check if this is a removable delete tombstone.
            if (config.storeCompactionPurgeDeleteTombstone && isDeleteTombstoneRemovable(entry, indexSegment)
                && getPutValueFromSrc(currentKey, currentValue, indexSegment) == null) {
              logger.debug(
                  "Delete tombstone of {} (with expiration time {} ms) is removable and won't be copied to target log segment",
                  currentKey, currentValue.getExpiresAtMs());
            } else {
              validEntries.add(entry);
            }
          }
        } else if (currentValue.isTtlUpdate()) {
          if (currentLatestState == null) {
            // Under only one circumstance would this happen, an TTL_UPDATE index value has no PUT before and no DELETE after.
            // This is because the PUT is already compacted due to DELETE is out of retention and DELETE is compacted due to
            // DELETE tombstone clean up. But TTL_UPDATE is in different log segment from PUT and DELETE so it never gets compacted.
            continue;
          }
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
