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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility to verify that compaction has been executed correctly. For the config required, please look at
 * {@link CompactionVerifierConfig}.
 *
 * Note that replicas cannot be used for the source store. It has to be the state of the store before the
 * compaction being verified. It is preferable to skip the last log segment (and related index segments) when creating
 * the source state directory. It is also important to retain the last modified times of the files on disk if the files
 * are being copied from other locations.
 *
 * The verifier verifies structure integrity and data integrity.
 *
 * Structure integrity verifications:
 * 1. Checks that log segments that should exist, still exist and ones that should not, no longer do.
 * 2. Checks that log segments that are newly created have the right generation number.
 * 3. Checks that the log segments that exist in the target log are as expected.
 * 4. Checks that log segment positions are  exclusive.
 * 5. Checks that "_temp" files and the temp clean shutdown file do no exist.
 * 6. Checks that the number of index files on disk must equal the number loaded into the PersistentIndex.
 * 7. Checks that all index segments except the latest have a bloom file and that the last modified times of index
 * segments are in non decreasing order.
 *
 * Data Integrity checks:
 * Walks through the index entries that existed before compaction and verifies their presence/absence in the target.
 * This ensures that
 * 1. Data that should have been retained, has been.
 * 2. Data that should have been compacted, has been.
 * 3. Ordering of the records in the log hasn't changed.
 * 4. No duplicates in the target (as long as there were none in the source).
 */
public class CompactionVerifier implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionVerifier.class);

  // general and compaction log related
  private final CompactionVerifierConfig config;
  private final CompactionLog cLog;
  private final long compactionStartTimeMs;
  private final long compactionEndTimeMs;
  private final long deleteRefTimeMs;
  private final List<String> segmentsCompactedNames = new ArrayList<>();

  // "src" store related
  private final File srcDir;
  private final Log srcLog;
  private final PersistentIndex srcIndex;

  // "tgt" store related
  private final File tgtDir;
  private final Log tgtLog;
  private final PersistentIndex tgtIndex;

  /**
   * Configuration for the {@link CompactionVerifier}.
   */
  private static class CompactionVerifierConfig {

    /**
     * The path of the directory where the compaction log is. The name of the compaction log should not contain any
     * timestamps (i.e. the name should be how it would be if it had not been renamed after compaction).
     */
    @Config("compaction.log.dir")
    final String cLogDirPath;

    /**
     * The path of the directory where the the pre-compaction store files are.
     */
    @Config("src.store.dir")
    final String srcStoreDirPath;

    /**
     * The path of the directory where the the post-compaction store files are.
     */
    @Config("tgt.store.dir")
    final String tgtStoreDirPath;

    /**
     * The ID of the store (is used to look up the compaction log).
     */
    @Config("store.id")
    final String storeId;

    /**
     * The total capacity of the store.
     */
    @Config("store.capacity")
    final long storeCapacity;

    /**
     * The path to the hardware layout file.
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file.
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * Whether to check data when outside the compaction range. Indexes are always checked (i.e. all source store
     * index entries should be present in the target unless compacted).
     * </p>
     * Please note that this can fail if the source store has not yet undergone hard delete but the target store
     * has. Use with care to avoid spurious failures.
     */
    @Config("check.all.data")
    @Default("true")
    final boolean checkAllData;

    /**
     * Whether to make sure that there are no left over index entries in the target.
     * </p>
     * Please note that this can fail if target has received traffic after being copied from source. Use with care to
     * avoid spurious failures.
     */
    @Config("check.all.entries.in.tgt")
    @Default("true")
    final boolean checkAllEntriesInTgt;

    /**
     * If stray files are found on disk, fails if this is {@code true} and warns if this is {@code false}. It is
     * useful to set to {@code false} when a second compaction has started before the first one could be checked.
     */
    @Config("fail.on.stray.files")
    @Default("true")
    final boolean failOnStrayFiles;

    /**
     * Loads the config.
     * @param verifiableProperties the {@link VerifiableProperties} to load the config from.
     */
    CompactionVerifierConfig(VerifiableProperties verifiableProperties) {
      cLogDirPath = verifiableProperties.getString("compaction.log.dir");
      srcStoreDirPath = verifiableProperties.getString("src.store.dir");
      tgtStoreDirPath = verifiableProperties.getString("tgt.store.dir");
      storeId = verifiableProperties.getString("store.id");
      storeCapacity = verifiableProperties.getLong("store.capacity");
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
      checkAllData = verifiableProperties.getBoolean("check.all.data", true);
      checkAllEntriesInTgt = verifiableProperties.getBoolean("check.all.entries.in.tgt", true);
      failOnStrayFiles = verifiableProperties.getBoolean("fail.on.stray.files", true);
    }
  }

  /**
   * Main function to trigger the verifier.
   * @param args CLI arguments
   * @throws Exception if the verifier encountered problems.
   */
  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    CompactionVerifierConfig verifierConfig = new CompactionVerifierConfig(verifiableProperties);
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    assert !storeConfig.storeEnableHardDelete : "Hard delete cannot be enabled in the properties";
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterMap clusterMap = new StaticClusterAgentsFactory(clusterMapConfig, verifierConfig.hardwareLayoutFilePath,
        verifierConfig.partitionLayoutFilePath).getClusterMap();
    StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
    try (CompactionVerifier compactionVerifier = new CompactionVerifier(verifierConfig, storeConfig, storeKeyFactory)) {
      compactionVerifier.verifyCompaction();
    }
    LOGGER.info("Verification completed successfully");
  }

  /**
   * Constructs a verifier.
   * @param verifierConfig the {@link CompactionVerifierConfig} to use.
   * @param storeConfig the {@link StoreConfig} to use.
   * @param storeKeyFactory the {@link StoreKeyFactory} for the keys in the log and index.
   * @throws IOException if there is any I/O error.
   * @throws StoreException if there is any problem performing store operations.
   */
  private CompactionVerifier(CompactionVerifierConfig verifierConfig, StoreConfig storeConfig,
      StoreKeyFactory storeKeyFactory) throws IOException, StoreException {
    config = verifierConfig;
    srcDir = new File(verifierConfig.srcStoreDirPath);
    tgtDir = new File(verifierConfig.tgtStoreDirPath);

    // load compaction log and perform basic checks
    cLog =
        new CompactionLog(verifierConfig.cLogDirPath, verifierConfig.storeId, storeKeyFactory, SystemTime.getInstance(),
            storeConfig);
    assert cLog.getCompactionPhase().equals(CompactionLog.Phase.DONE) : "Compaction is not finished!";
    assert cLog.cycleLogs.size() > 0 : "There should be at least one cycle of compaction in the compaction log";
    compactionStartTimeMs = cLog.startTime;
    long endTimeMs = Long.MIN_VALUE;
    for (CompactionLog.CycleLog cycleLog : cLog.cycleLogs) {
      segmentsCompactedNames.addAll(cycleLog.compactionDetails.getLogSegmentsUnderCompaction());
      endTimeMs = cycleLog.cycleEndTime;
    }
    compactionEndTimeMs = endTimeMs;
    assert compactionEndTimeMs >= compactionStartTimeMs : "Compaction end time is lower than compaction start time";
    assert segmentsCompactedNames.size()
        >= cLog.cycleLogs.size() : "There should be at least one segmented compacted in each cycle";
    deleteRefTimeMs = cLog.cycleLogs.get(0).compactionDetails.getReferenceTimeMs();

    LOGGER.info("Segments compacted: {}", segmentsCompactedNames);
    LOGGER.info("Compaction start time: {} ms", compactionStartTimeMs);
    LOGGER.info("Compaction end time: {} ms", compactionEndTimeMs);
    LOGGER.info("Delete ref time: {} ms", deleteRefTimeMs);

    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics srcMetrics = new StoreMetrics("src", metricRegistry);
    StoreMetrics tgtMetrics = new StoreMetrics("tgt", metricRegistry);
    UUID sessionId = UUID.randomUUID();
    UUID incarnationId = UUID.randomUUID();
    MessageStoreHardDelete hardDelete = new BlobStoreHardDelete();

    DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);
    DiskSpaceAllocator diskSpaceAllocator =
        new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(metricRegistry));
    // load "src compaction" log and index
    // the segment size is derived from existing log file not from store config, so it's safe to use passed-in store config
    srcLog =
        new Log(srcDir.getAbsolutePath(), verifierConfig.storeCapacity, diskSpaceAllocator, storeConfig, srcMetrics);
    srcIndex = new PersistentIndex(srcDir.getAbsolutePath(), "src", null, srcLog, storeConfig, storeKeyFactory, null,
        hardDelete, diskIOScheduler, srcMetrics, SystemTime.getInstance(), sessionId, incarnationId);

    // load "tgt" compaction log and index
    tgtLog =
        new Log(tgtDir.getAbsolutePath(), verifierConfig.storeCapacity, diskSpaceAllocator, storeConfig, tgtMetrics);
    tgtIndex = new PersistentIndex(tgtDir.getAbsolutePath(), "tgt", null, tgtLog, storeConfig, storeKeyFactory, null,
        hardDelete, diskIOScheduler, tgtMetrics, SystemTime.getInstance(), sessionId, incarnationId);
  }

  @Override
  public void close() throws IOException {
    try {
      srcIndex.close(false);
      tgtIndex.close(false);
    } catch (StoreException e) {
      throw new IOException(e);
    }
    srcLog.close(false);
    tgtLog.close(false);
    cLog.close();
  }

  /**
   * Verifies the compaction by checking structure and data integrity.
   * @throws IOException if there is any I/O error.
   * @throws StoreException if there is any problem performing store operations.
   */
  private void verifyCompaction() throws IOException, StoreException {
    verifyStructure();
    verifyData();
  }

  /**
   * Verifies that the structure of the store is consistent (both in mem and on disk). See the documentation of the
   * class for details on the verifications.
   */
  private void verifyStructure() {
    LOGGER.info("Verifying structure");
    List<String> srcSegmentNames = getSegmentNames(srcLog);
    List<String> tgtSegmentNames = getSegmentNames(tgtLog);
    List<String> expectedTgtSegmentNames = new ArrayList<>();

    // 1. Check that log segments that should exist, still exist and ones that should not, no longer do
    for (String srcSegmentName : srcSegmentNames) {
      if (segmentsCompactedNames.contains(srcSegmentName)) {
        assert !tgtSegmentNames.contains(srcSegmentName) :
            "Compacted log should not contain segment: " + srcSegmentName;
      } else {
        assert tgtSegmentNames.contains(srcSegmentName) : "Compacted log is missing segment: " + srcSegmentName;
        expectedTgtSegmentNames.add(srcSegmentName);
      }
    }

    // 2. Check that log segments that are newly created have the right generation number
    for (String segmentCompactedName : segmentsCompactedNames) {
      String tgtSegmentName = LogSegmentNameHelper.getNextGenerationName(segmentCompactedName);
      if (tgtSegmentNames.contains(tgtSegmentName)) {
        expectedTgtSegmentNames.add(tgtSegmentName);
      }
    }

    // 3. Check that the log segments that exist in the tgt are as expected
    Collections.sort(expectedTgtSegmentNames, LogSegmentNameHelper.COMPARATOR);
    assert tgtSegmentNames.equals(expectedTgtSegmentNames) : "Segment names in target log not as expected";

    // 4. Positions should be exclusive
    Set<Long> positionsSeen = new HashSet<>();
    for (String tgtSegmentName : tgtSegmentNames) {
      long position = LogSegmentNameHelper.getPosition(tgtSegmentName);
      assert !positionsSeen.contains(position) : "There are two log segments at position: " + position;
      positionsSeen.add(position);
    }

    int numTempFilesOnDisk = tgtDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length;
    int filesOnDiskCount = tgtDir.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER).length;
    int inMemIndexSegmentCount = tgtIndex.getIndexSegments().size();
    boolean tempCleanupFileExists = new File(tgtDir, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists();

    // 5. "_temp" files and the temp clean shutdown file should not exist
    // 6. The number of index files on disk must equal the number loaded into the PersistentIndex

    if (config.failOnStrayFiles) {
      assert numTempFilesOnDisk == 0 : "Some log segments haven't been cleaned";
      assert !tempCleanupFileExists : "The temp clean shutdown file has not been deleted";
      assert filesOnDiskCount == inMemIndexSegmentCount : "There are stray index segment files on disk";
    } else {
      if (numTempFilesOnDisk != 0) {
        LOGGER.warn("There are {} temp log segment files on disk", numTempFilesOnDisk);
      }
      if (tempCleanupFileExists) {
        LOGGER.warn("A temp clean shutdown file exists");
      }
      if (filesOnDiskCount != inMemIndexSegmentCount) {
        LOGGER.warn("There are {} index segment files on disk but {} in the in-mem index", filesOnDiskCount,
            inMemIndexSegmentCount);
      }
    }

    // 7. All index segments except the latest must have a bloom file and last modified times must be in non decreasing
    // order.
    long prevLastModTimeMs = Long.MIN_VALUE;
    Offset lastSegmentStartOffset = tgtIndex.getIndexSegments().lastKey();
    for (IndexSegment indexSegment : tgtIndex.getIndexSegments().values()) {
      if (!indexSegment.getStartOffset().equals(lastSegmentStartOffset)) {
        File indexSegmentFile = indexSegment.getFile();
        String bloomFileName = indexSegmentFile.getName()
            .replace(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX, IndexSegment.BLOOM_FILE_NAME_SUFFIX);
        assert new File(tgtDir, bloomFileName).exists() : "There is no bloom file for: " + indexSegmentFile;
      }
      long lastModTimeMs = indexSegment.getLastModifiedTimeMs();
      assert
          lastModTimeMs >= prevLastModTimeMs :
          "Last modified time of " + indexSegment.getStartOffset() + " (" + lastModTimeMs + ") is "
              + "lesser than predecessor (" + prevLastModTimeMs + ")";
      prevLastModTimeMs = indexSegment.getLastModifiedTimeMs();
    }

    LOGGER.info("Verified structure");
  }

  /**
   * Verifies the data in the store by walking through all the index entries in the source and verifying their
   * presence/absence in the target. Also implicitly verifies that ordering is unmodified.
   * @throws IOException if there is any I/O error.
   * @throws StoreException if there is any problem performing store operations.
   */
  private void verifyData() throws IOException, StoreException {
    LOGGER.info("Verifying data");
    IndexEntriesIterator srcEntriesIterator = new IndexEntriesIterator(srcIndex);
    IndexEntriesIterator tgtEntriesIterator = new IndexEntriesIterator(tgtIndex);

    while (srcEntriesIterator.hasNext()) {
      IndexEntry srcEntry = srcEntriesIterator.next();
      StoreKey key = srcEntry.getKey();
      IndexValue srcValue = srcEntry.getValue();
      String errMsgId = getErrorMessageId(srcEntry, srcIndex);

      boolean shouldVerifyRecord;
      boolean wasProcessedForCompaction = wasProcessedForCompaction(srcValue.getOffset());
      if (wasProcessedForCompaction) {
        if (!srcValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          // put entry
          IndexValue valueFromSrcIndex = srcIndex.findKey(key);
          IndexValue valueFromTgtIndex = tgtIndex.findKey(key);

          // expiry checks
          if (isExpiredAt(srcValue, compactionStartTimeMs)) {
            // put entry that had surely expired at compaction start time - so should have been cleaned up
            assert !isPutRecordPresentInTgt(key, valueFromTgtIndex) :
                errMsgId + ": Found an expired PUT record that should have been cleaned up in target index."
                    + " IndexVale: " + srcValue;
            shouldVerifyRecord = false;
          } else if (isExpiredAt(srcValue, compactionEndTimeMs)) {
            // put entry that had expired at end time but might not have been cleaned up because it wasn't expired
            // when examined
            shouldVerifyRecord = isPutRecordPresentInTgt(key, valueFromTgtIndex);
          } else {
            // not expired at compaction time
            shouldVerifyRecord = true;
          }
          if (shouldVerifyRecord && valueFromSrcIndex.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // blob has been deleted. Find the time of the delete
            IndexSegment indexSegment =
                srcIndex.getIndexSegments().floorEntry(valueFromSrcIndex.getOffset()).getValue();
            long deleteTimeMs = valueFromSrcIndex.getOperationTimeInMs();
            if (deleteTimeMs == Utils.Infinite_Time) {
              deleteTimeMs = indexSegment.getLastModifiedTimeMs();
            }
            if (deleteRefTimeMs <= deleteTimeMs) {
              shouldVerifyRecord = true;
            } else if (indexSegment.getStartOffset().equals(srcIndex.getIndexSegments().lastKey())) {
              // if the delete lies in the last index segment of the srcIndex, then it is possible that the
              // last modified time of the index segment has changed in the tgtIndex (due to traffic). If this is the
              // case, it is possible that the PUT record has not been cleaned up depending on when compaction ran
              shouldVerifyRecord = isPutRecordPresentInTgt(key, valueFromTgtIndex);
            } else {
              // PUT record should no longer be present
              assert !isPutRecordPresentInTgt(key, valueFromTgtIndex) :
                  errMsgId + ": Found a deleted PUT record that should have been cleaned up in target index."
                      + " IndexVale: " + srcValue;
              shouldVerifyRecord = false;
            }
          }
          if (!config.checkAllEntriesInTgt && shouldVerifyRecord && valueFromTgtIndex.isFlagSet(
              IndexValue.Flags.Delete_Index)) {
            // deleted in the target index. Because the start offset of the index segment until which deletes are valid
            // is calculated before compaction, it is possible that the LMT of the index segment changed after this
            // calculation was done. This can happen only if there has been no write to the index segment for the past
            // 7 days and only if the tgt is expected to have more entries than the src.
            Offset startOffsetDelIdxSegInTgt = tgtIndex.getIndexSegments().floorKey(valueFromTgtIndex.getOffset());
            if (startOffsetDelIdxSegInTgt.equals(srcIndex.getIndexSegments().lastKey())) {
              // if the delete lies in the last index segment that tgtIndex has in common with srcIndex, then it is
              // possible that the last modified time of the index segment has changed in the tgtIndex (due to traffic).
              // If this is the case, it is possible that the PUT record has been cleaned up depending on when the start
              // offset for delete was calculated.
              shouldVerifyRecord = isPutRecordPresentInTgt(key, valueFromTgtIndex);
            }
          }
        } else {
          // delete entry. Should exist in the target log/index
          shouldVerifyRecord = true;
        }
      } else {
        // never underwent compaction. Needs to be checked.
        shouldVerifyRecord = true;
      }
      if (shouldVerifyRecord) {
        verifyRecord(errMsgId, srcEntry, tgtEntriesIterator, wasProcessedForCompaction || config.checkAllData);
      }
    }

    assert !config.checkAllEntriesInTgt
        || !tgtEntriesIterator.hasNext() : "There should be no more entries in the target index";
  }

  /**
   * @param offset the {@link Offset} to check.
   * @return {@code true} if the offset was processed in the compaction. {@code false} otherwise
   */
  private boolean wasProcessedForCompaction(Offset offset) {
    return segmentsCompactedNames.contains(offset.getName());
  }

  /**
   * @param entry the {@link IndexEntry} being examined.
   * @param index the {@link PersistentIndex} that {@code entry} belongs to.
   * @return a string that can uniquely identify an entry in error messages.
   */
  private String getErrorMessageId(IndexEntry entry, PersistentIndex index) {
    IndexValue value = entry.getValue();
    Offset indexSegmentStartOffset = index.getIndexSegments().floorKey(value.getOffset());
    String kind = value.isFlagSet(IndexValue.Flags.Delete_Index) ? "DELETE" : "PUT";
    return "[" + indexSegmentStartOffset + ":" + entry.getKey() + ":" + kind + "]";
  }

  /**
   * @param log the log whose segment names are required.
   * @return list of the names of all the segments in {@code log}
   */
  private List<String> getSegmentNames(Log log) {
    List<String> names = new ArrayList<>();
    LogSegment segment = log.getFirstSegment();
    while (segment != null) {
      names.add(segment.getName());
      segment = log.getNextSegment(segment);
    }
    return names;
  }

  /**
   * @param logSegment the {@link LogSegment} to get the data from.
   * @param offset the offset inside {@code logSegment} to start the read at.
   * @param size the size of the data that must be read.
   * @return the bytes that were read.
   * @throws IOException if there was an I/O error while reading.
   */
  private byte[] getDataFromLogSegment(LogSegment logSegment, long offset, long size) throws IOException {
    assert size <= Integer.MAX_VALUE : "Cannot read more than " + Integer.MAX_VALUE + " bytes of data";
    byte[] data = new byte[(int) size];
    FileChannel fileChannel = logSegment.getView().getSecond();
    try {
      fileChannel.read(ByteBuffer.wrap(data), offset);
    } finally {
      logSegment.closeView();
    }
    return data;
  }

  /**
   * @param value the {@link IndexValue} to check.
   * @param refTimeMs the reference time to use to check for expiry.
   * @return {@code true} if {@code value} is considered expired at {@code refTimeMs}. {@code false} otherwise.
   */
  private boolean isExpiredAt(IndexValue value, long refTimeMs) {
    return value.getExpiresAtMs() != Utils.Infinite_Time && refTimeMs > value.getExpiresAtMs();
  }

  /**
   * @param key the {@link StoreKey} whose presence is being tested.
   * @param valueFromTgtIndex the {@link IndexValue} that was obtained from the {@link #tgtIndex}. Can be {@code null}.
   * @return {@code true} if the PUT record for {@code key} is present in {@link #tgtIndex}. {@code false} otherwise.
   */
  private boolean isPutRecordPresentInTgt(StoreKey key, IndexValue valueFromTgtIndex) {
    boolean putPresent = false;
    if (valueFromTgtIndex != null) {
      // there is an entry in the target index, ensure that there is no PUT record.
      try (BlobReadOptions options = tgtIndex.getBlobReadInfo(key, EnumSet.allOf(StoreGetOptions.class))) {
        putPresent = true;
      } catch (StoreException e) {
        // this means that the PUT entry no longer exists.
      }
    }
    return putPresent;
  }

  /**
   * Verifies that:
   * 1. Index entry metadata in the source == Index entry metadata in the target
   * 2. Log data in the source == Log data in the target.
   * @param errMsgId an unique identifier that will be printed with error messages to help debugging.
   * @param srcEntry the {@link IndexEntry} in the source store.
   * @param tgtEntriesIterator the {@link Iterator} for index entries in the target index.
   * @param verifyLogData {@code true} if the data in the log needs to be verified. {@code false} otherwise.
   * @throws IOException if there is any I/O error reading from the log/index.
   */
  private void verifyRecord(String errMsgId, IndexEntry srcEntry, IndexEntriesIterator tgtEntriesIterator,
      boolean verifyLogData) throws IOException {
    assert tgtEntriesIterator.hasNext() : "There are no more entries in the target index";
    IndexEntry tgtEntry = tgtEntriesIterator.next();

    IndexValue srcValue = srcEntry.getValue();
    IndexValue tgtValue = tgtEntry.getValue();

    assert srcEntry.getKey().equals(tgtEntry.getKey()) :
        errMsgId + ": Key mismatch: old - " + srcEntry.getKey() + ", new - " + tgtEntry.getKey();
    assert
        srcValue.getFlags() == tgtValue.getFlags() :
        errMsgId + ": Flags mismatch: old - " + srcValue.getFlags() + ", new - " + tgtValue.getFlags();
    assert
        srcValue.getSize() == tgtValue.getSize() :
        errMsgId + ": Size mismatch: old - " + srcValue.getSize() + ", new - " + tgtValue.getSize();
    assert Utils.getTimeInMsToTheNearestSec(srcValue.getExpiresAtMs()) == Utils.getTimeInMsToTheNearestSec(
        tgtValue.getExpiresAtMs()) :
        errMsgId + ": ExpiresAt mismatch: old - " + srcValue.getExpiresAtMs() + ", new - " + tgtValue.getExpiresAtMs();
    assert
        srcValue.getAccountId() == tgtValue.getAccountId() :
        errMsgId + ": AccountId ID mismatch: old - " + srcValue.getAccountId() + ", new - " + tgtValue.getAccountId();
    assert
        srcValue.getContainerId() == tgtValue.getContainerId() :
        errMsgId + ": Container ID mismatch: old - " + srcValue.getContainerId() + ", new - "
            + tgtValue.getContainerId();
    if (verifyLogData) {
      LogSegment srcLogSegment = srcLog.getSegment(srcValue.getOffset().getName());
      LogSegment tgtLogSegment = tgtLog.getSegment(tgtValue.getOffset().getName());
      byte[] srcBlob = getDataFromLogSegment(srcLogSegment, srcValue.getOffset().getOffset(), srcValue.getSize());
      byte[] tgtBlob = getDataFromLogSegment(tgtLogSegment, tgtValue.getOffset().getOffset(), tgtValue.getSize());
      for (int i = 0; i < srcBlob.length; i++) {
        assert srcBlob[i] == tgtBlob[i] : errMsgId + " Data does not match at index: " + i;
      }
    }
  }

  /**
   * An {@link Iterator} for iterating through all the index entries in a {@link PersistentIndex}.
   */
  private static class IndexEntriesIterator implements Iterator<IndexEntry> {
    private final PersistentIndex index;
    private final ConcurrentSkipListMap<Offset, IndexSegment> indexSegmentsByOffset;
    private Offset currentIndexSegmentStartOffset = null;
    private Iterator<IndexEntry> indexEntriesIterator = Collections.EMPTY_LIST.iterator();

    /**
     * Creates an {@link Iterator} that can iterate through all the index entries in {@code index}.
     * @param index the {@link PersistentIndex} whose entries need to be iterated on.
     */
    IndexEntriesIterator(PersistentIndex index) {
      this.index = index;
      indexSegmentsByOffset = index.getIndexSegments();
      if (indexSegmentsByOffset.size() > 0) {
        loadEntriesFromNextIndexSegment();
      }
    }

    @Override
    public boolean hasNext() {
      return indexEntriesIterator.hasNext() || hasMoreIndexSegments();
    }

    @Override
    public IndexEntry next() {
      if (!hasNext()) {
        throw new IllegalStateException("Called next() when hasNext() is false");
      }
      if (!indexEntriesIterator.hasNext()) {
        loadEntriesFromNextIndexSegment();
      }
      return indexEntriesIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Removing is not supported");
    }

    /**
     * Loads entries from the next index segment.
     */
    private void loadEntriesFromNextIndexSegment() {
      currentIndexSegmentStartOffset = currentIndexSegmentStartOffset == null ? indexSegmentsByOffset.firstKey()
          : indexSegmentsByOffset.higherKey(currentIndexSegmentStartOffset);
      IndexSegment indexSegment = indexSegmentsByOffset.get(currentIndexSegmentStartOffset);
      List<IndexEntry> indexEntries = new ArrayList<>();
      try {
        indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), indexEntries,
            new AtomicLong(0), true);
        // for each index entry, if it represents a squashed put entry, add an index entry to account for that.
        List<IndexEntry> entriesToAdd = new ArrayList<>();
        for (IndexEntry indexEntry : indexEntries) {
          IndexValue value = indexEntry.getValue();
          long origMsgOffset = value.getOriginalMessageOffset();
          if (value.isFlagSet(IndexValue.Flags.Delete_Index)
              && origMsgOffset != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET
              && origMsgOffset >= indexSegment.getStartOffset().getOffset()) {
            try (BlobReadOptions options = index.getBlobReadInfo(indexEntry.getKey(),
                EnumSet.allOf(StoreGetOptions.class))) {
              Offset putOffset = new Offset(indexSegment.getLogSegmentName(), options.getOffset());
              IndexValue putValue = new IndexValue(options.getMessageInfo().getSize(), putOffset,
                  options.getMessageInfo().getExpirationTimeInMs(), value.getOperationTimeInMs(), value.getAccountId(),
                  value.getContainerId());
              entriesToAdd.add(new IndexEntry(indexEntry.getKey(), putValue));
            }
          }
        }
        indexEntries.addAll(entriesToAdd);
        Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
        LOGGER.info("Loaded entries from {}", indexSegment.getFile());
      } catch (StoreException e) {
        throw new IllegalStateException(e);
      }
      indexEntriesIterator = indexEntries.iterator();
    }

    /**
     * @return {@code true} if there are index segments still left to fetch entries from. {@code false} otherwise.
     */
    private boolean hasMoreIndexSegments() {
      return currentIndexSegmentStartOffset != null && !currentIndexSegmentStartOffset.equals(
          indexSegmentsByOffset.lastKey());
    }
  }
}
