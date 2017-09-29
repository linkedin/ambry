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
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping indices in Ambry
 * Supported operations are
 * 1. Dump index.
 * 2. Dump an index segment.
 * 3. Verify an index for sanity (no duplicate records and that all index segments are deserializable).
 */
public class DumpIndexTool {

  /**
   * Contains information for a single blob regarding
   * 1. Its states (i.e. it could be deleted and expired).
   * 2. Whether it is in one of the two most recent index segments.
   */
  public class Info {
    private final EnumSet<BlobState> states;
    private final boolean isInRecentIndexSegment;

    public Info(EnumSet<BlobState> states, boolean isInRecentIndexSegment) {
      this.states = states;
      this.isInRecentIndexSegment = isInRecentIndexSegment;
    }

    /**
     * @return states the blob is in in the index.
     */
    public EnumSet<BlobState> getStates() {
      return states;
    }

    /**
     * @return {@code true} if the blob is one of the last two index segments in the index.
     */
    public boolean isInRecentIndexSegment() {
      return isInRecentIndexSegment;
    }
  }

  /**
   * Contains all the results obtained from processing the index.
   */
  public class IndexProcessingResults {
    private final Map<StoreKey, Info> keyToState;
    private final long processedCount;
    private final long putCount;
    private final long deleteCount;
    private final Set<StoreKey> duplicatePuts;
    private final Set<StoreKey> putAfterDeletes;
    private final Set<StoreKey> duplicateDeletes;
    private final long activeCount;

    IndexProcessingResults(Map<StoreKey, Info> keyToState, long processedCount, long putCount, long deleteCount,
        Set<StoreKey> duplicatePuts, Set<StoreKey> putAfterDeletes, Set<StoreKey> duplicateDeletes) {
      this.keyToState = keyToState;
      this.processedCount = processedCount;
      this.putCount = putCount;
      this.deleteCount = deleteCount;
      this.duplicatePuts = duplicatePuts;
      this.putAfterDeletes = putAfterDeletes;
      this.duplicateDeletes = duplicateDeletes;
      activeCount = keyToState.values().stream().filter(value -> value.getStates().contains(BlobState.Valid)).count();
    }

    /**
     * @return the map of {@link StoreKey} to an {@link Info} object that contains some details about it.
     */
    public Map<StoreKey, Info> getKeyToState() {
      return keyToState;
    }

    /**
     * @return the number of entries processed.
     */
    public long getProcessedCount() {
      return processedCount;
    }

    /**
     * @return the number of {@link StoreKey}s that are valid (i.e. not deleted or expired).
     */
    public long getActiveCount() {
      return activeCount;
    }

    /**
     * @return the total number of put entries.
     */
    public long getPutCount() {
      return putCount;
    }

    /**
     * @return the total number of delete entries.
     */
    public long getDeleteCount() {
      return deleteCount;
    }

    /**
     * @return the number of duplicate put entries found.
     */
    public Set<StoreKey> getDuplicatePuts() {
      return duplicatePuts;
    }

    /**
     * @return the number of put entries that were found after a delete entry for the same key.
     */
    public Set<StoreKey> getPutAfterDeletes() {
      return putAfterDeletes;
    }

    /**
     * @return the number of duplicate delete entries found.
     */
    public Set<StoreKey> getDuplicateDeletes() {
      return duplicateDeletes;
    }

    /**
     * @return {@code true} if there are no duplicate puts or deletes and no puts after deletes. {@code false} otherwise
     */
    public boolean isIndexSane() {
      return duplicatePuts.size() == 0 && putAfterDeletes.size() == 0 && duplicateDeletes.size() == 0;
    }

    @Override
    public String toString() {
      return "Processed: " + processedCount + ", Active: " + activeCount + ", Put: " + putCount + ", Delete: "
          + deleteCount + ", Duplicate Puts: " + duplicatePuts + ", Put After Delete: " + putAfterDeletes
          + ", Duplicate Delete: " + duplicateDeletes;
    }
  }

  /**
   * The possible states of a blob.
   */
  public enum BlobState {
    Valid, Deleted, Expired;
  }

  /**
   * The different operations supported by the tool.
   */
  private enum Operation {
    /**
     * Processes all the index segments and dumps all or the filtered entries.
     */
    DumpIndex, /**
     * Processes the given index segment and dumps all or the filtered entries.
     */
    DumpIndexSegment, /**
     * Processes all the index segments (deserialization check) and makes sure that there are no duplicate records
     * and no put after delete records.
     */
    VerifyIndex
  }

  /**
   * Config for the DumpIndexTool.
   */
  private static class DumpIndexToolConfig {

    /**
     * The type of operation.
     * Operations are: DumpIndex, DumpIndexSegment, VerifyIndex
     */
    @Config("type.of.operation")
    final Operation typeOfOperation;

    /**
     * The path where the input is. This may differ for different operations. For e.g., this is a file for
     * {@link Operation#DumpIndexSegment} but is a directory for {@link Operation#DumpIndex} and
     * {@link Operation#VerifyIndex}.
     */
    @Config("path.of.input")
    final File pathOfInput;

    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    /**
     * A comma separated list of blob IDs that the tool should operate on. Leaving this empty indicates that the tool
     * should work on all blobs.
     * For DumpIndex and DumpIndexSegment, this will be the list of blobs whose entries will be dumped.
     * For VerifyIndex, this will be the list of blobs whose entries are examined.
     */
    @Config("filter.set")
    @Default("")
    final Set<String> filterSet;

    /**
     * The number of index entries to process every second.
     */
    @Config("index.entries.to.process.per.sec")
    @Default("Long.MAX_VALUE")
    final long indexEntriesToProcessPerSec;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    DumpIndexToolConfig(VerifiableProperties verifiableProperties) {
      typeOfOperation = Operation.valueOf(verifiableProperties.getString("type.of.operation"));
      pathOfInput = new File(verifiableProperties.getString("path.of.input"));
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      String filterSetStr = verifiableProperties.getString("filter.set", "");
      if (!filterSetStr.isEmpty()) {
        filterSet = new HashSet<>(Arrays.asList(filterSetStr.split(",")));
      } else {
        filterSet = Collections.EMPTY_SET;
      }
      indexEntriesToProcessPerSec =
          verifiableProperties.getLongInRange("index.entries.to.process.per.sec", Long.MAX_VALUE, 1, Long.MAX_VALUE);
    }
  }

  private final StoreKeyFactory storeKeyFactory;
  private final StoreConfig storeConfig;
  private final StoreMetrics storeMetrics;
  private final StoreToolsMetrics metrics;
  private final Time time;

  private static final Logger logger = LoggerFactory.getLogger(DumpIndexTool.class);

  public DumpIndexTool(StoreKeyFactory storeKeyFactory, StoreConfig storeConfig, Time time, StoreToolsMetrics metrics,
      StoreMetrics storeMetrics) {
    this.storeKeyFactory = storeKeyFactory;
    this.storeConfig = storeConfig;
    this.time = time;
    this.metrics = metrics;
    this.storeMetrics = storeMetrics;
  }

  public static void main(String args[]) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    DumpIndexToolConfig config = new DumpIndexToolConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    try (ClusterMap clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory,
        clusterMapConfig, config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap()) {
      StoreConfig storeConfig = new StoreConfig(verifiableProperties);
      // this tool supports only blob IDs. It can become generic if StoreKeyFactory provides a deserFromString method.
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      StoreToolsMetrics metrics = new StoreToolsMetrics(clusterMap.getMetricRegistry());
      StoreMetrics storeMetrics = new StoreMetrics("DumpIndexTool", clusterMap.getMetricRegistry());
      Time time = SystemTime.getInstance();
      DumpIndexTool dumpIndexTool = new DumpIndexTool(blobIdFactory, storeConfig, time, metrics, storeMetrics);
      Set<StoreKey> filterKeySet = new HashSet<>();
      for (String key : config.filterSet) {
        filterKeySet.add(new BlobId(key, clusterMap));
      }
      switch (config.typeOfOperation) {
        case DumpIndex:
          dumpIndex(dumpIndexTool, config.pathOfInput, filterKeySet);
          break;
        case DumpIndexSegment:
          dumpIndexSegment(dumpIndexTool, config.pathOfInput, filterKeySet);
          break;
        case VerifyIndex:
          Throttler throttler = new Throttler(config.indexEntriesToProcessPerSec, 1000, true, time);
          IndexProcessingResults results =
              dumpIndexTool.processIndex(config.pathOfInput, filterKeySet, time.milliseconds(), throttler);
          if (results.isIndexSane()) {
            logger.info("Index of {} is well formed and without errors", config.pathOfInput);
          } else {
            logger.error("Index of {} has errors", config.pathOfInput);
          }
          logger.info("Processing results: {}", results);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized operation: " + config.typeOfOperation);
      }
    }
  }

  private static void dumpIndex(DumpIndexTool dumpIndexTool, File replicaDir, Set<StoreKey> filterSet)
      throws IOException, StoreException {
    verifyPath(replicaDir, true);
    logger.info("Dumping indexes from {}", replicaDir);
    File[] segmentFiles = getSegmentFilesFromDir(replicaDir);
    for (File segmentFile : segmentFiles) {
      dumpIndexSegment(dumpIndexTool, segmentFile, filterSet);
    }
  }

  private static void dumpIndexSegment(DumpIndexTool dumpIndexTool, File segmentFile, Set<StoreKey> filterSet)
      throws IOException, StoreException {
    verifyPath(segmentFile, false);
    List<IndexEntry> entries = dumpIndexTool.getAllEntriesFromIndexSegment(segmentFile);
    logger.info("Dumping {}", segmentFile);
    for (IndexEntry entry : entries) {
      if (filterSet == null || filterSet.isEmpty() || filterSet.contains(entry.getKey())) {
        logger.info("Key: {}, Value: {}", entry.getKey(), entry.getValue());
      }
    }
  }

  public IndexProcessingResults processIndex(File replicaDir, Set<StoreKey> filterSet, long currentTimeMs,
      Throttler throttler) throws InterruptedException, IOException, StoreException {
    verifyPath(replicaDir, true);
    final Timer.Context context = metrics.dumpReplicaIndexesTimeMs.time();
    try {
      File[] segmentFiles = getSegmentFilesFromDir(replicaDir);
      SortedMap<File, List<IndexEntry>> segmentFileToIndexEntries = getAllEntriesFromIndex(segmentFiles, throttler);
      Map<StoreKey, Info> keyToState = new HashMap<>();
      long processedCount = 0;
      long putCount = 0;
      long deleteCount = 0;
      Set<StoreKey> duplicatePuts = new HashSet<>();
      Set<StoreKey> putAfterDeletes = new HashSet<>();
      Set<StoreKey> duplicateDeletes = new HashSet<>();
      long indexSegmentCount = segmentFileToIndexEntries.size();
      for (int i = 0; i < indexSegmentCount; i++) {
        List<IndexEntry> entries = segmentFileToIndexEntries.get(segmentFiles[i]);
        boolean isInRecentIndexSegment = i >= indexSegmentCount - 2;
        processedCount += entries.size();
        for (IndexEntry entry : entries) {
          StoreKey key = entry.getKey();
          if (filterSet == null || filterSet.isEmpty() || filterSet.contains(key)) {
            boolean isDelete = entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index);
            boolean isExpired = DumpDataHelper.isExpired(entry.getValue().getExpiresAtMs(), currentTimeMs);
            EnumSet<BlobState> states = isExpired ? EnumSet.of(BlobState.Expired) : EnumSet.noneOf(BlobState.class);
            if (isDelete) {
              deleteCount++;
              states.add(BlobState.Deleted);
            } else {
              putCount++;
              if (!isExpired) {
                states.add(BlobState.Valid);
              }
            }
            Info info = keyToState.get(key);
            if (info == null) {
              keyToState.put(key, new Info(states, isInRecentIndexSegment));
            } else if (info.states.contains(BlobState.Deleted)) {
              if (isDelete) {
                duplicateDeletes.add(key);
              } else {
                putAfterDeletes.add(key);
              }
            } else {
              if (isDelete) {
                keyToState.put(key, new Info(states, isInRecentIndexSegment));
              } else {
                duplicatePuts.add(key);
              }
            }
          }
        }
      }
      return new IndexProcessingResults(keyToState, processedCount, putCount, deleteCount, duplicatePuts,
          putAfterDeletes, duplicateDeletes);
    } finally {
      context.stop();
    }
  }

  public SortedMap<File, List<IndexEntry>> getAllEntriesFromIndex(File[] segmentFiles, Throttler throttler)
      throws InterruptedException, IOException, StoreException {
    SortedMap<File, List<IndexEntry>> fileToIndexEntries = new TreeMap<>(PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    for (File segmentFile : segmentFiles) {
      List<IndexEntry> entries = getAllEntriesFromIndexSegment(segmentFile);
      fileToIndexEntries.put(segmentFile, entries);
      if (throttler != null) {
        throttler.maybeThrottle(entries.size());
      }
    }
    return fileToIndexEntries;
  }

  public List<IndexEntry> getAllEntriesFromIndexSegment(File segmentFile) throws IOException, StoreException {
    verifyPath(segmentFile, false);
    IndexSegment segment = new IndexSegment(segmentFile, false, storeKeyFactory, storeConfig, storeMetrics,
        new Journal(segmentFile.getParent(), 0, 0), time);
    List<IndexEntry> entries = new ArrayList<>();
    final Timer.Context context = metrics.findAllEntriesPerIndexTimeMs.time();
    try {
      segment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0));
    } finally {
      context.stop();
    }
    return entries;
  }

  private static File[] getSegmentFilesFromDir(File dir) {
    File[] segmentFiles = dir.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER);
    Arrays.sort(segmentFiles, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    return segmentFiles;
  }

  private static void verifyPath(File file, boolean shouldBeDir) {
    if (!file.exists()) {
      throw new IllegalArgumentException(file + " does not exist");
    }
    if (shouldBeDir && !file.isDirectory()) {
      throw new IllegalArgumentException(file + " should be a directory");
    }
  }
}
