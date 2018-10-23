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
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
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
    private boolean seenTtlUpdate = false;
    private boolean isPermanent = false;

    public Info(EnumSet<BlobState> states, boolean isInRecentIndexSegment, boolean isPermanent) {
      this.states = states;
      this.isInRecentIndexSegment = isInRecentIndexSegment;
      this.isPermanent = isPermanent;
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

    /**
     * Marks that a TTL update has been seen
     */
    public void markTtlUpdateSeen() {
      seenTtlUpdate = true;
      isPermanent = true;
    }

    /**
     * @return {@code true} if a TTL update has been seen
     */
    public boolean isTtlUpdateSeen() {
      return seenTtlUpdate;
    }

    /**
     * @return {@code true} if the blob is permanent
     */
    public boolean isPermanent() {
      return isPermanent;
    }
  }

  /**
   * Contains all the results obtained from processing the index.
   */
  public class IndexProcessingResults {
    private final Map<StoreKey, Info> keyToState;
    private final long processedCount;
    private final long putCount;
    private final long ttlUpdateCount;
    private final long deleteCount;
    private final Set<StoreKey> duplicatePuts;
    private final Set<StoreKey> putAfterUpdates;
    private final Set<StoreKey> duplicateDeletes;
    private final Set<StoreKey> duplicateUpdates;
    private final Set<StoreKey> updateAfterDeletes;
    private final long activeCount;

    IndexProcessingResults(Map<StoreKey, Info> keyToState, long processedCount, long putCount, long ttlUpdateCount,
        long deleteCount, Set<StoreKey> duplicatePuts, Set<StoreKey> putAfterUpdates, Set<StoreKey> duplicateDeletes,
        Set<StoreKey> duplicateUpdates, Set<StoreKey> updateAfterDeletes) {
      this.keyToState = keyToState;
      this.processedCount = processedCount;
      this.putCount = putCount;
      this.ttlUpdateCount = ttlUpdateCount;
      this.deleteCount = deleteCount;
      this.duplicatePuts = duplicatePuts;
      this.putAfterUpdates = putAfterUpdates;
      this.duplicateDeletes = duplicateDeletes;
      this.duplicateUpdates = duplicateUpdates;
      this.updateAfterDeletes = updateAfterDeletes;
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
     * @return the total number of ttl update entries.
     */
    public long getTtlUpdateCount() {
      return ttlUpdateCount;
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
     * @return the number of put entries that were found after a update (ttl update, delete etc) entry for the same key.
     */
    public Set<StoreKey> getPutAfterUpdates() {
      return putAfterUpdates;
    }

    /**
     * @return the number of duplicate delete entries found.
     */
    public Set<StoreKey> getDuplicateDeletes() {
      return duplicateDeletes;
    }

    /**
     * @return the number of duplicate update entries found.
     */
    public Set<StoreKey> getDuplicateUpdates() {
      return duplicateUpdates;
    }

    /**
     * @return the number of update entries that were found after a delete (ttl update) entry for the same key.
     */
    public Set<StoreKey> getUpdateAfterDeletes() {
      return updateAfterDeletes;
    }

    /**
     * @return {@code true} if there are no duplicate puts or deletes or updates and no puts after updates or updates
     * after deletes. {@code false} otherwise
     */
    public boolean isIndexSane() {
      return duplicatePuts.size() == 0 && putAfterUpdates.size() == 0 && duplicateDeletes.size() == 0
          && duplicateUpdates.size() == 0 && updateAfterDeletes.size() == 0;
    }

    @Override
    public String toString() {
      return "Processed: " + processedCount + ", Active: " + activeCount + ", Put: " + putCount + ", TTL update count: "
          + ttlUpdateCount + ", Delete: " + deleteCount + ", Duplicate Puts: " + duplicatePuts + ", Put After Update: "
          + putAfterUpdates + ", Duplicate Delete: " + duplicateDeletes + ", Duplicate Update: " + duplicateUpdates
          + ", Update After Delete: " + updateAfterDeletes;
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
  private final Throttler throttler;
  private final StoreKeyConverter storeKeyConverter;

  private static final Logger logger = LoggerFactory.getLogger(DumpIndexTool.class);

  public DumpIndexTool(StoreKeyFactory storeKeyFactory, StoreConfig storeConfig, Time time, StoreToolsMetrics metrics,
      StoreMetrics storeMetrics, Throttler throttler, StoreKeyConverter storeKeyConverter) {
    this.storeKeyFactory = storeKeyFactory;
    this.storeConfig = storeConfig;
    this.time = time;
    this.metrics = metrics;
    this.storeMetrics = storeMetrics;
    this.throttler = throttler;
    this.storeKeyConverter = storeKeyConverter;
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
      ServerConfig serverConfig = new ServerConfig(verifiableProperties);
      Time time = SystemTime.getInstance();
      Throttler throttler = new Throttler(config.indexEntriesToProcessPerSec, 1000, true, time);
      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, verifiableProperties,
              clusterMap.getMetricRegistry());
      DumpIndexTool dumpIndexTool =
          new DumpIndexTool(blobIdFactory, storeConfig, time, metrics, storeMetrics, throttler,
              storeKeyConverterFactory.getStoreKeyConverter());
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
          IndexProcessingResults results =
              dumpIndexTool.processIndex(config.pathOfInput, filterKeySet, time.milliseconds());
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

  public IndexProcessingResults processIndex(File replicaDir, Set<StoreKey> filterSet, long currentTimeMs)
      throws InterruptedException, IOException, StoreException {
    verifyPath(replicaDir, true);
    final Timer.Context context = metrics.dumpReplicaIndexesTimeMs.time();
    try {
      File[] segmentFiles = getSegmentFilesFromDir(replicaDir);
      SortedMap<File, List<IndexEntry>> segmentFileToIndexEntries = getAllEntriesFromIndex(segmentFiles, throttler);
      Map<StoreKey, Info> keyToState = new HashMap<>();
      long processedCount = 0;
      long putCount = 0;
      long deleteCount = 0;
      long ttlUpdateCount = 0;
      Set<StoreKey> duplicatePuts = new HashSet<>();
      Set<StoreKey> putAfterUpdates = new HashSet<>();
      Set<StoreKey> duplicateDeletes = new HashSet<>();
      Set<StoreKey> duplicateUpdates = new HashSet<>();
      Set<StoreKey> updatesAfterDeletes = new HashSet<>();
      long indexSegmentCount = segmentFileToIndexEntries.size();
      for (int i = 0; i < indexSegmentCount; i++) {
        List<IndexEntry> entries = segmentFileToIndexEntries.get(segmentFiles[i]);
        boolean isInRecentIndexSegment = i >= indexSegmentCount - 2;
        processedCount += entries.size();
        for (IndexEntry entry : entries) {
          StoreKey key = entry.getKey();
          if (filterSet == null || filterSet.isEmpty() || filterSet.contains(key)) {
            boolean isDelete = entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index);
            boolean isTtlUpdate = !isDelete && entry.getValue().isFlagSet(IndexValue.Flags.Ttl_Update_Index);
            boolean isExpired = DumpDataHelper.isExpired(entry.getValue().getExpiresAtMs(), currentTimeMs);
            boolean isPermanent = entry.getValue().getExpiresAtMs() == Utils.Infinite_Time;
            EnumSet<BlobState> states = isExpired ? EnumSet.of(BlobState.Expired) : EnumSet.noneOf(BlobState.class);
            if (isDelete) {
              deleteCount++;
              states.add(BlobState.Deleted);
            } else if (isTtlUpdate) {
              ttlUpdateCount++;
            } else {
              putCount++;
              if (!isExpired) {
                states.add(BlobState.Valid);
              }
            }
            Info info = keyToState.get(key);
            if (info == null) {
              info = new Info(states, isInRecentIndexSegment, isPermanent);
              if (isTtlUpdate) {
                info.markTtlUpdateSeen();
              }
              keyToState.put(key, info);
            } else if (info.states.contains(BlobState.Deleted)) {
              if (isDelete) {
                duplicateDeletes.add(key);
              } else if (isTtlUpdate) {
                updatesAfterDeletes.add(key);
              } else {
                putAfterUpdates.add(key);
              }
            } else {
              if (isDelete) {
                Info newInfo = new Info(states, isInRecentIndexSegment, isPermanent);
                if (info.isTtlUpdateSeen()) {
                  newInfo.markTtlUpdateSeen();
                }
                keyToState.put(key, newInfo);
              } else if (info.isTtlUpdateSeen()) {
                if (isTtlUpdate) {
                  duplicateUpdates.add(key);
                } else {
                  putAfterUpdates.add(key);
                }
              } else if (isTtlUpdate) {
                states.add(isExpired ? BlobState.Expired : BlobState.Valid);
                info = new Info(states, isInRecentIndexSegment, isPermanent);
                info.markTtlUpdateSeen();
                keyToState.put(key, new Info(states, isInRecentIndexSegment, isPermanent));
              } else {
                duplicatePuts.add(key);
              }
            }
          }
        }
      }
      return new IndexProcessingResults(keyToState, processedCount, putCount, ttlUpdateCount, deleteCount,
          duplicatePuts, putAfterUpdates, duplicateDeletes, duplicateUpdates, updatesAfterDeletes);
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
      segment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), entries, new AtomicLong(0), false);
    } finally {
      context.stop();
    }
    return entries;
  }

  /**
   * Will examine the index files in the File replicas for store keys
   * and will then convert these store keys and return the
   * conversion map
   * @param replicas replicas that should contain index files that will be
   *                 parsed
   * @return conversion map of old store keys and new store keys
   * @throws Exception
   */
  public Map<StoreKey, StoreKey> retrieveAndMaybeConvertBlobIds(File[] replicas) throws Exception {
    Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> resultsByReplica =
        getIndexProcessingResults(replicas, null);
    boolean success = resultsByReplica.getFirst();
    if (success) {
      return createConversionKeyMap(replicas, resultsByReplica.getSecond());
    }
    return null;
  }

  /**
   * Takes all the StoreKeys from the file replicas and runs them
   * through the StoreKeyConverter in a batch operation
   * @param replicas An Array of replica directories from which blobIds need to be collected
   * @param results the results of processing the indexes of the given {@code replicas}.
   * @return mapping of original keys to converted keys.  If there's no converted equivalent
   * the value will be the same as the key.
   * @throws Exception
   */
  public Map<StoreKey, StoreKey> createConversionKeyMap(File[] replicas,
      Map<File, DumpIndexTool.IndexProcessingResults> results) throws Exception {
    Set<StoreKey> storeKeys = new HashSet<>();
    for (File replica : replicas) {
      DumpIndexTool.IndexProcessingResults result = results.get(replica);
      for (Map.Entry<StoreKey, DumpIndexTool.Info> entry : result.getKeyToState().entrySet()) {
        storeKeys.add(entry.getKey());
      }
    }
    logger.info("Converting {} store keys...", storeKeys.size());
    storeKeyConverter.dropCache();
    Map<StoreKey, StoreKey> ans = storeKeyConverter.convert(storeKeys);
    logger.info("Store keys converted!");
    return ans;
  }

  /**
   * Processes the indexes of each of the replicas and returns the results.
   * @param replicas the replicas to process indexes for.
   * @return a {@link Pair} whose first indicates whether all results were sane and whose second contains the map of
   * individual results by replica.
   * @throws Exception if there is any error in processing the indexes.
   */
  Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> getIndexProcessingResults(File[] replicas,
      Set<StoreKey> filterSet) throws Exception {
    long currentTimeMs = time.milliseconds();
    Map<File, DumpIndexTool.IndexProcessingResults> results = new HashMap<>();
    boolean sane = true;
    for (File replica : replicas) {
      logger.info("Processing segment files for replica {} ", replica);
      DumpIndexTool.IndexProcessingResults result = processIndex(replica, filterSet, currentTimeMs);
      sane = sane && result.isIndexSane();
      results.put(replica, result);
    }
    return new Pair<>(sane, results);
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
