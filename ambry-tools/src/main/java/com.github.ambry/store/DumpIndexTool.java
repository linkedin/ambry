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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.ServerConfig;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
    private boolean isPermanent;

    Info(EnumSet<BlobState> states, boolean isInRecentIndexSegment, boolean isPermanent) {
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
    public static final long INVALID_VALUE = -1;

    private final Map<StoreKey, Info> keyToState;
    private final long processedCount;
    private final long putCount;
    private final long ttlUpdateCount;
    private final long deleteCount;
    private final long craftedIdCount;
    private final Set<StoreKey> duplicatePuts;
    private final Set<StoreKey> putAfterUpdates;
    private final Set<StoreKey> duplicateDeletes;
    private final Set<StoreKey> duplicateUpdates;
    private final Set<StoreKey> updateAfterDeletes;
    private final Map<StoreKey, StoreKey> duplicateKeys;
    private final long activeCount;
    private final Throwable throwable;

    IndexProcessingResults(Map<StoreKey, Info> keyToState, long processedCount, long putCount, long ttlUpdateCount,
        long deleteCount, long craftedIdCount, Set<StoreKey> duplicatePuts, Set<StoreKey> putAfterUpdates,
        Set<StoreKey> duplicateDeletes, Set<StoreKey> duplicateUpdates, Set<StoreKey> updateAfterDeletes,
        Map<StoreKey, StoreKey> duplicateKeys) {
      this.keyToState = keyToState;
      this.processedCount = processedCount;
      this.putCount = putCount;
      this.ttlUpdateCount = ttlUpdateCount;
      this.deleteCount = deleteCount;
      this.craftedIdCount = craftedIdCount;
      this.duplicatePuts = duplicatePuts;
      this.putAfterUpdates = putAfterUpdates;
      this.duplicateDeletes = duplicateDeletes;
      this.duplicateUpdates = duplicateUpdates;
      this.updateAfterDeletes = updateAfterDeletes;
      this.duplicateKeys = duplicateKeys;
      activeCount = keyToState.values().stream().filter(value -> value.getStates().contains(BlobState.Valid)).count();
      throwable = null;
    }

    IndexProcessingResults(Throwable throwable) {
      this.throwable = throwable;
      keyToState = null;
      processedCount = putCount = ttlUpdateCount = deleteCount = craftedIdCount = activeCount = INVALID_VALUE;
      duplicatePuts = putAfterUpdates = duplicateDeletes = duplicateUpdates = updateAfterDeletes = null;
      duplicateKeys = null;
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
     * @return number of entries where the {@link StoreKey} was a crafted {@link BlobId}
     */
    public long getCraftedIdCount() {
      return craftedIdCount;
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
     * @return the keys marked as duplicates because another version of the key exists in the index
     */
    public Map<StoreKey, StoreKey> getDuplicateKeys() {
      return duplicateKeys;
    }

    /**
     * @return the throwable set (if any)
     */
    public Throwable getThrowable() {
      return throwable;
    }

    /**
     * @return {@code true} if there are no duplicate puts or deletes or updates and no puts after updates or updates
     * after deletes. {@code false} otherwise
     */
    public boolean isIndexSane() {
      return throwable == null && duplicatePuts.size() == 0 && putAfterUpdates.size() == 0
          && duplicateDeletes.size() == 0 && duplicateUpdates.size() == 0 && updateAfterDeletes.size() == 0
          && duplicateKeys.size() == 0;
    }

    @Override
    public String toString() {
      // @formatter:off
      return throwable == null ? "Processed: " + processedCount
              + ", Active: " + activeCount
              + ", Put: " + putCount
              + ", TTL update count: " + ttlUpdateCount
              + ", Delete: " + deleteCount
              + ", Crafted ID count: " + craftedIdCount
              + ", Duplicate Puts: " + duplicatePuts
              + ", Put After Update: " + putAfterUpdates
              + ", Duplicate Delete: " + duplicateDeletes
              + ", Duplicate Update: " + duplicateUpdates
              + ", Update After Delete: " + updateAfterDeletes
              + ", Duplicate Keys: " + duplicateKeys
          : "Exception Msg: " + throwable.getMessage();
      // @formatter:on
    }
  }

  /**
   * The possible states of a blob.
   */
  public enum BlobState {
    Valid, Deleted, Expired
  }

  /**
   * The different operations supported by the tool.
   */
  private enum Operation {
    /**
     * Processes all the index segments and dumps all or the filtered entries.
     */
    DumpIndex,

    /**
     * Processes the given index segment and dumps all or the filtered entries.
     */
    DumpIndexSegment,

    /**
     * Processes all the index segments and makes sure that the index is "sane". Optionally, can also check that there
     * are no crafted IDs in the store
     */
    VerifyIndex,

    /**
     * Processes all the indexes on a node and ensures that they are "sane". Optionally, can also check that there are
     * no crafted IDs in the store
     */
    VerifyDataNode
  }

  /**
   * Config for the DumpIndexTool.
   */
  private static class DumpIndexToolConfig {

    /**
     * The type of operation.
     * Operations are as listed in {@link Operation}
     */
    @Config("type.of.operation")
    final Operation typeOfOperation;

    /**
     * The path where the input is. This may differ for different operations. For e.g., this is a file for
     * {@link Operation#DumpIndexSegment} but is a directory for {@link Operation#DumpIndex} and
     * {@link Operation#VerifyIndex}. This is not required for {@link Operation#VerifyDataNode}
     */
    @Config("path.of.input")
    final File pathOfInput;

    /**
     * The hostname of the target datanode as it appears in the partition layout. Required only for
     * {@link Operation#VerifyDataNode}
     */
    @Config("hostname")
    @Default("localhost")
    final String hostname;

    /**
     * The port of the target datanode in the partition layout (need not be the actual port to connect to). Required
     * only for {@link Operation#VerifyDataNode}
     */
    @Config("port")
    @Default("6667")
    final int port;

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
     * The number of index entries to process every second. In case of {@link Operation#VerifyDataNode}, this is across
     * all partitions being processed.
     */
    @Config("index.entries.to.process.per.sec")
    @Default("Long.MAX_VALUE")
    final long indexEntriesToProcessPerSec;

    /**
     * Used for {@link Operation#VerifyIndex} and {@link Operation#VerifyDataNode} if the presence of crafted IDs needs
     * to be reported as a failure. This check takes precedence over index sanity.
     */
    @Config("fail.if.crafted.ids.present")
    @Default("false")
    final boolean failIfCraftedIdsPresent;

    /**
     * The number of partitions to verify in parallel for {@link Operation#VerifyDataNode}. The maximum is capped at the
     * number of partitions on the node.
     */
    @Config("parallelism")
    @Default("1")
    final int parallelism;

    /**
     * If {@code true}, uses {@link StoreKeyConverter} to detect duplicates across keys
     */
    @Config("detect.duplicates.across.keys")
    @Default("true")
    final boolean detectDuplicatesAcrossKeys;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    DumpIndexToolConfig(VerifiableProperties verifiableProperties) {
      typeOfOperation = Operation.valueOf(verifiableProperties.getString("type.of.operation"));
      pathOfInput = new File(verifiableProperties.getString("path.of.input"));
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getIntInRange("port", 6667, 1, 65535);
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      String filterSetStr = verifiableProperties.getString("filter.set", "");
      if (!filterSetStr.isEmpty()) {
        filterSet = new HashSet<>(Arrays.asList(filterSetStr.split(",")));
      } else {
        filterSet = Collections.emptySet();
      }
      indexEntriesToProcessPerSec =
          verifiableProperties.getLongInRange("index.entries.to.process.per.sec", Long.MAX_VALUE, 1, Long.MAX_VALUE);
      failIfCraftedIdsPresent = verifiableProperties.getBoolean("fail.if.crafted.ids.present", false);
      parallelism = verifiableProperties.getIntInRange("parallelism", 1, 1, Integer.MAX_VALUE);
      detectDuplicatesAcrossKeys = verifiableProperties.getBoolean("detect.duplicates.across.keys", true);
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
    final AtomicInteger exitCode = new AtomicInteger(0);
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
              dumpIndexTool.processIndex(config.pathOfInput, filterKeySet, time.milliseconds(),
                  config.detectDuplicatesAcrossKeys);
          exitCode.set(reportVerificationResults(config.pathOfInput, results, config.failIfCraftedIdsPresent));
          break;
        case VerifyDataNode:
          DataNodeId dataNodeId = clusterMap.getDataNodeId(config.hostname, config.port);
          if (dataNodeId == null) {
            logger.error("No data node corresponding to {}:{}", config.hostname, config.port);
          } else {
            Set<File> replicaDirs = clusterMap.getReplicaIds(dataNodeId)
                .stream()
                .map(replicaId -> new File(replicaId.getMountPath()))
                .collect(Collectors.toSet());
            Map<File, IndexProcessingResults> resultsByReplica =
                dumpIndexTool.processIndex(replicaDirs, filterKeySet, config.parallelism,
                    config.detectDuplicatesAcrossKeys);
            replicaDirs.removeAll(resultsByReplica.keySet());
            if (replicaDirs.size() != 0) {
              logger.error("Results obtained missing {}", replicaDirs);
              exitCode.set(5);
            } else {
              resultsByReplica.forEach((replicaDir, result) -> exitCode.set(Math.max(exitCode.get(),
                  reportVerificationResults(replicaDir, result, config.failIfCraftedIdsPresent))));
            }
          }
          break;
        default:
          throw new IllegalArgumentException("Unrecognized operation: " + config.typeOfOperation);
      }
    }
    System.exit(exitCode.get());
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

  /**
   * Reports verification results and returns possible exit code
   * @param replicaDir the directory that contains the indexes for which {@code results} was generated
   * @param results the processing results for index in {@code replicaDir}
   * @param failIfCraftedIdsPresent if {@code true}, reports the presence of crafted IDs as a failure
   * @return the suggested exit code
   */
  public static int reportVerificationResults(File replicaDir, IndexProcessingResults results,
      boolean failIfCraftedIdsPresent) {
    int exitCode = 0;
    if (results == null) {
      logger.error("Index of {} has no results", replicaDir);
      exitCode = 1;
    } else if (results.getThrowable() != null) {
      logger.error("Processing of index of {} failed", replicaDir, results.getThrowable());
      exitCode = 2;
    } else if (failIfCraftedIdsPresent && results.getCraftedIdCount() != 0) {
      logger.error("Index of {} has {} crafted IDs", replicaDir, results.getCraftedIdCount());
      exitCode = 3;
    } else if (!results.isIndexSane()) {
      logger.error("Index of {} has errors. Results are {}", replicaDir, results);
      exitCode = 4;
    } else {
      logger.info("Index of {} is well formed and without errors", replicaDir);
    }
    return exitCode;
  }

  /**
   * Returns {@link IndexProcessingResults} for each replicaDir in {@code replicaDirs}
   * @param replicaDirs the directories to examine
   * @param filterSet the filter set of keys to examine. Can be {@code null}
   * @param parallelism the desired parallelism. The minimum of this and the size of {@code replicaDirs} is the actual
   *                    chosen parallelism.
   * @param detectDuplicatesAcrossKeys if {@code true}, uses the {@link StoreKeyConverter} to detect duplicates across
   *                                   keys. If set to {@code true}, parallelism has to be 1.
   * @return @link IndexProcessingResults} for each replicaDir in {@code replicaDirs}
   */
  public Map<File, IndexProcessingResults> processIndex(Set<File> replicaDirs, Set<StoreKey> filterSet, int parallelism,
      boolean detectDuplicatesAcrossKeys) {
    if (detectDuplicatesAcrossKeys && parallelism != 1) {
      throw new IllegalArgumentException("Parallelism cannot be > 1 because StoreKeyConverter is not thread safe");
    }
    if (replicaDirs == null || replicaDirs.size() == 0) {
      return Collections.emptyMap();
    }
    long currentTimeMs = time.milliseconds();
    ConcurrentMap<File, IndexProcessingResults> resultsByReplica = new ConcurrentHashMap<>();
    ExecutorService executorService = Executors.newFixedThreadPool(Math.min(parallelism, replicaDirs.size()));
    List<Future<?>> taskFutures = new ArrayList<>(replicaDirs.size());
    for (File replicaDir : replicaDirs) {
      Future<?> taskFuture = executorService.submit(() -> {
        logger.info("Processing segment files for replica {} ", replicaDir);
        IndexProcessingResults results = null;
        try {
          results = processIndex(replicaDir, filterSet, currentTimeMs, detectDuplicatesAcrossKeys);
        } catch (Throwable e) {
          results = new IndexProcessingResults(e);
        } finally {
          resultsByReplica.put(replicaDir, results);
        }
      });
      taskFutures.add(taskFuture);
    }
    for (Future<?> taskFuture : taskFutures) {
      try {
        taskFuture.get();
      } catch (Exception e) {
        throw new IllegalStateException("Future encountered error while waiting", e);
      }
    }
    return resultsByReplica;
  }

  public IndexProcessingResults processIndex(File replicaDir, Set<StoreKey> filterSet, long currentTimeMs,
      boolean detectDuplicatesAcrossKeys) throws Exception {
    verifyPath(replicaDir, true);
    final Timer.Context context = metrics.dumpReplicaIndexesTimeMs.time();
    try {
      File[] segmentFiles = getSegmentFilesFromDir(replicaDir);
      SortedMap<File, List<IndexEntry>> segmentFileToIndexEntries = getAllEntriesFromIndex(segmentFiles);
      Map<StoreKey, Info> keyToState = new HashMap<>();
      long processedCount = 0;
      long putCount = 0;
      long deleteCount = 0;
      long ttlUpdateCount = 0;
      long craftedIdCount = 0;
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
            if (key instanceof BlobId && BlobId.isCrafted(key.getID())) {
              craftedIdCount++;
            }
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
                Info newInfo = new Info(states, isInRecentIndexSegment, isPermanent);
                newInfo.markTtlUpdateSeen();
                keyToState.put(key, newInfo);
              } else {
                duplicatePuts.add(key);
              }
            }
          }
        }
      }
      Map<StoreKey, StoreKey> duplicateKeys = new HashMap<>();
      if (detectDuplicatesAcrossKeys) {
        logger.info("Converting {} store keys...", keyToState.size());
        Map<StoreKey, StoreKey> conversions = storeKeyConverter.convert(keyToState.keySet());
        logger.info("Store keys converted!");
        conversions.forEach((k, v) -> {
          if (!k.equals(v) && keyToState.containsKey(v)) {
            // if k == v, the processing above should have caught it
            // if v exists in keyToState, it means both k and v exist in the store and this is a problem
            duplicateKeys.put(k, v);
          }
        });
      }
      return new IndexProcessingResults(keyToState, processedCount, putCount, ttlUpdateCount, deleteCount,
          craftedIdCount, duplicatePuts, putAfterUpdates, duplicateDeletes, duplicateUpdates, updatesAfterDeletes,
          duplicateKeys);
    } finally {
      context.stop();
    }
  }

  public SortedMap<File, List<IndexEntry>> getAllEntriesFromIndex(File[] segmentFiles)
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
   * Will examine the index files in the File replicas for store keys and will then convert these store keys and return
   * the conversion map
   * @param replicas replicas that should contain index files that will be parsed
   * @return conversion map of old store keys and new store keys
   * @throws Exception
   */
  public Map<StoreKey, StoreKey> retrieveAndMaybeConvertStoreKeys(File[] replicas) throws Exception {
    Map<File, IndexProcessingResults> results = processIndex(new HashSet<>(Arrays.asList(replicas)), null, 1, false);
    boolean success = true;
    for (DumpIndexTool.IndexProcessingResults result : results.values()) {
      if (!result.isIndexSane()) {
        success = false;
        break;
      }
    }
    if (success) {
      return createConversionKeyMap(replicas, results);
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
   * Gets all index segment files that are present in a directory and sorts them by their logical order
   * @param dir the directory to look for index segment files in
   * @return all index segment files that are present in {@code dir} sorted by their logical order
   */
  public static File[] getSegmentFilesFromDir(File dir) {
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
