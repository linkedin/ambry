/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class used for processing indexes and batching conversion maps of store keys
 */
class IndexProcessingHelper {

  private static final Logger logger = LoggerFactory.getLogger(IndexProcessingHelper.class);
  private final StoreKeyConverter storeKeyConverter;
  private final Set<StoreKey> filterSet;
  private final Throttler throttler;
  private final Time time;
  private final DumpIndexTool dumpIndexTool;

  IndexProcessingHelper(StoreKeyFactory storeKeyFactory, StoreConfig storeConfig, Set<StoreKey> filterSet,
      Throttler throttler, StoreMetrics storeMetrics, StoreToolsMetrics metrics, Time time,
      StoreKeyConverter storeKeyConverter) {
    this.time = time;
    this.storeKeyConverter = storeKeyConverter;
    this.filterSet = filterSet;
    this.throttler = throttler;
    dumpIndexTool = new DumpIndexTool(storeKeyFactory, storeConfig, time, metrics, storeMetrics);
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
  Map<StoreKey, StoreKey> createConversionKeyMap(File[] replicas,
      Map<File, DumpIndexTool.IndexProcessingResults> results) throws Exception {
    Set<StoreKey> storeKeys = new HashSet<>();
    for (File replica : replicas) {
      DumpIndexTool.IndexProcessingResults result = results.get(replica);
      for (Map.Entry<StoreKey, DumpIndexTool.Info> entry : result.getKeyToState().entrySet()) {
        storeKeys.add(entry.getKey());
      }
    }
    logger.info("Converting {} store keys...", storeKeys.size());
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
  Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> getIndexProcessingResults(File[] replicas)
      throws Exception {
    long currentTimeMs = time.milliseconds();
    Map<File, DumpIndexTool.IndexProcessingResults> results = new HashMap<>();
    boolean sane = true;
    for (File replica : replicas) {
      logger.info("Processing segment files for replica {} ", replica);
      DumpIndexTool.IndexProcessingResults result =
          dumpIndexTool.processIndex(replica, filterSet, currentTimeMs, throttler);
      sane = sane && result.isIndexSane();
      results.put(replica, result);
    }
    return new Pair<>(sane, results);
  }
}
