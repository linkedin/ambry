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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import java.io.File;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Takes an array of replica directories, scans them for store keys,
 * then runs the store keys through a store key converter
 */
public class ReplicaToBlobIdConverter {

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyCheckerTool.class);

  private final IndexProcessingHelper indexProcessingHelper;

  public ReplicaToBlobIdConverter(ClusterMap clusterMap, StoreKeyFactory storeKeyFactory, StoreConfig storeConfig,
      Set<StoreKey> filterSet, Throttler throttler, StoreToolsMetrics metrics, Time time,
      StoreKeyConverter storeKeyConverter) {
    StoreMetrics storeMetrics = new StoreMetrics("ReplicaToBlobIdConverter", clusterMap.getMetricRegistry());
    indexProcessingHelper =
        new IndexProcessingHelper(storeKeyFactory, storeConfig, filterSet, throttler, storeMetrics, metrics, time,
            storeKeyConverter);
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
  public Map<StoreKey, StoreKey> convertBlobIds(File[] replicas) throws Exception {
    Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> resultsByReplica =
        indexProcessingHelper.getIndexProcessingResults(replicas);
    boolean success = resultsByReplica.getFirst();
    if (success) {
      return indexProcessingHelper.createConversionKeyMap(replicas, resultsByReplica.getSecond());
    }
    return null;
  }
}
