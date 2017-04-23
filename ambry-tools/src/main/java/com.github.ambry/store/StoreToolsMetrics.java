/**
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Metrics for all store tools
 */
public class StoreToolsMetrics {
  final Timer dumpIndexTime;
  final Timer dumpReplicaIndexesTime;
  final Timer dumpLogTime;
  final Timer findAllEntriesPerIndexTime;
  final Timer readSingleBlobRecordFromLogTime;
  final Timer readFromLogAndVerifyTime;
  final Timer compareIndexFileToLogTime;
  final Timer compareReplicaIndexFilesToLogTime;

  final Counter logDeserializationError;
  final Counter endOfFileOnDumpLogError;
  final Counter unknownErrorOnDumpIndex;
  final Counter unknownErrorOnDumpLog;
  final Counter indexToLogDeleteFlagMisMatchError;
  final Counter indexToLogExpiryMisMatchError;
  final Counter indexToLogBlobIdMisMatchError;
  final Counter indexToLogBlobRecordComparisonFailure;
  final Counter logRangeNotFoundInIndexError;
  final Counter indexLogEndOffsetMisMatchError;

  StoreToolsMetrics(MetricRegistry registry, String storeId) {
    String metricsPrefix = storeId + ".";
    dumpIndexTime = registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "DumpIndexTime"));
    dumpReplicaIndexesTime =
        registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "DumpReplicaIndexesTime"));
    dumpLogTime = registry.timer(MetricRegistry.name(DumpLogTool.class, metricsPrefix + "dumpLogTime"));
    findAllEntriesPerIndexTime =
        registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "findAllEntriesPerIndexTime"));
    readSingleBlobRecordFromLogTime =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "readSingleBlobRecordFromLogTime"));
    readFromLogAndVerifyTime =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "readFromLogAndVerifyTime"));
    compareIndexFileToLogTime =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "compareIndexFileToLogTime"));
    compareReplicaIndexFilesToLogTime =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "compareReplicaIndexFilesToLogTime"));

    logDeserializationError =
        registry.counter(MetricRegistry.name(DumpLogTool.class, metricsPrefix + "LogDeserializationErrorCount"));
    endOfFileOnDumpLogError =
        registry.counter(MetricRegistry.name(DumpLogTool.class, metricsPrefix + "EndOfFileOnDumpLogErrorCount"));
    unknownErrorOnDumpIndex =
        registry.counter(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "UnknownErrorOnDumpIndexCount"));
    unknownErrorOnDumpLog =
        registry.counter(MetricRegistry.name(DumpLogTool.class, metricsPrefix + "UnknownErrorOnDumpLogCount"));
    indexToLogDeleteFlagMisMatchError = registry.counter(
        MetricRegistry.name(DumpDataTool.class, metricsPrefix + "IndexToLogDeleteFlagMisMatchErrorCount"));
    indexToLogExpiryMisMatchError =
        registry.counter(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "IndexToLogExpiryMisMatchErrorCount"));
    indexToLogBlobIdMisMatchError =
        registry.counter(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "IndexToLogBlobIdMisMatchErrorCount"));
    indexToLogBlobRecordComparisonFailure = registry.counter(
        MetricRegistry.name(DumpDataTool.class, metricsPrefix + "IndexToLogBlobRecordComparisonFailureCount"));
    logRangeNotFoundInIndexError =
        registry.counter(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "LogRangeNotFoundInIndexErrorCount"));
    indexLogEndOffsetMisMatchError = registry.counter(
        MetricRegistry.name(DumpDataTool.class, metricsPrefix + "IndexLogEndOffsetMisMatchErrorCount"));
  }
}
