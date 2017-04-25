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
  final Timer dumpIndexTimeMs;
  final Timer dumpReplicaIndexesTimeMs;
  final Timer dumpLogTimeMs;
  final Timer findAllEntriesPerIndexTimeMs;
  final Timer readSingleBlobRecordFromLogTimeMs;
  final Timer readFromLogAndVerifyTimeMs;
  final Timer compareIndexFileToLogTimeMs;
  final Timer compareReplicaIndexFilesToLogTimeMs;

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
    dumpIndexTimeMs = registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "DumpIndexTimeMs"));
    dumpReplicaIndexesTimeMs =
        registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "DumpReplicaIndexesTimeMs"));
    dumpLogTimeMs = registry.timer(MetricRegistry.name(DumpLogTool.class, metricsPrefix + "DumpLogTimeMs"));
    findAllEntriesPerIndexTimeMs =
        registry.timer(MetricRegistry.name(DumpIndexTool.class, metricsPrefix + "FindAllEntriesPerIndexTimeMs"));
    readSingleBlobRecordFromLogTimeMs =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "ReadSingleBlobRecordFromLogTimeMs"));
    readFromLogAndVerifyTimeMs =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "ReadFromLogAndVerifyTimeMs"));
    compareIndexFileToLogTimeMs =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "CompareIndexFileToLogTimeMs"));
    compareReplicaIndexFilesToLogTimeMs =
        registry.timer(MetricRegistry.name(DumpDataTool.class, metricsPrefix + "CompareReplicaIndexFilesToLogTimeMs"));

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
