/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import java.util.HashMap;
import java.util.Map;


/**
 * Metrics related to reserved metadata blob id.
 */
public class ReservedMetadataIdMetrics {
  static final Map<MetricRegistry, ReservedMetadataIdMetrics> METRIC_MAP = new HashMap<>();
  public final Counter numFailedPartitionReserveAttempts;
  public final Counter numUnexpectedReservedPartitionClassCount;
  public final Counter numReservedPartitionFoundReadOnlyCount;
  public final Counter numReservedPartitionFoundUnavailableInLocalDcCount;
  public final Counter numReservedPartitionFoundUnavailableInAllDcCount;
  public final Counter reserveMetadataIdFailedForPostSignedUrlCount;
  public final Counter noReservedMetadataFoundForChunkedUploadResponseCount;
  public final Counter noReservedMetadataForChunkedUploadCount;
  public final Counter mismatchedReservedMetadataForChunkedUploadCount;

  /**
   * Constructor for {@link ReservedMetadataIdMetrics}.
   * @param metricRegistry {@link MetricRegistry} object.
   */
  private ReservedMetadataIdMetrics(MetricRegistry metricRegistry) {
    numUnexpectedReservedPartitionClassCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NumUnexpectedReservedPartitionClassCount"));
    numFailedPartitionReserveAttempts = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NumFailedPartitionReserveAttempts"));
    numReservedPartitionFoundReadOnlyCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NumReservedPartitionFoundReadOnlyCount"));
    numReservedPartitionFoundUnavailableInLocalDcCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NumReservedPartitionFoundUnavailableInLocalDcCount"));
    numReservedPartitionFoundUnavailableInAllDcCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NumReservedPartitionFoundUnavailableInAllDcCount"));
    reserveMetadataIdFailedForPostSignedUrlCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "ReserveMetadataIdFailedForPostSignedUrlCount"));
    noReservedMetadataFoundForChunkedUploadResponseCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NoReservedMetadataFoundForChunkedUploadResponseCount"));
    noReservedMetadataForChunkedUploadCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "NoReservedMetadataForChunkedUploadCount"));
    mismatchedReservedMetadataForChunkedUploadCount = metricRegistry.counter(
        MetricRegistry.name(ReservedMetadataIdMetrics.class, "MismatchedReservedMetadataForChunkedUploadCount"));
  }

  /**
   * Return {@link ReservedMetadataIdMetrics} associated with the specified {@link MetricRegistry} object. If no
   * {@link ReservedMetadataIdMetrics} is associated with the specified {@link MetricRegistry} object, then create one
   * and return the created object.
   * @param metricRegistry {@link MetricRegistry} object.
   * @return ReservedMetadataIdMetrics object.
   */
  public static ReservedMetadataIdMetrics getReservedMetadataIdMetrics(MetricRegistry metricRegistry) {
    synchronized (ReservedMetadataIdMetrics.class) {
      if (!METRIC_MAP.containsKey(metricRegistry)) {
        METRIC_MAP.put(metricRegistry, new ReservedMetadataIdMetrics(metricRegistry));
      }
      return METRIC_MAP.get(metricRegistry);
    }
  }
}
