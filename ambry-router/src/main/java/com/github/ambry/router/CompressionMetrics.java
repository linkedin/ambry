/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics for both compression service and compression algorithm.
 * The compression service metrics counts why compression is skip due to configuration and its error rate.
 * The algorithm-specific metrics measure the time and speed of the algorithm.
 * The service metric names use the format [MetricNamePrefix].[MetricName].
 * These algorithm-specific metric names use the format [MetricNamePrefix].[AlgorithmName].[MetricName].
 * Example algorithm names include "LZ4" and "ZSTD".
 */
public class CompressionMetrics {
  // The prefix of all compression metric names.
  // Using constant string prefix rather than class name is safer because dashboard
  // and alarms are based on the class name.  That means the class cannot be refactored.
  // Code is supposed to improve over time and refactor as needed without impacting external system.
  public final static String MetricNamePrefix = "com.github.ambry.compression";

  /**
   * Algorithm-specific metrics.  There will be one set per algorithm.
   */
  public static class AlgorithmMetrics {

    // Algorithm's compress() metrics.
    public final Meter compressRate;      // Rate compress() is called.
    public final Counter compressError;   // Rate compress() throws exception.
    public final Histogram compressRatioPercent;    // compression ratio % = 100 * UncompressedSize/CompressedSize

    // Full size is the maximum chunk size.  For example, the default full chunk size is 4MB.
    // Less than 4MB is considered partial full.  For simplicity, let's call it small size (< 4 MB).
    // The duration measurements for small size chunks (< 4 MB) are highly skewed due to overheads.
    // For example, compressing a 1KB chunk is probably less efficient than compressing a 100 KB chunk,
    // so the metrics for compressing 1 KB chunks will generate many misleading metrics (like low compression ratio,
    // low throughput due to overhead). It's recommended to use the full size (4 MB) compression metrics for
    // consistent and comparable picture of the algorithm performance.
    public final Histogram fullSizeCompressTimeInMicroseconds;
    public final Histogram fullSizeCompressSpeedMBPerSec;
    public final Histogram smallSizeCompressTimeInMicroseconds;
    public final Histogram smallSizeCompressSpeedMBPerSec;

    // Algorithm's decompress() metrics.
    public final Meter decompressRate;     // Rate decompress() is called.
    public final Counter decompressError;  // Rate decompress() throws exception.
    public final Histogram fullSizeDecompressTimeInMicroseconds;
    public final Histogram fullSizeDecompressSpeedMBPerSec;
    public final Histogram smallSizeDecompressTimeInMicroseconds;
    public final Histogram smallSizeDecompressSpeedMBPerSec;

    /**
     * Create an instance of algorithm-specific metrics.
     * @param algorithmName Name of the algorithm.  The metric name will contain the algorithm name.
     * @param registry The metric registry to add the new metrics.
     */
    public AlgorithmMetrics(String algorithmName, MetricRegistry registry) {
      // Compression metrics
      compressRate = registry.meter(MetricRegistry.name(MetricNamePrefix, algorithmName, "CompressRate"));
      compressError = registry.counter(MetricRegistry.name(MetricNamePrefix, algorithmName, "CompressError"));
      compressRatioPercent = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName, "CompressRatioPercent"));
      fullSizeCompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "FullSizeCompressTimeInMicroseconds"));
      fullSizeCompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "FullSizeCompressSpeedMBPerSec"));
      smallSizeCompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "SmallSizeCompressTimeInMicroseconds"));
      smallSizeCompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "SmallSizeCompressSpeedMBPerSec"));

      // Decompression metrics.
      decompressRate = registry.meter(MetricRegistry.name(MetricNamePrefix, algorithmName, "DecompressRate"));
      decompressError = registry.counter(MetricRegistry.name(MetricNamePrefix, algorithmName, "DecompressError"));
      fullSizeDecompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "FullSizeDecompressTimeInMicroseconds"));
      fullSizeDecompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "FullSizeDecompressSpeedMBPerSec"));
      smallSizeDecompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "SmallSizeDecompressTimeInMicroseconds"));
      smallSizeDecompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(MetricNamePrefix, algorithmName,
          "SmallSizeDecompressSpeedMBPerSec"));
    }
  }

  // The prefix of all compression metrics.
  // It is not a good idea to use class name because if class refactored or moved, all their metrics name changed it
  // will break inGraph dashboard and possibly alarms.  Instead, it should be a constant string that doesn't change.
  // These metrics are per chunk, not per operation.
  private final MetricRegistry metricsRegistry;
  private final Map<String, AlgorithmMetrics> algorithmToMetricsMap = new ConcurrentHashMap<>();

  // Compression metrics, regardless of compression algorithm.
  public final Meter compressSkipRate;      // The rate compression is skipped due to config settings.
  public final Meter compressErrorRate;     // The overall compress failure rate, eg: compress() throws exception.
  public final Meter compressAcceptRate;    // The rate compression is applied and accepted after filtering.
  public final Counter compressReduceSizeBytes;  // For accepted, bytes reduce = OriginalSize - CompressedSize.

  // Compression error metrics.  These metrics must be monitored and raise alarm if as soon as they occurred.
  public final Counter compressErrorConfigInvalidCompressorName;
  public final Counter compressErrorCompressFailed;

  // Compression skip metrics based on config settings.
  public final Counter compressSkipContentEncoding;
  public final Counter compressSkipContentTypeFiltering;
  public final Counter compressSkipSizeTooSmall;
  public final Counter compressSkipRatioTooSmall;

  // Decompression metrics, regardless of decompression algorithms.
  public final Meter decompressSuccessRate;
  public final Meter decompressErrorRate;
  public final Counter decompressExpandSizeBytes;  // Bytes expand  = OriginalSize - CompressedSize
  public final Counter decompressErrorBufferTooSmall;
  public final Counter decompressErrorUnknownAlgorithmName;
  public final Counter decompressErrorDecompressFailed;

  /**
   * Create an instance of compression metrics.
   *
   * @param registry The metrics registry to register its metrics.
   */
  public CompressionMetrics(MetricRegistry registry) {
    metricsRegistry = registry;

    // Compression metrics.
    compressSkipRate = registry.meter(MetricRegistry.name(MetricNamePrefix,"CompressSkipRate"));
    compressErrorRate = registry.meter(MetricRegistry.name(MetricNamePrefix,"CompressErrorRate"));
    compressAcceptRate = registry.meter(MetricRegistry.name(MetricNamePrefix,"CompressAcceptRate"));
    compressReduceSizeBytes = registry.counter(MetricRegistry.name(MetricNamePrefix, "CompressReduceSizeBytes"));
    compressErrorConfigInvalidCompressorName = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressErrorInvalidCompressorName"));
    compressErrorCompressFailed = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressErrorCompressFailed"));
    compressSkipContentEncoding = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressSkipContentEncoding"));
    compressSkipContentTypeFiltering = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressSkipContentTypeFiltering"));
    compressSkipSizeTooSmall = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressSkipSizeTooSmall"));
    compressSkipRatioTooSmall = registry.counter(MetricRegistry.name(MetricNamePrefix,"CompressSkipRatioTooSmall"));

    // Decompression metrics.
    decompressSuccessRate = registry.meter(MetricRegistry.name(MetricNamePrefix,"DecompressSuccessRate"));
    decompressErrorRate = registry.meter(MetricRegistry.name(MetricNamePrefix,"DecompressErrorRate"));
    decompressExpandSizeBytes = registry.counter(MetricRegistry.name(MetricNamePrefix,"DecompressExpandSizeBytes"));
    decompressErrorBufferTooSmall = registry.counter(MetricRegistry.name(MetricNamePrefix,
        "DecompressErrorBufferTooSmall"));
    decompressErrorUnknownAlgorithmName = registry.counter(MetricRegistry.name(MetricNamePrefix,
        "DecompressErrorUnknownAlgorithmName"));
    decompressErrorDecompressFailed = registry.counter(MetricRegistry.name(MetricNamePrefix,
        "DecompressErrorDecompressFailed"));
  }

  /**
   * Get the Algorithm metrics for a specific compression algorithm name.
   * The name of all metrics has the algorithm name.
   * @param algorithmName Name of the algorithm.
   * @return A set of compression algorithm-specific metrics.
   */
  public AlgorithmMetrics getAlgorithmMetrics(String algorithmName) {
    return algorithmToMetricsMap.computeIfAbsent(algorithmName,
        name -> new AlgorithmMetrics(algorithmName, metricsRegistry));
  }
}
