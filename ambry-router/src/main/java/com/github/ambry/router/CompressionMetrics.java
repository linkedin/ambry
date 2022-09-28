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
import com.github.ambry.compression.Compression;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompressionMetrics {
  public static class AlgorithmMetrics {

    // Algorithm's compress() metrics.
    public final Meter compressRate;      // Rate compress() is called.
    public final Counter compressError;   // Rate compress() throws exception.
    public final Histogram compressRatioPercent;    // compression ratio % = 100 * UncompressedSize/CompressedSize
    public final Histogram fullSizeCompressTimeInMicroseconds;
    public final Histogram fullSizeCompressSpeedMBPerSec;
    public final Histogram partialFullCompressTimeInMicroseconds;
    public final Histogram partialFullCompressSpeedMBPerSec;

    // Algorithm's decompress() metrics.
    public final Meter decompressRate;     // Rate decompress() is called.
    public final Counter decompressError;  // Rate decompress() throws exception.
    public final Histogram fullSizeDecompressTimeInMicroseconds;
    public final Histogram fullSizeDecompressSpeedMBPerSec;
    public final Histogram partialFullDecompressTimeInMicroseconds;
    public final Histogram partialFullDecompressSpeedMBPerSec;

    /**
     * Create an instance of algorithm-specific metrics.
     * @param algorithmName Name of the algorithm.  The metric name will contain the algorithm name.
     * @param registry The metric registry to add the new metrics.
     */
    public AlgorithmMetrics(String algorithmName, MetricRegistry registry) {
      // Compression metrics
      compressRate = registry.meter(MetricRegistry.name(ownerClass, algorithmName, "CompressRate"));
      compressError = registry.counter(MetricRegistry.name(ownerClass, algorithmName, "CompressError"));
      compressRatioPercent = registry.histogram(MetricRegistry.name(ownerClass, algorithmName, "CompressRatioPercent"));
      fullSizeCompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "FullSizeCompressTimeInMicroseconds"));
      fullSizeCompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "FullSizeCompressSpeedMBPerSec"));
      partialFullCompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "PartialFullCompressTimeInMicroseconds"));
      partialFullCompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "PartialFullCompressSpeedMBPerSec"));

      // Decompression metrics.
      decompressRate = registry.meter(MetricRegistry.name(ownerClass, algorithmName, "DecompressRate"));
      decompressError = registry.counter(MetricRegistry.name(ownerClass, algorithmName, "DecompressError"));
      fullSizeDecompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "FullSizeDecompressTimeInMicroseconds"));
      fullSizeDecompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "FullSizeDecompressSpeedMBPerSec"));
      partialFullDecompressTimeInMicroseconds = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "PartialFullDecompressTimeInMicroseconds"));
      partialFullDecompressSpeedMBPerSec = registry.histogram(MetricRegistry.name(ownerClass, algorithmName,
          "PartialFullDecompressSpeedMBPerSec"));
    }
  }

  // Group all compression related metrics under one owner "Compression".
  // These metrics are per chunk, not per operation.
  private final static Class<?> ownerClass = Compression.class;
  private final MetricRegistry metricsRegistry;
  private final Map<String, AlgorithmMetrics> algorithmToMetricsMap = new ConcurrentHashMap<>();

  // Compression metrics, regardless of compression algorithm.
  public final Meter compressSkipRate;      // The rate compression is skipped due to config settings.
  public final Meter compressErrorRate;     // The overall compress failure rate, eg: compress() throws exception.
  public final Meter compressAcceptRate;    // The rate compression is applied and accepted after filtering.
  public final Counter compressReduceSizeBytes;  // For accepted, bytes reduce = OriginalSize - CompressedSize.

  // Compression error metrics.  These metrics must be monitored and raise alarm if as soon as they occurred.
  public final Counter compressError_BufferConversion;
  public final Counter compressError_ConfigInvalidCompressorName;
  public final Counter compressError_CompressFailed;

  // Compression skip metrics based on config settings.
  public final Counter compressSkip_ContentEncoding;
  public final Counter compressSkip_ContentTypeFiltering;
  public final Counter compressSkip_SizeTooSmall;
  public final Counter compressSkip_RatioTooSmall;

  // Decompression metrics, regardless of decompression algorithms.
  public final Meter decompressSuccessRate;
  public final Meter decompressErrorRate;
  public final Counter decompressExpandSizeBytes;  // Bytes expand  = OriginalSize - CompressedSize
  public final Counter decompressError_BufferConversion;
  public final Counter decompressError_BufferTooSmall;
  public final Counter decompressError_UnknownAlgorithmName;
  public final Counter decompressError_DecompressFailed;

  /**
   * Create an instance of compression metrics.
   *
   * @param registry The metrics registry to register its metrics.
   */
  public CompressionMetrics(MetricRegistry registry) {
    metricsRegistry = registry;

    // Compression metrics.
    compressSkipRate = registry.meter(MetricRegistry.name(ownerClass,"CompressSkipRate"));
    compressErrorRate = registry.meter(MetricRegistry.name(ownerClass,"CompressErrorRate"));
    compressAcceptRate = registry.meter(MetricRegistry.name(ownerClass,"CompressAcceptRate"));
    compressReduceSizeBytes = registry.counter(MetricRegistry.name(ownerClass, "CompressReduceSizeBytes"));
    compressError_BufferConversion = registry.counter(MetricRegistry.name(ownerClass,"CompressErrorBufferConversion"));
    compressError_ConfigInvalidCompressorName = registry.counter(MetricRegistry.name(ownerClass,"CompressErrorInvalidCompressorName"));
    compressError_CompressFailed = registry.counter(MetricRegistry.name(ownerClass,"CompressErrorCompressFailed"));
    compressSkip_ContentEncoding = registry.counter(MetricRegistry.name(ownerClass,"CompressSkipContentEncoding"));
    compressSkip_ContentTypeFiltering = registry.counter(MetricRegistry.name(ownerClass,"CompressSkipContentTypeFiltering"));
    compressSkip_SizeTooSmall = registry.counter(MetricRegistry.name(ownerClass,"CompressSkipSizeTooSmall"));
    compressSkip_RatioTooSmall = registry.counter(MetricRegistry.name(ownerClass,"CompressSkipRatioTooSmall"));

    // Decompression metrics.
    decompressSuccessRate = registry.meter(MetricRegistry.name(ownerClass,"DecompressSuccessRate"));
    decompressErrorRate = registry.meter(MetricRegistry.name(ownerClass,"DecompressErrorRate"));
    decompressExpandSizeBytes = registry.counter(MetricRegistry.name(ownerClass,"DecompressExpandSizeBytes"));
    decompressError_BufferConversion = registry.counter(MetricRegistry.name(ownerClass,
        "DecompressErrorBufferConversion"));
    decompressError_BufferTooSmall = registry.counter(MetricRegistry.name(ownerClass,
        "DecompressErrorBufferTooSmall"));
    decompressError_UnknownAlgorithmName = registry.counter(MetricRegistry.name(ownerClass,
        "DecompressErrorUnknownAlgorithmName"));
    decompressError_DecompressFailed = registry.counter(MetricRegistry.name(ownerClass,
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
