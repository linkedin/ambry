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
package com.github.ambry.config;

/**
 * Configuration parameters required by a CompressionService.
 * Configurations are read-only.  The only way to alter the values is by modifying the config files.
 * The configuration applies to compression only.  Decompression is automatic and has no configuration.
 */
public class CompressionConfig {

  // Boolean whether compression is enabled in PUT operation.
  public static final String COMPRESSION_ENABLED = "router.compression.enabled";
  static final boolean DEFAULT_COMPRESSION_ENABLED = false;

  // Whether to skip compression if content-encoding present.
  static final String SKIP_IF_CONTENT_ENCODED = "router.compression.skip.if.content.encoded";
  static final boolean DEFAULT_SKIP_IF_CONTENT_ENCODED = true;

  // A comma-separated list of compressible content-types.  For example, "text/javascript, text/log, text".
  // This list supports both full content-type and content-prefix that has no slash "/".
  // All other content-type not in this list will not be compressed.  Default is compress every start with "text/*".
  static final String COMPRESS_CONTENT_TYPES = "router.compression.compress.content.types";
  static final String DEFAULT_COMPRESS_CONTENT_TYPES = "text";

  // The minimal source/chunk size in bytes in order to qualify for compression.
  static final String MINIMAL_DATA_SIZE_IN_BYTES = "router.compression.minimal.content.size";
  static final int DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES = 1024;

  // The minimal compression ratio (uncompressedSize/compressSize) in order to keep compressed content.
  // The higher the compression ratio the better with 1 means the same size.
  static final String MINIMAL_COMPRESS_RATIO = "router.compression.minimal.ratio";
  static final float DEFAULT_MINIMAL_COMPRESS_RATIO = 1.1f;

  // The name of the algorithm to use.  This name must be pre-registered in CompressionMap class.
  // Default is Zstandard compression algorithm.  This string is defined in ambry-common that cannot be referenced here.
  static final String ALGORITHM_NAME = "router.compression.algorithm.name";
  static final String DEFAULT_ALGORITHM_NAME = "ZSTD";

  /**
   * Whether compression is enabled.
   */
  @Config(COMPRESSION_ENABLED)
  public final boolean isCompressionEnabled;

  /**
   * Whether compression is skipped when the content-encoding is present in the request.
   */
  @Config(SKIP_IF_CONTENT_ENCODED)
  public final boolean isSkipWithContentEncoding;

  /**
   * The string that contains the list of comma-separated compressible full content-types.
   */
  @Config(COMPRESS_CONTENT_TYPES)
  public final String compressibleContentTypesCSV;

  /**
   * The minimal source/chunk size in bytes in order to qualify for compression.
   * Chunk size smaller than this size will not be compressed.
   */
  @Config(MINIMAL_DATA_SIZE_IN_BYTES)
  public final int minimalSourceDataSizeInBytes;

  /**
   * The minimal compression ratio in order to keep compressed content.
   * If the size of compressed content is almost the same size as original content, discard the compressed content.
   * Compression ratio is defined as OriginalSize/CompressedSize.  Normally, higher is better.
   */
  @Config(MINIMAL_COMPRESS_RATIO)
  public final double minimalCompressRatio;

  /**
   * The name of the compression to use in PUT operation.
   */
  @Config(ALGORITHM_NAME)
  public final String algorithmName;

  /**
   * Construct a compression config instance using a specific verifiable properties.
   * @param verifiableProperties The verifiable properties.
   */
  public CompressionConfig(VerifiableProperties verifiableProperties) {
    isCompressionEnabled = verifiableProperties.getBoolean(COMPRESSION_ENABLED, DEFAULT_COMPRESSION_ENABLED);
    isSkipWithContentEncoding = verifiableProperties.getBoolean(SKIP_IF_CONTENT_ENCODED, DEFAULT_SKIP_IF_CONTENT_ENCODED);
    algorithmName = verifiableProperties.getString(ALGORITHM_NAME, DEFAULT_ALGORITHM_NAME);
    compressibleContentTypesCSV = verifiableProperties.getString(COMPRESS_CONTENT_TYPES, DEFAULT_COMPRESS_CONTENT_TYPES);
    minimalSourceDataSizeInBytes = verifiableProperties.getInt(MINIMAL_DATA_SIZE_IN_BYTES,
        DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES);
    minimalCompressRatio = verifiableProperties.getDouble(MINIMAL_COMPRESS_RATIO,
        DEFAULT_MINIMAL_COMPRESS_RATIO);
  }
}
