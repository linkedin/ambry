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

import com.github.ambry.utils.Utils;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Configuration parameters required by a CompressionService.
 * Configurations are read-only.  The only way to alter the values is by modifying the config files.
 * The configuration applies to compression only.  Decompression is automatic and has no configuration.
 */
public class CompressionConfig {

  // Boolean whether compression is enabled in PUT operation.
  // TODO - Compression is disabled by default.  Need to enable after testing and verification.
  static final String COMPRESSION_ENABLED = "router.compression.enabled";
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
  public boolean isCompressionEnabled;

  /**
   * Whether compression is skipped when the content-encoding is present in the request.
   */
  @Config(SKIP_IF_CONTENT_ENCODED)
  public boolean isSkipWithContentEncoding;

  /**
   * The string that contains the list of comma-separated compressible full content-types.
   */
  @Config(COMPRESS_CONTENT_TYPES)
  public String compressibleContentTypesCSV;

  /**
   * The minimal source/chunk size in bytes in order to qualify for compression.
   * Chunk size smaller than this size will not be compressed.
   */
  @Config(MINIMAL_DATA_SIZE_IN_BYTES)
  public int minimalSourceDataSizeInBytes;

  /**
   * The minimal compression ratio in order to keep compressed content.
   * If the size of compressed content is almost the same size as original content, discard the compressed content.
   * Compression ratio is defined as OriginalSize/CompressedSize.  Normally, higher is better.
   */
  @Config(MINIMAL_COMPRESS_RATIO)
  public double minimalCompressRatio;

  /**
   * The name of the compression to use in PUT operation.
   */
  @Config(ALGORITHM_NAME)
  public String algorithmName;

  /**
   * The set contains a list of compressible content-types and content-prefixes.
   * The key (content type) must be lower-case only.
   * This set is generated from the compressibleContentTypesCSV property.
   */
  public Set<String> compressibleContentTypes;

  /**
   * Create an instance of CompressionConfig using the default values.
   */
  public CompressionConfig() {
    isCompressionEnabled = DEFAULT_COMPRESSION_ENABLED;
    isSkipWithContentEncoding = DEFAULT_SKIP_IF_CONTENT_ENCODED;
    algorithmName = DEFAULT_ALGORITHM_NAME;
    compressibleContentTypesCSV = DEFAULT_COMPRESS_CONTENT_TYPES;
    minimalSourceDataSizeInBytes = DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES;
    minimalCompressRatio = DEFAULT_MINIMAL_COMPRESS_RATIO;
    compressibleContentTypes = parseContentTypes(compressibleContentTypesCSV);
  }

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

    // Build the content-type set.
    compressibleContentTypes = parseContentTypes(compressibleContentTypesCSV);
  }

  /**
   * Parse the comma-separated list of content-types and prefixes into a set.
   *
   * @param compressibleContentTypesCSV Comma-separated list of compressible content-types and content prefixes.
   * @return A set of compressible content-type and content-type prefixes.
   */
  private Set<String> parseContentTypes(String compressibleContentTypesCSV) {
    Set<String> contentTypes = new HashSet<>();
    if (compressibleContentTypesCSV != null && compressibleContentTypesCSV.length() > 0) {
      for (String contentType : compressibleContentTypesCSV.split(",")) {
        String loweredContentType = contentType.trim().toLowerCase(Locale.ENGLISH);
        contentTypes.add(loweredContentType);
      }
    }

    return contentTypes;
  }

  /**
   * Check whether compression should be applied to the specified content type.
   * Support format of content-type (3 variations):
   * See <a href="https://www.geeksforgeeks.org/http-headers-content-type/">Content-Type spec</a> for detail.
   *   - text/html
   *   - text/html; charset=UTF-8
   *   - multipart/form-data; boundary=something
   * <p>
   * Content type format is "prefix/specific; option=value".
   * Content check is applied in this order.  If not in the list, it's not compressible.
   * 1. Check whether the full contentType parameter is specified in content-type set.  Example "text/html"
   * 2. If contentType parameter contains option (separated by ";"), remove the option and check again.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-type "text/html".
   * 3. Get the content-type prefix (the string before the "/" separator) and lookup prefix again.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-prefix "text".
   * @param contentType The HTTP content type.
   * @return TRUE if compression allowed; FALSE if compression not allowed.
   */
  public boolean isCompressibleContentType(String contentType) {
    // If there is no content-type, assume it's other content-type.
    if (Utils.isNullOrEmpty(contentType)) {
      return false;
    }

    // If an exact match by the full content-type is found, it's compressible.
    String contentTypeLower = contentType.trim().toLowerCase();
    if (compressibleContentTypes.contains(contentTypeLower)) {
      return true;
    }

    // Check whether the content-type contains options separated by ";"
    int mimeSeparatorIndex = contentTypeLower.indexOf(';');
    if (mimeSeparatorIndex > 0) {
      String contentTypeWithoutOption = contentTypeLower.substring(0, mimeSeparatorIndex);
      if (compressibleContentTypes.contains(contentTypeWithoutOption)) {
        return true;
      }
    }

    // Check whether there's a match by content-type prefix.
    // For example, if input content-type is "text/csv", check whether "text" is compressible.
    int prefixSeparatorIndex = contentTypeLower.indexOf('/');
    if (prefixSeparatorIndex > 0) {
      String prefix = contentTypeLower.substring(0, prefixSeparatorIndex);
      if (compressibleContentTypes.contains(prefix)) {
        return true;
      }
    }

    // Content-type is not found in the allowed list, assume it's not compressible.
    return false;
  }
}
