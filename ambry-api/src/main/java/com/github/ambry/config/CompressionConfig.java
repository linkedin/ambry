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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration parameters required by a CompressionService.
 * Configurations are read-only.  The only way to alter the values is by modifying the config files.
 * The configuration applies to compression only.  Decompression is automatic and has no configuration.
 */
public class CompressionConfig {

  // Boolean whether compression is enabled in PUT operation.
  private static final String CONFIG_COMPRESSION_ENABLED = "router.compression.enabled";
  private static final boolean DEFAULT_COMPRESSION_ENABLED = true;

  // Whether to skip compression if content-encoding present.
  private static final String CONFIG_SKIP_CONTENT_ENCODING = "router.compression.skip.content.encoding";
  private static final boolean DEFAULT_SKIP_CONTENT_ENCODING = true;

  // A comma-separated list of compressible fully content-types, like "text/javascript, text/log; Charset=UTF8"
  private static final String CONFIG_CONTENT_TYPES_COMPRESSIBLE = "router.compression.content.types.compressible";
  private static final String DEFAULT_CONTENT_TYPES_COMPRESSIBLE = null;

  // A comma-separated list of incompressible fully content-types, like "text/javascript, text/log; Charset=UTF8"
  private static final String CONFIG_CONTENT_TYPES_INCOMPRESSIBLE = "router.compression.content.types.incompressible";
  private static final String DEFAULT_CONTENT_TYPES_INCOMPRESSIBLE = null;

  // A comma-separated list of compressible content-prefixes, such as "text" for text/*
  private static final String CONFIG_CONTENT_PREFIXES_COMPRESSIBLE = "router.compression.content.prefixes.compressible";
  private static final String DEFAULT_CONTENT_PREFIXES_COMPRESSIBLE = "text";

  // A comma-separated list of incompressible content-prefixes, such as "image" for image/*.
  private static final String CONFIG_CONTENT_PREFIXES_INCOMPRESSIBLE = "router.compression.content.prefixes.incompressible";
  private static final String DEFAULT_CONTENT_PREFIXES_INCOMPRESSIBLE = "image";

  // Boolean whether to compress other content-types not specified in content-types and content-prefixes.
  // It applies to PUT operation only.
  private static final String CONFIG_COMPRESS_OTHER_CONTENT_TYPE = "router.compression.other.content.types";
  private static final boolean DEFAULT_COMPRESS_OTHER_CONTENT_TYPE = false;

  // The minimal source/chunk size in bytes in order to qualify for compression.
  private static final String CONFIG_MINIMAL_DATA_SIZE_IN_BYTES = "router.compression.minimal.content.size";
  private static final int DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES = 1024;

  // The minimal compression ratio (uncompressedSize/compressSize) in order to keep compressed content.
  // The higher the compression ratio the better with 1 means the same size.
  private static final String CONFIG_MINIMAL_COMPRESS_RATIO = "router.compression.minimal.ratio";
  private static final float DEFAULT_MINIMAL_COMPRESS_RATIO = 1.1f;

  // The name of the algorithm to use.  This name must be pre-registered in CompressionMap class.
  private static final String CONFIG_ALGORITHM_NAME = "router.compression.algorithm.name";
  private static final String DEFAULT_ALGORITHM_NAME = null;

  /**
   * Whether compression is enabled.
   */
  public boolean isCompressionEnabled;

  /**
   * Whether compression is skipped when the content-encoding is present in the request.
   */
  public boolean isSkipWithContentEncoding;

  /**
   * The map contains list of content-types and whether they are compressible.
   * The map key (content type) must be lower-case only.
   * The map value should either be true for compressible content-type or false for incompressible content-type.
   */
  public Map<String, Boolean> compressContentTypes;

  /**
   * The map contains list from content-type prefix and whether they are compressible.
   * Unlike compressContentTypes that has fully content-type, such as text/javascript,
   * content-type prefix is the content category before separator, such as "text" means "text/*".
   * The map key (content type prefix) must be in lower-case only.
   * The map value should either be true for compressible content-type or false for incompressible content-type.
   */
  public Map<String, Boolean> compressContentPrefixes;

  /**
   * Whether unknown content-type should be compressed or not.
   * Unknown content-type are those not listed in compressibleContentTypes and compressibleContentTypePrefixs.
   */
  public boolean compressOtherContentTypes;

  /**
   * The minimal source/chunk size in bytes in order to qualify for compression.
   * Chunk size smaller than this size will not be compressed.
   */
  public int minimalSourceDataSizeInBytes;

  /**
   * The minimal compression ratio in order to keep compressed content.
   * If the size of compressed content is almost the same size as original content, discard the compressed content.
   * Compression ratio is defined as OriginalSize/CompressedSize.  Normally, higher is better.
   */
  public double minimalCompressRatio;

  /**
   * The name of the compression to use in PUT operation.
   */
  public String algorithmName;

  /**
   * Create an instance of CompressionConfig using the default values.
   */
  public CompressionConfig() {
    isCompressionEnabled = DEFAULT_COMPRESSION_ENABLED;
    isSkipWithContentEncoding = DEFAULT_SKIP_CONTENT_ENCODING;
    algorithmName = DEFAULT_ALGORITHM_NAME;
    compressOtherContentTypes = DEFAULT_COMPRESS_OTHER_CONTENT_TYPE;
    minimalSourceDataSizeInBytes = DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES;
    minimalCompressRatio = DEFAULT_MINIMAL_COMPRESS_RATIO;
    compressContentTypes = parseContentTypes(DEFAULT_CONTENT_TYPES_COMPRESSIBLE, DEFAULT_CONTENT_TYPES_INCOMPRESSIBLE);
    compressContentPrefixes = parseContentPrefixes(DEFAULT_CONTENT_PREFIXES_COMPRESSIBLE,
        DEFAULT_CONTENT_PREFIXES_INCOMPRESSIBLE);
  }

  /**
   * Construct a compression config instance using a specific verifiable properties.
   * @param verifiableProperties The verifiable properties.
   */
  public CompressionConfig(VerifiableProperties verifiableProperties) {
    isCompressionEnabled = verifiableProperties.getBoolean(CONFIG_COMPRESSION_ENABLED, DEFAULT_COMPRESSION_ENABLED);
    isSkipWithContentEncoding = verifiableProperties.getBoolean(CONFIG_SKIP_CONTENT_ENCODING,
        DEFAULT_SKIP_CONTENT_ENCODING);
    algorithmName = verifiableProperties.getString(CONFIG_ALGORITHM_NAME, DEFAULT_ALGORITHM_NAME);
    compressOtherContentTypes = verifiableProperties.getBoolean(CONFIG_COMPRESS_OTHER_CONTENT_TYPE,
        DEFAULT_COMPRESS_OTHER_CONTENT_TYPE);
    minimalSourceDataSizeInBytes = verifiableProperties.getInt(CONFIG_MINIMAL_DATA_SIZE_IN_BYTES,
        DEFAULT_MINIMAL_DATA_SIZE_IN_BYTES);
    minimalCompressRatio = verifiableProperties.getDouble(CONFIG_MINIMAL_COMPRESS_RATIO,
        DEFAULT_MINIMAL_COMPRESS_RATIO);

    // Build the content-type map.
    String compressibleContentTypes = verifiableProperties.getString(CONFIG_CONTENT_TYPES_COMPRESSIBLE,
        DEFAULT_CONTENT_TYPES_COMPRESSIBLE);
    String incompressibleContentTypes = verifiableProperties.getString(CONFIG_CONTENT_TYPES_INCOMPRESSIBLE,
        DEFAULT_CONTENT_TYPES_INCOMPRESSIBLE);
    compressContentTypes = parseContentTypes(compressibleContentTypes, incompressibleContentTypes);

    // build the content-type prefix map.
    String compressibleContentPrefixes = verifiableProperties.getString(CONFIG_CONTENT_PREFIXES_COMPRESSIBLE,
        DEFAULT_CONTENT_PREFIXES_COMPRESSIBLE);
    String incompressibleContentPrefixes = verifiableProperties.getString(CONFIG_CONTENT_PREFIXES_INCOMPRESSIBLE,
        DEFAULT_CONTENT_PREFIXES_INCOMPRESSIBLE);
    compressContentPrefixes = parseContentPrefixes(compressibleContentPrefixes, incompressibleContentPrefixes);
  }

  /**
   * Combine the compressible and incompressible list of content-types into a map of content-type to boolean.
   *
   * @param compressibleContentTypes List of compressible content-types.
   * @param incompressibleContentTypes List of incompressible content-types.
   * @return A map from content-type to boolean (compressible or incompressible).
   */
  private Map<String, Boolean> parseContentTypes(String compressibleContentTypes, String incompressibleContentTypes) {
    Map<String, Boolean> result = new HashMap<>();
    addContentTypesToMap(result, compressibleContentTypes, true);
    addContentTypesToMap(result, incompressibleContentTypes, false);
    return result;
  }

  /**
   * Combine the compressible and incompressible list of content-type prefixes into a map of prefix to boolean.
   *
   * @param compressibleContentPrefixes List of compressible content-types.
   * @param incompressibleContentPrefixes List of incompressible content-types.
   * @return A map from content-type prefix to boolean (compressible or incompressible).
   */
  private Map<String, Boolean> parseContentPrefixes(String compressibleContentPrefixes,
      String incompressibleContentPrefixes) {
    Map<String, Boolean> result = new HashMap<>();
    addContentPrefixToMap(result, compressibleContentPrefixes, true);
    addContentPrefixToMap(result, incompressibleContentPrefixes, false);
    return result;
  }

  /**
   * A helper method to build the map from content-type to compressibility value.
   * @param map The map to store the result.
   * @param contentTypesCsv A comma-separated content-types to add.  The keys will be split, trimmed, in lower case.
   * @param value The fixed value to add to the map for each key.
   */
  private void addContentTypesToMap(Map<String, Boolean> map, String contentTypesCsv, boolean value) {
    if (contentTypesCsv != null && contentTypesCsv.length() > 0) {
      for (String contentType : contentTypesCsv.split(",")) {
        String loweredContentType = contentType.trim().toLowerCase(Locale.ENGLISH);
        if (loweredContentType.length() > 0) {
          map.put(loweredContentType, value);
        }
      }
    }
  }

  /**
   * A helper method to build the map from content-type to compressibility value.
   * @param map The map to store the result.
   * @param contentPrefixesCsv A comma-separated prefixes to add.  The keys will be split, trimmed, in lower case.
   * @param value The fixed value to add to the map for each key.
   */
  private void addContentPrefixToMap(Map<String, Boolean> map, String contentPrefixesCsv, boolean value) {
    if (contentPrefixesCsv != null && contentPrefixesCsv.length() > 0) {
      for (String contentPrefix : contentPrefixesCsv.split(",")) {
        String loweredContentPrefix = contentPrefix.trim().toLowerCase(Locale.ENGLISH);
        // If prefix contains slash like "text/*", pickup the prefix "text".
        int separatorPrefix = loweredContentPrefix.indexOf("/");
        if (separatorPrefix >= 0) {
          loweredContentPrefix = loweredContentPrefix.substring(0, separatorPrefix);
        }
        if (loweredContentPrefix.length() > 0) {
          map.put(loweredContentPrefix, value);
        }
      }
    }
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
   * Content check is applied in this order:
   * 1. Check whether the full contentType is specified in content-type map.  Example "text/html; charset=UTF-8"
   * 2. If contentType contains option (separated by ";"), remove the option and check content-type map again.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-type "text/html".
   * 3. Get the content-type prefix (the string before the "/" separator) and lookup prefix in the content prefix map.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-prefix "text".
   * 4. The content-type is not configured, return the compressOtherContentTypes value.
   * @param contentType The HTTP content type.
   * @return TRUE if compression allowed; FALSE if compression not allowed.
   */
  public boolean isCompressibleContentType(String contentType) {
    // If there is no content-type, assume it's other content-type.
    if (Utils.isNullOrEmpty(contentType)) {
      return compressOtherContentTypes;
    }

    // Check whether there's an exact match by the full content-type.
    String contentTypeLower = contentType.trim().toLowerCase();
    Boolean isCompressible = compressContentTypes.get(contentTypeLower);
    if (isCompressible != null) {
      return isCompressible;
    }

    // Check whether the content-type contains options separated by ";"
    int mimeSeparatorIndex = contentTypeLower.indexOf(';');
    if (mimeSeparatorIndex > 0) {
      String contentTypeWithoutOption = contentTypeLower.substring(0, mimeSeparatorIndex);
      isCompressible = compressContentTypes.get(contentTypeWithoutOption);
      if (isCompressible != null) {
        return isCompressible;
      }
    }

    // Check whether there's a match by content-type prefix.
    int prefixSeparatorIndex = contentTypeLower.indexOf('/');
    if (prefixSeparatorIndex > 0) {
      String prefix = contentTypeLower.substring(0, prefixSeparatorIndex);
      isCompressible = compressContentPrefixes.get(prefix);
      if (isCompressible != null) {
        return isCompressible;
      }
    }

    // Content-type is not found in either map.  Apply other content-type value.
    return compressOtherContentTypes;
  }
}
