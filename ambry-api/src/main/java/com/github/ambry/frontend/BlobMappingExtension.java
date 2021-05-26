/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * A layer for getting mapping Ambry blob Ids for external service url paths. Implementations of this could also do other
 * operations such as validations of incoming request based on its headers such as authorization, date etc.
 */
public interface BlobMappingExtension {

  /**
   * Return a mapping record {@link BlobMappingRecord} from external service url paths to Ambry blob Ids.
   * @param externalPath path of external url.
   * @param requestHeaders map of incoming request headers like authorization, date etc that might be used to validate
   *                       incoming request before fetching Ambry blob mapping.
   * @return a {@link CompletableFuture} that will eventually contain either the {@link BlobMappingRecord} record
   *         or an exception given by the external service.
   */
  CompletableFuture<BlobMappingRecord> getBlobMapping(String externalPath, Map<String, String> requestHeaders);

  /**
   * Represents a mapping record of external url path to Ambry blob Id and any headers such as cache-control, etc which
   * might need to be included in response to the request.
   */
  class BlobMappingRecord {
    private final String externalPath;
    private final String ambryId;
    private final Map<String, String> responseHeaders;

    /**
     * @param externalPath external url path.
     * @param ambryId Ambry blob Id.
     * @param responseHeaders headers which needs to be included in response for this request. For example, cache-control, etc.
     */
    public BlobMappingRecord(String externalPath, String ambryId, Map<String, String> responseHeaders) {
      this.externalPath = externalPath;
      this.ambryId = ambryId;
      this.responseHeaders = responseHeaders;
    }

    /**
     * @return the external url path.
     */
    public String getExternalId() {
      return externalPath;
    }

    /**
     * @return the mapping Ambry blob ID.
     */
    public String getAmbryId() {
      return ambryId;
    }

    /**
     * @return headers which needs to be included in response for this request. For example, cache-control, etc.
     */
    public Map<String, String> getResponseHeaders() {
      return Collections.unmodifiableMap(responseHeaders);
    }
  }
}
