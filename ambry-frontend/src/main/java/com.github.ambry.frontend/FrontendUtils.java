/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;


/**
 * Common utility functions that will be used across frontend package
 */
class FrontendUtils {

  /**
   * Fetches {@link BlobId} from the given string representation of BlobId
   * @param blobIdStr string representation of BlobId
   * @param clusterMap {@link ClusterMap} instance to use
   * @return the {@link BlobId} thus generated
   * @throws RestServiceException on invalid blobId
   */
  static BlobId getBlobIdFromString(String blobIdStr, ClusterMap clusterMap) throws RestServiceException {
    try {
      return new BlobId(blobIdStr, clusterMap);
    } catch (Exception e) {
      throw new RestServiceException("Invalid blob id=" + blobIdStr, RestServiceErrorCode.BadRequest);
    }
  }
}
