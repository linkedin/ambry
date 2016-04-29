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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a utility class used by Router.
 */
public class RouterUtils {

  private static Logger logger = LoggerFactory.getLogger(RestUtils.class);

  /**
   * Get {@link BlobId} from a blob string.
   * @param blobIdString The string of blobId.
   * @param clusterMap The {@link ClusterMap} based on which to generate the {@link BlobId}.
   * @return BlobId
   * @throws RouterException If parsing a string blobId fails.
   */
  static BlobId getBlobIdFromString(String blobIdString, ClusterMap clusterMap)
      throws RouterException {
    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
      logger.trace("BlobId created " + blobId + " with partition " + blobId.getPartition());
    } catch (Exception e) {
      logger.error("Caller passed in invalid BlobId " + blobIdString);
      throw new RouterException("BlobId is invalid " + blobIdString, RouterErrorCode.InvalidBlobId);
    }
    return blobId;
  }
}
