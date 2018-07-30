/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.account.CloudReplicationConfig;
import java.io.InputStream;


public interface CloudDestination {

  /**
   * Initialize the destination with the specified configuration string.
   * @param config The configuration to use
   * @throws Exception if initialization encountered an error.
   */
  void initialize(CloudReplicationConfig config) throws Exception;

    /**
     * Upload blob to the cloud destination.
     * @param blobId id of the Ambry blob
     * @param blobSize size of the blob in bytes
     * @param blobInputStream the stream to read blob data
     * @return flag indicating whether the blob was uploaded
     * @throws Exception if an exception occurs
     */
  boolean uploadBlob(String blobId, long blobSize, InputStream blobInputStream) throws Exception;

  /**
   * Delete blob in the cloud destination.
   * @param blobId id of the Ambry blob
   * @return flag indicating whether the blob was deleted
   * @throws Exception if an exception occurs
   */
  boolean deleteBlob(String blobId) throws Exception;
}
