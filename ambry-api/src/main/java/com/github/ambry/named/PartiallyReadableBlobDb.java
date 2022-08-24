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
 *
 */

package com.github.ambry.named;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import java.util.List;

public interface PartiallyReadableBlobDb {

  List<PartiallyReadableBlobRecord> get(String accountName, String containerName, String blobName, long chunkOffset)
      throws RestServiceException;

  void put(PartiallyReadableBlobRecord record) throws RestServiceException;

  BlobInfo getBlobInfo(String accountName, String containerName, String blobName, RestRequest restRequest)
      throws RestServiceException;

  void putBlobInfo(String accountName, String containerName, String blobName, long blobSize, String serviceId,
      byte[] userMetadata) throws RestServiceException;

  void updateStatus(String accountName, String containerName, String blobName) throws RestServiceException;
}