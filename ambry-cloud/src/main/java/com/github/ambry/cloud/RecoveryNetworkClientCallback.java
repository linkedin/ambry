/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.models.BlobItem;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;

public interface RecoveryNetworkClientCallback {
  default void onListBlobs(ReplicaMetadataRequestInfo request, PagedResponse<BlobItem> response) {}
}
