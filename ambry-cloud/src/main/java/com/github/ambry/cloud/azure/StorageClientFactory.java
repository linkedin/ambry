/**
 * Copyright 2020  LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.github.ambry.config.CloudConfig;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;


/**
 * An interface to create {@link BlobServiceClient} object.
 */
public interface StorageClientFactory {

  /**
   * Create the {@link BlobServiceClient} object.
   * @return {@link BlobServiceClient} object.
   */
  BlobServiceClient createBlobStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException;
}
