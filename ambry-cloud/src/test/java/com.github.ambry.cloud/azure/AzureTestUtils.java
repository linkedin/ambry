/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.microsoft.azure.documentdb.Document;
import java.io.IOException;


/**
 * Class to define utilities for azure tests.
 */
public class AzureTestUtils {

  /**
   * Create {@link Document} object from {@link CloudBlobMetadata} object.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} object.
   * @return {@link Document} object.
   */
  public static Document createDocumentFromCloudBlobMetadata(CloudBlobMetadata cloudBlobMetadata,
      ObjectMapper objectMapper) throws IOException {
    Document document = new Document(objectMapper.writeValueAsString(cloudBlobMetadata));
    document.set(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, System.currentTimeMillis());
    return document;
  }

  /**
   * Create {@link Document} object from {@link CloudBlobMetadata} object with specified updateTime.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} object.
   * @param uploadTime specified upload time.
   * @return {@link Document} object.
   */
  public static Document createDocumentFromCloudBlobMetadata(CloudBlobMetadata cloudBlobMetadata, long uploadTime,
      ObjectMapper objectMapper) throws JsonProcessingException {
    Document document = new Document(objectMapper.writeValueAsString(cloudBlobMetadata));
    document.set(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, uploadTime);
    return document;
  }
}
