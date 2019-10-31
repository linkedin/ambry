package com.github.ambry.cloud.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.microsoft.azure.documentdb.Document;


public class AzureTestUtil {
  /**
   * Create {@link Document} object from {@link CloudBlobMetadata} object.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} object.
   * @return {@link Document} object.
   */
  public static Document createDocumentFromCloudBlobMetadata(ObjectMapper objectMapper,
      CloudBlobMetadata cloudBlobMetadata) throws JsonProcessingException {
    Document document = new Document(objectMapper.writeValueAsString(cloudBlobMetadata));
    document.set(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, System.currentTimeMillis());
    return document;
  }
}
