package com.github.ambry.cloud.azure;

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
    document.set(CloudBlobMetadata.FIELD_UPDATE_TIME, System.currentTimeMillis());
    return document;
  }
}
