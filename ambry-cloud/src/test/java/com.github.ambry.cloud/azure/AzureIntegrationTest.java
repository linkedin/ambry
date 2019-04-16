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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;


/**
 * Integration Test cases for {@link AzureCloudDestination}
 * Must supply file azure-test.properties in classpath with valid config property values.
 */
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class AzureIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(AzureIntegrationTest.class);
  private AzureCloudDestination azureDest;
  private int blobSize = 1024;
  private byte dataCenterId = 66;
  private short accountId = 101;
  private short containerId = 5;
  private String cosmosCollectionLink;

  @Before
  public void setup() throws Exception {
    String propFileName = "azure-test.properties";
    Properties props = new Properties();
    try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(propFileName)) {
      if (input == null) {
        throw new IllegalStateException("Could not find resource: " + propFileName);
      }
      props.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from resource: " + propFileName);
    }
    VerifiableProperties verProps = new VerifiableProperties(props);
    azureDest =
        (AzureCloudDestination) new AzureCloudDestinationFactory(verProps, new MetricRegistry()).getCloudDestination();
    cosmosCollectionLink = verProps.getString(AzureCloudConfig.COSMOS_COLLECTION_LINK);
  }

  /**
   * Test normal operations.
   * @throws Exception on error
   */
  @Test
  public void testNormalFlow() throws Exception {
    PartitionId partitionId = new MockPartitionId();
    BlobId blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    InputStream inputStream = getBlobInputStream(blobSize);
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize);
    assertTrue("Expected upload to return true",
        azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    // Try to upload same blob again
    assertFalse("Expected duplicate upload to return false",
        azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    long expirationTime = Utils.Infinite_Time;
    assertTrue("Expected update to return true", azureDest.updateBlobExpiration(blobId, expirationTime));
    CloudBlobMetadata metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(blobId.getID());
    assertEquals(expirationTime, metadata.getExpirationTime());
    long deletionTime = System.currentTimeMillis() + 1000;
    assertTrue("Expected deletion to return true", azureDest.deleteBlob(blobId, deletionTime));
    metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(blobId.getID());
    assertEquals(deletionTime, metadata.getDeletionTime());
  }

  /**
   * Test batch query on large number of blobs.
   * @throws Exception on error
   */
  @Test
  public void testBatchQuery() throws Exception {
    int numBlobs = 100;
    long partition = 666;
    PartitionId partitionId = new MockPartitionId(partition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    List<BlobId> blobIdList = new ArrayList<>();
    long creationTime = System.currentTimeMillis();
    for (int j = 0; j < numBlobs; j++) {
      BlobId blobId =
          new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
              BlobDataType.DATACHUNK);
      blobIdList.add(blobId);
      InputStream inputStream = getBlobInputStream(blobSize);
      CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    }

    Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(blobIdList);
    assertEquals("Unexpected size of returned metadata map", numBlobs, metadataMap.size());
    for (BlobId blobId : blobIdList) {
      CloudBlobMetadata metadata = metadataMap.get(blobId.getID());
      assertNotNull("No metadata found for blobId: " + blobId);
      assertEquals("Unexpected metadata id", blobId.getID(), metadata.getId());
      assertEquals("Unexpected metadata accountId", accountId, metadata.getAccountId());
      assertEquals("Unexpected metadata containerId", containerId, metadata.getContainerId());
      assertEquals("Unexpected metadata partitionId", partitionId.toPathString(), metadata.getPartitionId());
      assertEquals("Unexpected metadata creationTime", creationTime, metadata.getCreationTime());
    }

    // Cleanup
    DocumentClient documentClient = azureDest.getDocumentClient();
    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPartitionKey(new PartitionKey(partitionId.toPathString()));
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setPartitionKey(feedOptions.getPartitionKey());
    FeedResponse<Document> feedResponse =
        documentClient.queryDocuments(cosmosCollectionLink, "SELECT * FROM c", feedOptions);
    int numDeletes = 0;
    for (Document document : feedResponse.getQueryIterable().toList()) {
      documentClient.deleteDocument(document.getSelfLink(), requestOptions);
      numDeletes++;
    }
    logger.info("Deleted {} metadata documents in partition {}", numDeletes, partitionId.toPathString());
  }

  /**
   * Utility method to get blob input stream.
   * @param blobSize size of blob to consider.
   * @return the blob input stream.
   */
  private static InputStream getBlobInputStream(int blobSize) {
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    return new ByteArrayInputStream(randomBytes);
  }
}
