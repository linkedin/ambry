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

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;


/**
 * Integration Test cases for {@link AzureCloudDestination}
 * Must be supplied with valid system property values for:
 *   "storageConfigSpec" (Azure Blob Storage connection string)
 *   "cosmosEndpoint"
 *   "cosmosCollectionLink"
 *   "cosmosKey"
 */
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class AzureIntegrationTest {

  private AzureCloudDestination azureDest;
  private int blobSize = 1024;
  private byte dataCenterId = 66;
  private short accountId = 101;
  private short containerId = 5;

  @Before
  public void setup() throws Exception {

    VerifiableProperties verProps = new VerifiableProperties(System.getProperties());
    azureDest = new AzureCloudDestination(verProps);
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
    CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, 0, blobSize);
    assertTrue("Expected upload to return true",
        azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    long expirationTime = Utils.Infinite_Time;
    assertTrue("Expected update to return true", azureDest.updateBlobExpiration(blobId, expirationTime));
    CloudBlobMetadata metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(0);
    assertEquals(expirationTime, metadata.getExpirationTime());
    long deletionTime = System.currentTimeMillis() + 1000;
    assertTrue("Expected deletion to return true", azureDest.deleteBlob(blobId, deletionTime));
    metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(0);
    assertEquals(deletionTime, metadata.getDeletionTime());
  }

  /**
   * Test batch query on large number of blobs.
   * @throws Exception on error
   */
  @Test
  public void testBatchQuery() throws Exception {
    int numBlobs = 100;
    PartitionId partitionId = new MockPartitionId(666, MockClusterMap.DEFAULT_PARTITION_CLASS);
    List<BlobId> blobIdList = new ArrayList<>();
    long creationTime = System.currentTimeMillis();
    for (int j = 0; j < numBlobs; j++) {
      BlobId blobId =
          new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
              BlobDataType.DATACHUNK);
      blobIdList.add(blobId);
      InputStream inputStream = getBlobInputStream(blobSize);
      CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    }

    List<CloudBlobMetadata> metadataList = azureDest.getBlobMetadata(blobIdList);
    assertEquals(numBlobs, metadataList.size());
    for (CloudBlobMetadata metadata : metadataList) {
      assertEquals(accountId, metadata.getAccountId());
      assertEquals(containerId, metadata.getContainerId());
      assertEquals(partitionId.toPathString(), metadata.getPartitionId());
      assertEquals(creationTime, metadata.getCreationTime());
    }
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
