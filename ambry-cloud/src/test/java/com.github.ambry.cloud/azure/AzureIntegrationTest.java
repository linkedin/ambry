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
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.After;
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
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class AzureIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(AzureIntegrationTest.class);
  private final String vcrKmsContext = "backup-default";
  private final String cryptoAgentFactory = CloudConfig.DEFAULT_CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS;
  private VerifiableProperties verifiableProperties;
  private AzureCloudDestination azureDest;
  private int blobSize = 1024;
  private byte dataCenterId = 66;
  private short accountId = 101;
  private short containerId = 5;
  private long testPartition = 666;
  // one day retention
  private int retentionPeriodDays = 1;
  private String propFileName = "azure-test.properties";
  private String tokenFileName = "replicaTokens";
  private Properties testProperties;

  @Before
  public void setup() {
    testProperties = new Properties();
    try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(propFileName)) {
      if (input == null) {
        throw new IllegalStateException("Could not find resource: " + propFileName);
      }
      testProperties.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from resource: " + propFileName);
    }
    testProperties.setProperty("clustermap.cluster.name", "Integration-Test");
    testProperties.setProperty("clustermap.datacenter.name", "uswest");
    testProperties.setProperty("clustermap.host.name", "localhost");
    testProperties.setProperty(CloudConfig.CLOUD_DELETED_BLOB_RETENTION_DAYS, String.valueOf(retentionPeriodDays));
    verifiableProperties = new VerifiableProperties(testProperties);
    azureDest = getAzureDestination(verifiableProperties);
  }

  /**
   * @return an {@link AzureCloudDestination} instance built from the supplied properties.
   * @param verProps the {@link VerifiableProperties} to use.
   */
  private AzureCloudDestination getAzureDestination(VerifiableProperties verProps) {
    return (AzureCloudDestination) new AzureCloudDestinationFactory(verProps,
        new MetricRegistry()).getCloudDestination();
  }

  @After
  public void destroy() throws IOException {
    if (azureDest != null) {
      azureDest.close();
    }
  }

  /**
   * Test normal operations.
   * @throws Exception on error
   */
  @Test
  public void testNormalFlow() throws Exception {
    PartitionId partitionId = new MockPartitionId(testPartition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    BlobId blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    byte[] uploadData = TestUtils.getRandomBytes(blobSize);
    InputStream inputStream = new ByteArrayInputStream(uploadData);
    long now = System.currentTimeMillis();
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, now, now + 60000, blobSize, CloudBlobMetadata.EncryptionOrigin.VCR, vcrKmsContext,
            cryptoAgentFactory, blobSize);
    assertTrue("Expected upload to return true",
        azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));

    // Get blob should return the same data
    verifyDownloadMatches(blobId, uploadData);

    // Try to upload same blob again
    assertFalse("Expected duplicate upload to return false",
        azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, new ByteArrayInputStream(uploadData)));
    long expirationTime = Utils.Infinite_Time;
    assertTrue("Expected update to return true", azureDest.updateBlobExpiration(blobId, expirationTime));
    CloudBlobMetadata metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(blobId.getID());
    assertEquals(expirationTime, metadata.getExpirationTime());
    long deletionTime = now + 10000;
    assertTrue("Expected deletion to return true", azureDest.deleteBlob(blobId, deletionTime));
    metadata = azureDest.getBlobMetadata(Collections.singletonList(blobId)).get(blobId.getID());
    assertEquals(deletionTime, metadata.getDeletionTime());

    azureDest.purgeBlobs(Collections.singletonList(metadata));
    assertTrue("Expected empty set after purge",
        azureDest.getBlobMetadata(Collections.singletonList(blobId)).isEmpty());

    // Get blob should fail after purge
    try {
      verifyDownloadMatches(blobId, uploadData);
      fail("download blob should fail after data is purged");
    } catch (CloudStorageException csex) {
    }
  }

  /**
   * Test batch query on large number of blobs.
   * @throws Exception on error
   */
  @Test
  public void testBatchQuery() throws Exception {
    cleanup();

    int numBlobs = 100;
    PartitionId partitionId = new MockPartitionId(testPartition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    List<BlobId> blobIdList = new ArrayList<>();
    long creationTime = System.currentTimeMillis();
    Map<BlobId, byte[]> blobIdtoDataMap = new HashMap<>();
    for (int j = 0; j < numBlobs; j++) {
      BlobId blobId =
          new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
              BlobDataType.DATACHUNK);
      blobIdList.add(blobId);
      byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
      blobIdtoDataMap.put(blobId, randomBytes);
      CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, new ByteArrayInputStream(randomBytes)));
    }
    long uploadTime = System.currentTimeMillis() - creationTime;
    logger.info("Uploaded {} blobs in {} ms", numBlobs, uploadTime);
    Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(blobIdList);
    assertEquals("Unexpected size of returned metadata map", numBlobs, metadataMap.size());
    for (BlobId blobId : blobIdList) {
      CloudBlobMetadata metadata = metadataMap.get(blobId.getID());
      assertNotNull("No metadata found for blobId: " + blobId, metadata);
      assertEquals("Unexpected metadata id", blobId.getID(), metadata.getId());
      assertEquals("Unexpected metadata accountId", accountId, metadata.getAccountId());
      assertEquals("Unexpected metadata containerId", containerId, metadata.getContainerId());
      assertEquals("Unexpected metadata partitionId", partitionId.toPathString(), metadata.getPartitionId());
      assertEquals("Unexpected metadata creationTime", creationTime, metadata.getCreationTime());
      assertEquals("Unexpected metadata encryption origin", CloudBlobMetadata.EncryptionOrigin.NONE,
          metadata.getEncryptionOrigin());

      verifyDownloadMatches(blobId, blobIdtoDataMap.get(blobId));
    }

    cleanup();
  }

  /**
   * Test blob compaction.
   * @throws Exception on error
   */
  @Test
  public void testPurgeDeadBlobs() throws Exception {
    cleanup();

    int bucketCount = 10;
    PartitionId partitionId = new MockPartitionId(testPartition, MockClusterMap.DEFAULT_PARTITION_CLASS);

    // Upload blobs in various lifecycle states
    long now = System.currentTimeMillis();
    long creationTime = now - TimeUnit.DAYS.toMillis(7);
    int expectedDeadBlobs = 0;
    for (int j = 0; j < bucketCount; j++) {
      Thread.sleep(20);
      logger.info("Uploading bucket {}", j);
      // Active blob
      BlobId blobId =
          new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
              BlobDataType.DATACHUNK);
      InputStream inputStream = getBlobInputStream(blobSize);
      CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));

      // Blob deleted before retention cutoff (should match)
      long timeOfDeath = now - TimeUnit.DAYS.toMillis(retentionPeriodDays + 1);
      blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
          BlobDataType.DATACHUNK);
      inputStream = getBlobInputStream(blobSize);
      cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      cloudBlobMetadata.setDeletionTime(timeOfDeath);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
      expectedDeadBlobs++;

      // Blob expired before retention cutoff (should match)
      blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
          BlobDataType.DATACHUNK);
      inputStream = getBlobInputStream(blobSize);
      cloudBlobMetadata =
          new CloudBlobMetadata(blobId, creationTime, timeOfDeath, blobSize, CloudBlobMetadata.EncryptionOrigin.NONE);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
      expectedDeadBlobs++;

      // Blob deleted after retention cutoff
      timeOfDeath = now - TimeUnit.HOURS.toMillis(1);
      blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
          BlobDataType.DATACHUNK);
      inputStream = getBlobInputStream(blobSize);
      cloudBlobMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      cloudBlobMetadata.setDeletionTime(timeOfDeath);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));

      // Blob expired after retention cutoff
      blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
          BlobDataType.DATACHUNK);
      inputStream = getBlobInputStream(blobSize);
      cloudBlobMetadata =
          new CloudBlobMetadata(blobId, creationTime, timeOfDeath, blobSize, CloudBlobMetadata.EncryptionOrigin.NONE);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream));
    }

    // run getDeadBlobs query, should return 2 * bucketCount
    String partitionPath = String.valueOf(testPartition);
    logger.info("First call to getDeletedBlobs");
    List<CloudBlobMetadata> deadBlobs = azureDest.getDeletedBlobs(partitionPath, now, bucketCount);
    assertEquals("Unexpected number returned", bucketCount, deadBlobs.size());
    logger.info("First call to purge");
    assertEquals("Not all blobs were purged", bucketCount, azureDest.purgeBlobs(deadBlobs));
    logger.info("Second call to getDeletedBlobs");
    deadBlobs = azureDest.getDeletedBlobs(partitionPath, now, bucketCount);
    assertEquals("Expected zero", 0, deadBlobs.size());
    logger.info("First call to getExpiredBlobs");
    deadBlobs = azureDest.getExpiredBlobs(partitionPath, now, bucketCount);
    assertEquals("Unexpected number returned", bucketCount, deadBlobs.size());
    logger.info("First call to purge");
    assertEquals("Not all blobs were purged", bucketCount, azureDest.purgeBlobs(deadBlobs));
    logger.info("Second call to getExpiredBlobs");
    deadBlobs = azureDest.getExpiredBlobs(partitionPath, now, bucketCount);
    assertEquals("Expected zero", 0, deadBlobs.size());
    cleanup();
  }

  /**
   * Test findEntriesSince with CosmosUpdateTimeFindTokenFactory.
   */
  @Test
  public void testFindEntriesSinceByUpdateTime() throws Exception {
    testFindEntriesSince("com.github.ambry.cloud.azure.CosmosUpdateTimeFindTokenFactory");
  }

  /**
   * Test findEntriesSince with CosmosChangeFeedFindTokenFactory.
   */
  @Ignore // Fails with wrong number of queries.
  @Test
  public void testFindEntriesSinceByChangeFeed() throws Exception {
    testFindEntriesSince("com.github.ambry.cloud.azure.CosmosChangeFeedFindTokenFactory");
  }

  /**
   * Test findEntriesSince with specified cloud token factory.
   * @#param replicationCloudTokenFactory the factory to use.
   * @throws Exception on error
   */
  private void testFindEntriesSince(String replicationCloudTokenFactory) throws Exception {

    logger.info("Testing findEntriesSince with {}", replicationCloudTokenFactory);
    testProperties.setProperty(ReplicationConfig.REPLICATION_CLOUD_TOKEN_FACTORY, replicationCloudTokenFactory);
    VerifiableProperties verifiableProperties = new VerifiableProperties(testProperties);
    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    FindTokenFactory findTokenFactory =
        new FindTokenHelper(null, replicationConfig).getFindTokenFactoryFromReplicaType(ReplicaType.CLOUD_BACKED);
    azureDest = (AzureCloudDestination) new AzureCloudDestinationFactory(verifiableProperties,
        new MetricRegistry()).getCloudDestination();

    cleanup();

    PartitionId partitionId = new MockPartitionId(testPartition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    String partitionPath = String.valueOf(testPartition);

    // Upload some blobs with different upload times
    int blobCount = 90;
    int chunkSize = 1000;
    int maxTotalSize = 20000;
    int expectedNumQueries = (blobCount * chunkSize) / maxTotalSize + 2;

    long now = System.currentTimeMillis();
    long startTime = now - TimeUnit.DAYS.toMillis(7);
    for (int j = 0; j < blobCount; j++) {
      BlobId blobId =
          new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
              BlobDataType.DATACHUNK);
      InputStream inputStream = getBlobInputStream(chunkSize);
      CloudBlobMetadata cloudBlobMetadata = new CloudBlobMetadata(blobId, startTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.VCR, vcrKmsContext, cryptoAgentFactory, chunkSize);
      cloudBlobMetadata.setUploadTime(startTime + j * 1000);
      assertTrue("Expected upload to return true",
          azureDest.uploadBlob(blobId, chunkSize, cloudBlobMetadata, inputStream));
    }

    FindToken findToken = findTokenFactory.getNewFindToken();
    // Call findEntriesSince in a loop until no new entries are returned
    FindResult findResult;
    int numQueries = 0;
    int totalBlobsReturned = 0;
    do {
      findResult = azureDest.findEntriesSince(partitionPath, findToken, maxTotalSize);
      findToken = findResult.getUpdatedFindToken();
      numQueries++;
      totalBlobsReturned += findResult.getMetadataList().size();
    } while (!findResult.getMetadataList().isEmpty());

    assertEquals("Wrong number of queries", expectedNumQueries, numQueries);
    assertEquals("Wrong number of blobs", blobCount, totalBlobsReturned);
    assertEquals("Wrong byte count", blobCount * chunkSize, findToken.getBytesRead());

    cleanup();
  }

  /**
   * Test that concurrent updates fail when the precondition does not match.
   * We don't test retries here since CloudBlobStoreTest covers that.
   */
  @Test
  public void testConcurrentUpdates() throws Exception {
    PartitionId partitionId = new MockPartitionId(testPartition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    BlobId blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    InputStream inputStream = getBlobInputStream(blobSize);
    long now = System.currentTimeMillis();
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, now, now + 60000, blobSize, CloudBlobMetadata.EncryptionOrigin.NONE);
    azureDest.uploadBlob(blobId, blobSize, cloudBlobMetadata, inputStream);
    // Different instance to simulate concurrent update in separate session.
    AzureCloudDestination concurrentUpdater = getAzureDestination(verifiableProperties);
    String fieldName = CloudBlobMetadata.FIELD_UPLOAD_TIME;
    long newUploadTime = now++;

    // Case 1: concurrent modification to blob metadata.
    azureDest.getAzureBlobDataAccessor()
        .setUpdateCallback(
            () -> concurrentUpdater.getAzureBlobDataAccessor().updateBlobMetadata(blobId, fieldName, newUploadTime));
    try {
      azureDest.updateBlobExpiration(blobId, ++now);
      fail("Expected 412 error");
    } catch (CloudStorageException csex) {
      // TODO: check nested exception is BlobStorageException with status code 412
      assertEquals("Expected update conflict", 1, azureDest.getAzureMetrics().blobUpdateConflictCount.getCount());
    }
    // Case 2: concurrent modification to Cosmos record.
    azureDest.getCosmosDataAccessor()
        .setUpdateCallback(
            () -> concurrentUpdater.getCosmosDataAccessor().updateMetadata(blobId, fieldName, newUploadTime));
    try {
      azureDest.updateBlobExpiration(blobId, ++now);
      fail("Expected 412 error");
    } catch (CloudStorageException csex) {
      // TODO: check nested exception is DocClientException with status code 412
      assertEquals("Expected update conflict", 2, azureDest.getAzureMetrics().blobUpdateConflictCount.getCount());
    }
  }

  private void cleanup() throws Exception {
    String partitionPath = String.valueOf(testPartition);
    Timer dummyTimer = new Timer();
    List<CloudBlobMetadata> allBlobsInPartition =
        azureDest.getCosmosDataAccessor().queryMetadata(partitionPath, "SELECT * FROM c", dummyTimer);
    int numPurged = azureDest.purgeBlobs(allBlobsInPartition);
    logger.info("Cleaned up {} blobs", numPurged);
  }

  /** Persist tokens to Azure, then read them back and verify they match. */
  @Test
  public void testTokens() throws Exception {
    String partitionPath = String.valueOf(testPartition);
    InputStream input = this.getClass().getClassLoader().getResourceAsStream(tokenFileName);
    if (input == null) {
      throw new IllegalStateException("Could not find resource: " + tokenFileName);
    }
    byte[] tokenBytes = new byte[2000];
    int tokensLength = input.read(tokenBytes, 0, 2000);
    tokenBytes = Arrays.copyOf(tokenBytes, tokensLength);
    InputStream tokenStream = new ByteArrayInputStream(tokenBytes);
    azureDest.persistTokens(partitionPath, tokenFileName, tokenStream);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(tokensLength);
    assertTrue("Retrieve tokens returned false", azureDest.retrieveTokens(partitionPath, tokenFileName, outputStream));
    assertArrayEquals("Retrieved token did not match sent", outputStream.toByteArray(), tokenBytes);
    // Try for nonexistent partition
    outputStream.reset();
    assertFalse("Expected retrieve to return false for nonexistent path",
        azureDest.retrieveTokens("unknown-path", tokenFileName, outputStream));
  }

  /**
   * test download matches the blob previously uploaded.
   * @param blobId id of the blob to download
   * @param uploadedData data uploaded to the blob
   * @throws CloudStorageException
   */
  private void verifyDownloadMatches(BlobId blobId, byte[] uploadedData) throws CloudStorageException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(blobSize);
    azureDest.downloadBlob(blobId, outputStream);
    assertArrayEquals("Downloaded data should match the uploaded data", uploadedData, outputStream.toByteArray());
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
