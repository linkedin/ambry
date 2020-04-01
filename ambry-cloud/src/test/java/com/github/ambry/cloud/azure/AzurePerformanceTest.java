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

import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzurePerformanceTest {

  private static final Logger logger = LoggerFactory.getLogger(AzurePerformanceTest.class);
  private static final String STORAGE_CONNECTION_STRING = "azure.storage.connection.string";
  private static AzureBlobDataAccessor blobDataAccessor;
  private static final String commandName = AzurePerformanceTest.class.getSimpleName();

  public static void main(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException("Usage: " + commandName + " <propertiesFilePath>");
    }
    String propFileName = args[0];
    Properties props = new Properties();
    try (InputStream input = new FileInputStream(propFileName)) {
      if (input == null) {
        throw new IllegalArgumentException("Could not find file: " + propFileName);
      }
      props.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from file: " + propFileName);
    }
    String connectionString = props.getProperty(STORAGE_CONNECTION_STRING);
    if (connectionString == null) {
      throw new IllegalArgumentException("Required property not found: " + STORAGE_CONNECTION_STRING);
    }
    props.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, connectionString);
    String[] requiredCosmosProperties =
        {AzureCloudConfig.COSMOS_ENDPOINT, AzureCloudConfig.COSMOS_COLLECTION_LINK, AzureCloudConfig.COSMOS_KEY};
    for (String cosmosProperty : requiredCosmosProperties) {
      props.setProperty(cosmosProperty, "something");
    }

    try {
      VerifiableProperties verifiableProperties = new VerifiableProperties(props);
      CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
      AzureCloudConfig azureCloudConfig = new AzureCloudConfig(verifiableProperties);
      String clusterName = "perftest";
      AzureBlobLayoutStrategy blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
      blobDataAccessor = new AzureBlobDataAccessor(cloudConfig, azureCloudConfig, blobLayoutStrategy,
          new AzureMetrics(new MetricRegistry()));

      testPerformance();
    } catch (Exception ex) {
      logger.error("Command {} failed", commandName, ex);
      System.exit(1);
    }
  }

  public static void testPerformance() throws IOException, BlobStorageException {
    // create container with this class name
    String containerName = commandName.toLowerCase();

    // Warm up connection with a few untimed uploads
    byte[] buffer = new byte[100];
    Arrays.fill(buffer, (byte) 'x');
    for (int j = 0; j < 10; j++) {
      String blobName = String.format("Warmup-%d", j);
      InputStream inputStream = new ByteArrayInputStream(buffer);
      blobDataAccessor.uploadFile(containerName, blobName, inputStream);
    }

    int testCount = 100;
    int blobSizes[] = {1000, 100000, 1000000};
    String sizeLabels[] = {"1K", "100K", "1M"};

    // upload blobs of different sizes: 1K, 100K, 1M, each count times, get mean/max time per blob size
    for (int sizeIndex = 0; sizeIndex < sizeLabels.length; sizeIndex++) {
      String label = sizeLabels[sizeIndex];
      buffer = new byte[blobSizes[sizeIndex]];
      Arrays.fill(buffer, (byte) 'x');
      long meanTime = 0, maxTime = 0, totalTime = 0;
      for (int j = 0; j < testCount; j++) {
        String blobName = String.format("Test-%s-%d", label, j);
        InputStream inputStream = new ByteArrayInputStream(buffer);
        long startTime = System.currentTimeMillis();
        blobDataAccessor.uploadFile(containerName, blobName, inputStream);
        long uploadTime = System.currentTimeMillis() - startTime;
        totalTime += uploadTime;
        maxTime = Math.max(maxTime, uploadTime);
      }
      meanTime = totalTime / testCount;
      String msg = String.format("Upload time for blob size %s: mean %d, max %d", label, meanTime, maxTime);
      System.out.println(msg);
    }

    // same thing with download
    for (int sizeIndex = 0; sizeIndex < sizeLabels.length; sizeIndex++) {
      String label = sizeLabels[sizeIndex];
      long meanTime = 0, maxTime = 0, totalTime = 0;
      for (int j = 0; j < testCount; j++) {
        String blobName = String.format("Test-%s-%d", label, j);
        OutputStream outputStream = new ByteArrayOutputStream(blobSizes[sizeIndex]);
        long startTime = System.currentTimeMillis();
        blobDataAccessor.downloadFile(containerName, blobName, outputStream, true);
        long downloadTime = System.currentTimeMillis() - startTime;
        totalTime += downloadTime;
        maxTime = Math.max(maxTime, downloadTime);
      }
      meanTime = totalTime / testCount;
      String msg = String.format("Download time for blob size %s: mean %d, max %d", label, meanTime, maxTime);
      System.out.println(msg);
    }
  }
}
