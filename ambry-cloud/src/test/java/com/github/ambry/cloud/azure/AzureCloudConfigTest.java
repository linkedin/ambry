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

import com.github.ambry.config.VerifiableProperties;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class AzureCloudConfigTest {
  private Properties configProps = new Properties();

  @Before
  public void setup() throws Exception {
    AzureTestUtils.setConfigProperties(configProps, 0);
  }

  /** Test Azure cloud config for a single storage account */
  @Test
  public void testStorageAccountInfoConfigSimple() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"0-1000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount1.blob.core.windows.net/.default\",\n"
            + "        \"storageConnectionString\":\"https://testStorageAccount1.blob.core.windows.net/\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount1.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    List<AzureCloudConfig.StorageAccountInfo> storageAccountInfoList = azureCloudConfig.azureStorageAccountInfo;
    Assert.assertEquals(1, storageAccountInfoList.size());
    Assert.assertEquals(0, storageAccountInfoList.get(0).getPartitionRangeStart());
    Assert.assertEquals(1000000, storageAccountInfoList.get(0).getPartitionRangeEnd());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net/.default",
        storageAccountInfoList.get(0).getStorageScope());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net/",
        storageAccountInfoList.get(0).getStorageConnectionString());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net",
        storageAccountInfoList.get(0).getStorageEndpoint());
  }

  /** Test Azure cloud config for a multiple storage accounts */
  @Test
  public void testStorageAccountInfoConfigMultipleAccounts() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"0-1000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount1.blob.core.windows.net/.default\",\n"
            + "        \"storageConnectionString\":\"https://testStorageAccount1.blob.core.windows.net/\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount1.blob.core.windows.net\",\n" + "      },\n"
            + "      {\n" + "        \"name\":\"testStorageAccount2\",\n"
            + "        \"partitionRange\":\"1000000-10000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount2.blob.core.windows.net/.default\",\n"
            + "        \"storageConnectionString\":\"https://testStorageAccount2.blob.core.windows.net/\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount2.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    List<AzureCloudConfig.StorageAccountInfo> storageAccountInfoList = azureCloudConfig.azureStorageAccountInfo;
    Assert.assertEquals(2, storageAccountInfoList.size());
    Assert.assertEquals(0, storageAccountInfoList.get(0).getPartitionRangeStart());
    Assert.assertEquals(1000000, storageAccountInfoList.get(0).getPartitionRangeEnd());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net/.default",
        storageAccountInfoList.get(0).getStorageScope());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net/",
        storageAccountInfoList.get(0).getStorageConnectionString());
    Assert.assertEquals("https://testStorageAccount1.blob.core.windows.net",
        storageAccountInfoList.get(0).getStorageEndpoint());
    Assert.assertEquals(1000000, storageAccountInfoList.get(1).getPartitionRangeStart());
    Assert.assertEquals(10000000, storageAccountInfoList.get(1).getPartitionRangeEnd());
    Assert.assertEquals("https://testStorageAccount2.blob.core.windows.net/.default",
        storageAccountInfoList.get(1).getStorageScope());
    Assert.assertEquals("https://testStorageAccount2.blob.core.windows.net/",
        storageAccountInfoList.get(1).getStorageConnectionString());
    Assert.assertEquals("https://testStorageAccount2.blob.core.windows.net",
        storageAccountInfoList.get(1).getStorageEndpoint());
  }

  /** Test Azure cloud config for a storage account with no account name */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigNoAccountName() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"partitionRange\":\"0-100\",\n"
            + "        \"storageScope\":\"https://wus2ambryprodblobstore1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://wus2ambryprodblobstore1.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }

  /** Test Azure cloud config for a storage account with no partition range format */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigNoPartitionRange() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"storageScope\":\"https://wus2ambryprodblobstore1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://wus2ambryprodblobstore1.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }

  /** Test Azure cloud config for a storage account with invalid partition range format */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigBadFormat() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"0-\",\n"
            + "        \"storageScope\":\"https://wus2ambryprodblobstore1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://wus2ambryprodblobstore1.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }

  /** Test Azure cloud config for a storage account with invalid partition range */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigBadRange() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"10-10\",\n"
            + "        \"storageScope\":\"https://wus2ambryprodblobstore1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://wus2ambryprodblobstore1.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }

  /** Test Azure cloud config for multiple storage accounts with invalid partition ranges */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigBadRanges() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"0-1000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount1.blob.core.windows.net\",\n" + "      },\n"
            + "      {\n" + "        \"name\":\"testStorageAccount2\",\n"
            + "        \"partitionRange\":\"100000-10000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount2.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount2.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }

  /** Test Azure cloud config for multiple storage accounts with uncovered partition ids */
  @Test(expected = IllegalArgumentException.class)
  public void testStorageAccountInfoConfigWithGap() {
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
        "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"testStorageAccount1\",\n"
            + "        \"partitionRange\":\"0-1000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount1.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount1.blob.core.windows.net\",\n" + "      },\n"
            + "      {\n" + "        \"name\":\"testStorageAccount2\",\n"
            + "        \"partitionRange\":\"1000002-10000000\",\n"
            + "        \"storageScope\":\"https://testStorageAccount2.blob.core.windows.net/.default\",\n"
            + "        \"storageEndpoint\":\"https://testStorageAccount2.blob.core.windows.net\",\n" + "      }\n"
            + "    ]\n" + "    }");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
  }
}
