/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsMySqlStoreFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.utils.Utils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link AmbryStorageQuotaServiceFactory}.
 * The reason to have unit test here is we can only be sure mysql server is running in integration test.
 */
public class AmbryStorageQuotaServiceFactoryTest {
  private final Properties properties;
  private final VerifiableProperties verifiableProperties;
  private final MetricRegistry metricRegistry;
  private final AccountStatsStore accountStatsStore;

  public AmbryStorageQuotaServiceFactoryTest() throws Exception {
    properties = createProperties();
    verifiableProperties = new VerifiableProperties(properties);
    metricRegistry = new MetricRegistry();
    accountStatsStore = createAccountStatsStore();
  }

  @Test
  public void testBasic() throws Exception {
    StorageQuotaService service = new AmbryStorageQuotaServiceFactory(verifiableProperties, accountStatsStore,
        metricRegistry).getStorageQuotaService();
    assertNotNull(service);
  }

  private Properties createProperties() throws Exception {
    Path tempDir = Files.createTempDirectory("AmbryStorageQuotaServiceFactoryTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");
    Properties configProps = Utils.loadPropsFromResource("mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, "Ambry-test");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, "ambry.github.com");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(11111));
    configProps.setProperty(StorageQuotaConfig.BACKUP_FILE_DIR, localBackupFilePath.toString());
    configProps.setProperty(StorageQuotaConfig.MYSQL_STORE_RETRY_BACKOFF_MS, String.valueOf(1000));
    configProps.setProperty(StorageQuotaConfig.MYSQL_STORE_RETRY_MAX_COUNT, "1");
    return configProps;
  }

  private AccountStatsStore createAccountStatsStore() throws Exception {
    return new AccountStatsMySqlStoreFactory(verifiableProperties, new ClusterMapConfig(verifiableProperties),
        new StatsManagerConfig(verifiableProperties), metricRegistry).getAccountStatsMySqlStore();
  }
}
