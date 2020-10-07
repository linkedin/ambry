/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


public class AmbryStorageQuotaServiceFactory implements StorageQuotaServiceFactory {
  private static final String STORAGE_QUOTA_SERVICE_PREFIX = "storage-quota-service";
  private final StorageQuotaService storageQuotaService;

  public AmbryStorageQuotaServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry)
      throws Exception {
    StorageQuotaConfig storageQuotaConfig = new StorageQuotaConfig(verifiableProperties);
    HelixPropertyStoreConfig helixPropertyStoreConfig = new HelixPropertyStoreConfig(verifiableProperties);

    HelixPropertyStore<ZNRecord> helixStore =
        CommonUtils.createHelixPropertyStore(storageQuotaConfig.zkClientConnectAddress, helixPropertyStoreConfig, null);
    ScheduledExecutorService scheduler = Utils.newScheduler(1, STORAGE_QUOTA_SERVICE_PREFIX, false);
    HelixStorageUsageRefresher storageUsageRefresher =
        new HelixStorageUsageRefresher(helixStore, scheduler, storageQuotaConfig);
    JSONStringStorageQuotaSource storageQuotaSource = new JSONStringStorageQuotaSource(storageQuotaConfig);
    AmbryStorageQuotaEnforcer storageQuotaEnforcer = new AmbryStorageQuotaEnforcer();
    storageQuotaService =
        new AmbryStorageQuotaService(storageUsageRefresher, storageQuotaSource, storageQuotaEnforcer, scheduler,
            storageQuotaConfig);
  }

  @Override
  public StorageQuotaService getStorageQuotaService() {
    return storageQuotaService;
  }
}
