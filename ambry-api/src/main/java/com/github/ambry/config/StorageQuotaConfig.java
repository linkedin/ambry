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
package com.github.ambry.config;

/**
 * Config for Storage Quota service.
 */
public class StorageQuotaConfig {
  public static final String STORAGE_QUOTA_PREFIX = "storage.quota.";
  public static final String HELIX_PROPERTY_ROOT_PATH = STORAGE_QUOTA_PREFIX + "helix.property.root.path";
  public static final String ZK_CLIENT_CONNECT_ADDRESS = STORAGE_QUOTA_PREFIX + "zk.client.connect.address";
  public static final String REFRESHER_POLLING_INTERVAL_MS = STORAGE_QUOTA_PREFIX + "refresher.polling.interval.ms";
  public static final String CONTAINER_STORAGE_QUOTA_IN_JSON = STORAGE_QUOTA_PREFIX + "container.storage.quota.in.json";
  public static final String SOURCE_POLLING_INTERVAL_MS = STORAGE_QUOTA_PREFIX + "source.polling.interval.ms";

  //////////////// Config for HelixStorageUsageRefresher ///////////////

  /**
   * The root path of helix property store in ZooKeeper for HelixStorageUsageRefresher. Must start with {@code /}, and
   * must not end with {@code /}. The root path should be {@code /{clustername}/PROPERTYSTORE}
   */
  @Config(HELIX_PROPERTY_ROOT_PATH)
  public final String helixPropertyRootPath;

  /**
   * The ZooKeeper server address to connect to. This config is required.
   */
  @Config(ZK_CLIENT_CONNECT_ADDRESS)
  public final String zkClientConnectAddress;

  /**
   * The interval in milliseconds for refresher to refresh storage usage from its source.
   */
  @Config(REFRESHER_POLLING_INTERVAL_MS)
  @Default("30 * 60 * 1000") // 30 minutes
  public final int refresherPollingIntervalMs;

  //////////////// Config for JSONStringStorageQuotaSource ///////////////

  /**
   * A JSON string representing storage quota for all containers. eg:
   * {
   *   "101": {
   *     "1": 1024000000,
   *     "2": 258438456
   *   },
   *   "102": {
   *     "1": 10737418240
   *   }
   * }
   * The key of the top object is the acount id and the key of the inner object is the container id.
   * The value of the each container id is the storage quota in bytes for this container.
   *
   * If the targeted container doesn't have a storage quota in this JSON string, it's up to StorageQuotaEnforcer
   * to decide whether to allow uploads or not.
   */
  @Config(CONTAINER_STORAGE_QUOTA_IN_JSON)
  @Default("")
  public final String containerStorageQuotaInJson;

  /**
   * The interval in milliseconds for quota source to refresh each container's storage quota.
   */
  @Config(SOURCE_POLLING_INTERVAL_MS)
  @Default("30 * 60 * 1000")
  public final int sourcePollingIntervalMs;

  /**
   * Constructor to create a {@link StorageQuotaConfig}.
   * @param verifiableProperties The {@link VerifiableProperties} that contains all the properties.
   */
  public StorageQuotaConfig(VerifiableProperties verifiableProperties) {
    helixPropertyRootPath = verifiableProperties.getString(HELIX_PROPERTY_ROOT_PATH);
    zkClientConnectAddress = verifiableProperties.getString(ZK_CLIENT_CONNECT_ADDRESS);
    refresherPollingIntervalMs =
        verifiableProperties.getIntInRange(REFRESHER_POLLING_INTERVAL_MS, 30 * 60 * 1000, 0, Integer.MAX_VALUE);
    containerStorageQuotaInJson = verifiableProperties.getString(CONTAINER_STORAGE_QUOTA_IN_JSON, "");
    sourcePollingIntervalMs =
        verifiableProperties.getIntInRange(SOURCE_POLLING_INTERVAL_MS, 30 * 60 * 1000, 0, Integer.MAX_VALUE);
  }
}
