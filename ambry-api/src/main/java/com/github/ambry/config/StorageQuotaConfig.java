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
  public static final String REFRESHER_POLLING_INTERVAL_MS = STORAGE_QUOTA_PREFIX + "refresher.polling.interval.ms";
  public static final String STORAGE_QUOTA_IN_JSON = STORAGE_QUOTA_PREFIX + "storage.quota.in.json";
  public static final String BACKUP_FILE_DIR = STORAGE_QUOTA_PREFIX + "backup.file.dir";
  public static final String MYSQL_MONTHLY_BASE_FETCH_OFFSET_SEC =
      STORAGE_QUOTA_PREFIX + "mysql.monthly.base.fetch.offset.sec";
  public static final String MYSQL_STORE_RETRY_BACKOFF_MS = STORAGE_QUOTA_PREFIX + "mysql.store.retry.backoff.ms";
  public static final String MYSQL_STORE_RETRY_MAX_COUNT = STORAGE_QUOTA_PREFIX + "mysql.store.retry.max.count";
  public static final String SHOULD_THROTTLE = STORAGE_QUOTA_PREFIX + "should.throttle";
  public static final String USE_PHYSICAL_STORAGE = STORAGE_QUOTA_PREFIX + "use.physical.storage";
  public static final String SHOULD_REJECT_REQUEST_WITHOUT_QUOTA =
      STORAGE_QUOTA_PREFIX + "should.reject.request.without.quota";

  /**
   * The interval in milliseconds for refresher to refresh storage usage from its source.
   */
  @Config(REFRESHER_POLLING_INTERVAL_MS)
  @Default("30 * 60 * 1000") // 30 minutes
  public final int refresherPollingIntervalMs;

  //////////////// Config for JSONStringStorageQuotaSource ///////////////

  /**
   * A JSON string representing storage quota for all accounts and containers. eg:
   * {
   *   "101": {
   *     "1": 1024000000,
   *     "2": 258438456
   *   },
   *   "102": {
   *     "1": 10737418240
   *   },
   *   "103": 10737418240
   * }
   * The key of the top object is the account id and the key of the inner object is the container id.
   * If there is no inner object, then the value is the storage quota in bytes for this account.
   * The value of the each container id is the storage quota in bytes for this container.
   *
   * If the targeted account/container doesn't have a storage quota in this JSON string, it's up to
   * StorageQuotaEnforcer to decide whether to allow uploads or not.
   */
  @Config(STORAGE_QUOTA_IN_JSON)
  @Default("")
  public final String storageQuotaInJson;

  /**
   * The directory to store quota related backup files. If empty, then backup files will be disabled.
   */
  @Config(BACKUP_FILE_DIR)
  @Default("")
  public final String backupFileDir;

  /**
   * Duration in milliseconds to backoff if the mysql database query failed.
   */
  @Config(MYSQL_STORE_RETRY_BACKOFF_MS)
  @Default("10*60*1000")
  public final long mysqlStoreRetryBackoffMs;

  /**
   * Maximum retry times to execute a mysql database query.
   */
  @Config(MYSQL_STORE_RETRY_MAX_COUNT)
  @Default("1")
  public final int mysqlStoreRetryMaxCount;

  /**
   * Offset in seconds to fetch container usage monthly base.
   */
  @Config(MYSQL_MONTHLY_BASE_FETCH_OFFSET_SEC)
  @Default("60 * 60")
  public final long mysqlMonthlyBaseFetchOffsetSec;

  /**
   * True to enable throttle for storage quota enforcer.
   */
  @Config(SHOULD_THROTTLE)
  @Default("true")
  public final boolean shouldThrottle;

  /**
   * True to use physical storage instead of logical storage usage to enforce storage quota.
   */
  @Config(USE_PHYSICAL_STORAGE)
  @Default("false")
  public final boolean usePhysicalStorage;

  /**
   * True to reject request when there is no storage quota created for this targeted account or container.
   */
  @Config(SHOULD_REJECT_REQUEST_WITHOUT_QUOTA)
  @Default("false")
  public final boolean shouldRejectRequestWithoutQuota;

  /**
   * Constructor to create a {@link StorageQuotaConfig}.
   * @param verifiableProperties The {@link VerifiableProperties} that contains all the properties.
   */
  public StorageQuotaConfig(VerifiableProperties verifiableProperties) {
    refresherPollingIntervalMs =
        verifiableProperties.getIntInRange(REFRESHER_POLLING_INTERVAL_MS, 30 * 60 * 1000, 0, Integer.MAX_VALUE);
    storageQuotaInJson = verifiableProperties.getString(STORAGE_QUOTA_IN_JSON, "");
    backupFileDir = verifiableProperties.getString(BACKUP_FILE_DIR, "");
    mysqlStoreRetryBackoffMs = verifiableProperties.getLong(MYSQL_STORE_RETRY_BACKOFF_MS, 10 * 60 * 1000);
    mysqlStoreRetryMaxCount = verifiableProperties.getInt(MYSQL_STORE_RETRY_MAX_COUNT, 1);
    mysqlMonthlyBaseFetchOffsetSec = verifiableProperties.getLong(MYSQL_MONTHLY_BASE_FETCH_OFFSET_SEC, 60 * 60);
    shouldThrottle = verifiableProperties.getBoolean(SHOULD_THROTTLE, true);
    usePhysicalStorage = verifiableProperties.getBoolean(USE_PHYSICAL_STORAGE, false);
    shouldRejectRequestWithoutQuota = verifiableProperties.getBoolean(SHOULD_REJECT_REQUEST_WITHOUT_QUOTA, false);
  }
}
