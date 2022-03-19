/**
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

public class AccountStatsMySqlConfig {
  private static final String PREFIX = "account.stats.mysql.";

  public static final String DB_INFO = PREFIX + "db.info";
  public static final String DOMAIN_NAMES_TO_REMOVE = PREFIX + "domain.names.to.remove";
  public static final String POOL_SIZE = PREFIX + "pool.size";
  public static final String UPDATE_BATCH_SIZE = PREFIX + "update.batch.size";
  public static final String ENABLE_REWRITE_BATCHED_STATEMENT = PREFIX + "enable.rewrite.batched.statements";
  public static final String CONNECTION_IDLE_TIMEOUT = PREFIX + "connection.idle.timeout.ms";
  public static final String LOCAL_BACKUP_FILE_PATH = PREFIX + "local.backup.file.path";

  /**
   * Serialized json containing the information about all mysql end points. This information should be of the following form:
   * <pre>
   *   [
   *     {
   *       "url":"mysql-host1"
   *       "datacenter":"dc1",
   *       "isWriteable": "true",
   *       "username":"root",
   * 	     "password":"password"
   *     },
   *     {
   *       "url":"mysql-host2"
   *       "datacenter":"dc2",
   *       "isWriteable": "false",
   *       "username":"root",
   * 	     "password":"password"
   *     },
   *     {
   *       "url":"mysql-host3"
   *       "datacenter":"dc3",
   *       "isWriteable": "false",
   *       "username":"root",
   * 	     "password":"password"
   *     }
   *   ]
   * </pre>
   */
  @Config(DB_INFO)
  @Default("")
  public final String dbInfo;

  /**
   * Domain names to remove from hostname. This is a list of domains names to remove. For example, if your hostname
   * is app1.github.com and you only want to use app1 as the hostname, you can include ".github.com" in this list.
   */
  @Config(DOMAIN_NAMES_TO_REMOVE)
  @Default("")
  public final String domainNamesToRemove;

  /**
   * Number of connections and threads to use for executing transactions.
   */
  @Config(POOL_SIZE)
  @Default("2")
  public final int poolSize;

  /**
   * Negative numbers means disable batch. 0 means unlimited batch size. Any positive number means the size of each batch.
   */
  @Config(UPDATE_BATCH_SIZE)
  @Default("0")
  public final int updateBatchSize;

  /**
   * This configuration is specific for mysql database to better support batch operations. Enable this would give a better
   * batch performance for mysql database.
   */
  @Config(ENABLE_REWRITE_BATCHED_STATEMENT)
  @Default("false")
  public final boolean enableRewriteBatchedStatement;

  /**
   * Connection idle timeout in ms. Once a connection is idle for more than the timeout, it will be closed by the pool.
   */
  @Config(CONNECTION_IDLE_TIMEOUT)
  @Default("60 * 1000")
  public final long connectionIdleTimeoutMs;

  /**
   * Backup file path for each host to save host account storage stats.
   */
  @Config(LOCAL_BACKUP_FILE_PATH)
  @Default("")
  public final String localBackupFilePath;

  public AccountStatsMySqlConfig(VerifiableProperties verifiableProperties) {
    dbInfo = verifiableProperties.getString(DB_INFO, "");
    domainNamesToRemove = verifiableProperties.getString(DOMAIN_NAMES_TO_REMOVE, "");
    poolSize = verifiableProperties.getIntInRange(POOL_SIZE, 2, 1, Integer.MAX_VALUE);
    updateBatchSize = verifiableProperties.getInt(UPDATE_BATCH_SIZE, 0);
    enableRewriteBatchedStatement = verifiableProperties.getBoolean(ENABLE_REWRITE_BATCHED_STATEMENT, false);
    connectionIdleTimeoutMs = verifiableProperties.getLong(CONNECTION_IDLE_TIMEOUT, 60 * 1000);
    localBackupFilePath = verifiableProperties.getString(LOCAL_BACKUP_FILE_PATH, "");
  }
}
