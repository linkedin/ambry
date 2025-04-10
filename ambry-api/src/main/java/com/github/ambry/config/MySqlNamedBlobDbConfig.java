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
 *
 */

package com.github.ambry.config;

import com.github.ambry.named.TransactionIsolationLevel;

import static com.github.ambry.rest.RestUtils.*;


public class MySqlNamedBlobDbConfig {
  private static final String PREFIX = "mysql.named.blob.";
  public static final String DB_INFO = PREFIX + "db.info";
  public static final String DB_TRANSITION = PREFIX + "db.transition";
  public static final String DB_RELY_ON_NEW_TABLE = PREFIX + "db.rely.on.new.table";
  public static final String LOCAL_POOL_SIZE = PREFIX + "local.pool.size";
  public static final String REMOTE_POOL_SIZE = PREFIX + "remote.pool.size";
  public static final String LIST_MAX_RESULTS = PREFIX + "list.max.results";
  public static final String QUERY_STALE_DATA_MAX_RESULTS = PREFIX + "query.stale.data.max.results";
  public static final String STALE_DATA_RETENTION_DAYS = PREFIX + "stale.data.retention.days";
  public static final String TRANSACTION_ISOLATION_LEVEL = PREFIX + "transaction.isolation.level";
  public static final String LIST_NAMED_BLOBS_SQL_OPTION = "list.named.blobs.sql.option";
  public static final String ENABLE_HARD_DELETE = PREFIX + "enable.hard.delete";

  /**
   * Option to pick the SQL query to use for listing named blobs.
   * Check getListNamedBlobsSQL() for more details.
   */
  @Config(LIST_NAMED_BLOBS_SQL_OPTION)
  public static final int DEFAULT_LIST_NAMED_BLOBS_SQL_OPTION = 2;
  public static final int MIN_LIST_NAMED_BLOBS_SQL_OPTION = 1;
  public static final int MAX_LIST_NAMED_BLOBS_SQL_OPTION = 2;
  public final int listNamedBlobsSQLOption;

  /**
   * Serialized json array containing the information about all mysql end points.
   * See {@link MySqlAccountServiceConfig#dbInfo} for information about the format of the json array.
   */
  @Config(DB_INFO)
  public final String dbInfo;

  /**
   * Number of connections and threads to use for executing transactions against databases in the local datacenter.
   */
  @Config(LOCAL_POOL_SIZE)
  @Default("5")
  public final int localPoolSize;

  /**
   * Number of connections and threads to use for executing transactions against databases in a remote datacenter.
   */
  @Config(REMOTE_POOL_SIZE)
  @Default("1")
  public final int remotePoolSize;

  /**
   * The maximum number of entries to return per response page when listing blobs.
   */
  @Config(LIST_MAX_RESULTS)
  @Default("1000")
  public final int listMaxResults;

  /**
   * The maximum number of entries to return per each stale blobs pull request.
   */
  @Config(QUERY_STALE_DATA_MAX_RESULTS)
  @Default("1000")
  public final int queryStaleDataMaxResults;

  /**
   * The maximum number of days for a stale blob to say uncleaned.
   */
  @Config(STALE_DATA_RETENTION_DAYS)
  @Default("20")
  public final int staleDataRetentionDays;

  /**
   * Transaction isolation level to be set on DB Connection. When nothing is set, default MySQL DB transaction level
   * (REPEATABLE_READ) will take effect.
   */
  @Config(TRANSACTION_ISOLATION_LEVEL)
  public final TransactionIsolationLevel transactionIsolationLevel;

  @Config(ENABLE_HARD_DELETE)
  public final boolean enableHardDelete;

  public MySqlNamedBlobDbConfig(VerifiableProperties verifiableProperties) {
    this.listNamedBlobsSQLOption =
        verifiableProperties.getIntInRange(LIST_NAMED_BLOBS_SQL_OPTION, DEFAULT_LIST_NAMED_BLOBS_SQL_OPTION,
            MIN_LIST_NAMED_BLOBS_SQL_OPTION, MAX_LIST_NAMED_BLOBS_SQL_OPTION);
    this.dbInfo = verifiableProperties.getString(DB_INFO);
    this.localPoolSize = verifiableProperties.getIntInRange(LOCAL_POOL_SIZE, 5, 1, Integer.MAX_VALUE);
    this.remotePoolSize = verifiableProperties.getIntInRange(REMOTE_POOL_SIZE, 1, 1, Integer.MAX_VALUE);
    this.listMaxResults =
        verifiableProperties.getIntInRange(LIST_MAX_RESULTS, DEFAULT_MAX_KEY_VALUE, 1, Integer.MAX_VALUE);
    this.queryStaleDataMaxResults =
        verifiableProperties.getIntInRange(QUERY_STALE_DATA_MAX_RESULTS, 1000, 1, Integer.MAX_VALUE);
    this.staleDataRetentionDays =
        verifiableProperties.getIntInRange(STALE_DATA_RETENTION_DAYS, 20, 1, Integer.MAX_VALUE);
    this.transactionIsolationLevel =
        verifiableProperties.getEnum(TRANSACTION_ISOLATION_LEVEL, TransactionIsolationLevel.class,
            TransactionIsolationLevel.TRANSACTION_NONE);
    this.enableHardDelete = verifiableProperties.getBoolean(ENABLE_HARD_DELETE, false);
  }
}
