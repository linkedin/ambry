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


public class MySqlNamedBlobDbConfig {
  private static final String PREFIX = "mysql.named.blob.";
  public static final String DB_INFO = PREFIX + "db.info";
  public static final String DB_TRANSITION = PREFIX + "db.transition";
  public static final String DB_RELY_ON_NEW_TABLE = PREFIX + "db.rely.on.new.table";
  public static final String LOCAL_POOL_SIZE = PREFIX + "local.pool.size";
  public static final String REMOTE_POOL_SIZE = PREFIX + "remote.pool.size";
  public static final String  LIST_MAX_RESULTS = PREFIX + "list.max.results";
  public static final String  QUERY_STALE_DATA_MAX_RESULTS = PREFIX + "query.stale.data.max.results";
  public static final String  STALE_DATA_RETENTION_DAYS = PREFIX + "stale.data.retention.days";
  public static final String TRANSACTION_ISOLATION_LEVEL = PREFIX + "transaction.isolation.level";
  public static final String LIST_NAMED_BLOBS_SQL = "list.named.blobs.sql";

  /**
   * List named-blobs query.
   * The inner query will select blobs based on the filters, and then group them on blob_name.
   * In each group, it will then select the one with the highest version.
   * The outer query will then filter out blobs that have been deleted, and return the latest blob in each group.
   */
  @Config(LIST_NAMED_BLOBS_SQL)
  public final String DEFAULT_LIST_NAMED_BLOBS_SQL = ""
      + "WITH LatestBlob AS ( "
      + "  SELECT blob_name, blob_id, MAX(version) AS version, deleted_ts, blob_size, modified_ts "
      + "  FROM named_blobs_v2 "
      + "  WHERE account_id = ? "
      + "    AND container_id = ? "
      + "    AND blob_state = %1$s "
      + "    AND blob_name LIKE ? "
      + "    AND blob_name >= ? "
      + "  GROUP BY blob_name "
      + ") "
      + "SELECT * "
      + "FROM LatestBlob "
      + "WHERE (deleted_ts IS NULL OR deleted_ts > %2$S) "
      + "ORDER BY blob_name ASC "
      + "LIMIT ?; ";
  public final String listNamedBlobsSQL;


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
   * A flag on whether to turn on DB Transition run
   */
  @Config(DB_TRANSITION)
  public final boolean dbTransition;

  /**
   * A flag on whether to use new table data
   */
  @Config(DB_RELY_ON_NEW_TABLE)
  @Default("false")
  public final boolean dbRelyOnNewTable;

  /**
   * Transaction isolation level to be set on DB Connection. When nothing is set, default MySQL DB transaction level
   * (REPEATABLE_READ) will take effect.
   */
  @Config(TRANSACTION_ISOLATION_LEVEL)
  public final TransactionIsolationLevel transactionIsolationLevel;

  public MySqlNamedBlobDbConfig(VerifiableProperties verifiableProperties) {
    this.listNamedBlobsSQL = verifiableProperties.getString(LIST_NAMED_BLOBS_SQL, DEFAULT_LIST_NAMED_BLOBS_SQL);
    this.dbInfo = verifiableProperties.getString(DB_INFO);
    this.localPoolSize = verifiableProperties.getIntInRange(LOCAL_POOL_SIZE, 5, 1, Integer.MAX_VALUE);
    this.remotePoolSize = verifiableProperties.getIntInRange(REMOTE_POOL_SIZE, 1, 1, Integer.MAX_VALUE);
    this.listMaxResults = verifiableProperties.getIntInRange(LIST_MAX_RESULTS, 100, 1, Integer.MAX_VALUE);
    this.queryStaleDataMaxResults = verifiableProperties.getIntInRange(QUERY_STALE_DATA_MAX_RESULTS, 1000, 1, Integer.MAX_VALUE);
    this.staleDataRetentionDays = verifiableProperties.getIntInRange(STALE_DATA_RETENTION_DAYS, 20, 1, Integer.MAX_VALUE);
    this.dbTransition = verifiableProperties.getBoolean(DB_TRANSITION, false);
    this.dbRelyOnNewTable = verifiableProperties.getBoolean(DB_RELY_ON_NEW_TABLE, false);
    this.transactionIsolationLevel =
        verifiableProperties.getEnum(TRANSACTION_ISOLATION_LEVEL, TransactionIsolationLevel.class,
            TransactionIsolationLevel.TRANSACTION_NONE);
  }
}
