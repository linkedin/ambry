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

public class MySqlNamedBlobDbConfig {
  private static final String PREFIX = "mysql.named.blob.";
  public static final String DB_INFO = PREFIX + "db.info";
  public static final String DB_TRANSITION = PREFIX + "db.transition";
  public static final String DB_RELY_ON_NEW_TABLE = PREFIX + "db.rely.on.new.table";
  public static final String LOCAL_POOL_SIZE = PREFIX + "local.pool.size";
  public static final String REMOTE_POOL_SIZE = PREFIX + "remote.pool.size";
  public static final String  LIST_MAX_RESULTS = PREFIX + "list.max.results";

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
  @Default("100")
  public final int listMaxResults;

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

  public MySqlNamedBlobDbConfig(VerifiableProperties verifiableProperties) {
    this.dbInfo = verifiableProperties.getString(DB_INFO);
    this.localPoolSize = verifiableProperties.getIntInRange(LOCAL_POOL_SIZE, 5, 1, Integer.MAX_VALUE);
    this.remotePoolSize = verifiableProperties.getIntInRange(REMOTE_POOL_SIZE, 1, 1, Integer.MAX_VALUE);
    this.listMaxResults = verifiableProperties.getIntInRange(LIST_MAX_RESULTS, 100, 1, Integer.MAX_VALUE);
    this.dbTransition = verifiableProperties.getBoolean(DB_TRANSITION, false);
    this.dbRelyOnNewTable = verifiableProperties.getBoolean(DB_RELY_ON_NEW_TABLE, false);
  }
}
