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
  public static final String POOL_SIZE = PREFIX + "pool.size";
  public static final String  LIST_MAX_RESULTS = PREFIX + "list.max.results";

  /**
   * Serialized json array containing the information about all mysql end points.
   * See {@link MySqlAccountServiceConfig#dbInfo} for information about the format of the json array.
   */
  @Config(DB_INFO)
  public final String dbInfo;

  /**
   * Number of connections and threads to use for executing transactions.
   */
  @Config(POOL_SIZE)
  @Default("10")
  public final int poolSize;

  /**
   * The maximum number of entries to return per response page when listing blobs.
   */
  @Config(LIST_MAX_RESULTS)
  @Default("100")
  public final int listMaxResults;

  public MySqlNamedBlobDbConfig(VerifiableProperties verifiableProperties) {
    this.dbInfo = verifiableProperties.getString(DB_INFO);
    this.poolSize = verifiableProperties.getIntInRange(POOL_SIZE, 10, 1, Integer.MAX_VALUE);
    this.listMaxResults = verifiableProperties.getIntInRange(LIST_MAX_RESULTS, 100, 1, Integer.MAX_VALUE);
  }
}
