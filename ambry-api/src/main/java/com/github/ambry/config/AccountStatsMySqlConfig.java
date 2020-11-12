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

  @Config(DOMAIN_NAMES_TO_REMOVE)
  @Default("")
  public final String domainNamesToRemove;

  public AccountStatsMySqlConfig(VerifiableProperties verifiableProperties) {
    dbInfo = verifiableProperties.getString(DB_INFO, "");
    domainNamesToRemove = verifiableProperties.getString(DOMAIN_NAMES_TO_REMOVE, "");
  }
}
