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

package com.github.ambry.named;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils.DbEndpoint;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


public class MySqlNamedBlobDbFactory implements NamedBlobDbFactory {
  private final MySqlNamedBlobDbConfig config;
  private final String localDatacenter;
  private final MetricRegistry metricRegistry;
  private final AccountService accountService;
  private final Time time;

  public MySqlNamedBlobDbFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      AccountService accountService, Time time) {
    config = new MySqlNamedBlobDbConfig(verifiableProperties);
    localDatacenter = verifiableProperties.getString(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME);
    this.metricRegistry = metricRegistry;
    this.accountService = accountService;
    this.time = time;
  }

  public MySqlNamedBlobDbFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      AccountService accountService) {
    this(verifiableProperties, metricRegistry, accountService, SystemTime.getInstance());
  }

  @Override
  public MySqlNamedBlobDb getNamedBlobDb() {
    return new MySqlNamedBlobDb(accountService, config, this::buildDataSource, localDatacenter, metricRegistry,
        this.time);
  }

  /**
   * @param dbEndpoint struct containing JDBC connection information.
   * @return the {@link HikariDataSource} for the {@link DbEndpoint}.
   */
  public HikariDataSource buildDataSource(DbEndpoint dbEndpoint) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(dbEndpoint.getUrl());
    hikariConfig.setUsername(dbEndpoint.getUsername());
    hikariConfig.setPassword(dbEndpoint.getPassword());
    hikariConfig.setMaximumPoolSize(
        dbEndpoint.getDatacenter().equals(localDatacenter) ? config.localPoolSize : config.remotePoolSize);
    // Recommended properties for automatic prepared statement caching
    // https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
    hikariConfig.setMetricRegistry(metricRegistry);
    return new HikariDataSource(hikariConfig);
  }
}
