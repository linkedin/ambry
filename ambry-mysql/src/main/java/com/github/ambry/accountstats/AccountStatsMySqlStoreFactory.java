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
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlUtils.*;


/**
 * Factory to create a {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreFactory implements AccountStatsStoreFactory {
  private static final Logger logger = LoggerFactory.getLogger(AccountStatsMySqlStoreFactory.class);
  private static final String REWRITE_BATCHED_STATEMENTS_LITERAL = "rewriteBatchedStatements";

  private final AccountStatsMySqlConfig accountStatsMySqlConfig;
  private final HostnameHelper hostnameHelper;
  private final String localDC;
  private final String clusterName;
  private final String hostname;
  private final MetricRegistry registry;

  /**
   * Constructor to create a {@link AccountStatsMySqlStoreFactory}.
   * @param verifiableProperties
   * @param clusterMapConfig
   * @param registry
   */
  public AccountStatsMySqlStoreFactory(VerifiableProperties verifiableProperties, ClusterMapConfig clusterMapConfig,
      MetricRegistry registry) {
    accountStatsMySqlConfig = new AccountStatsMySqlConfig(verifiableProperties);
    clusterName = clusterMapConfig.clusterMapClusterName;
    hostnameHelper = new HostnameHelper(accountStatsMySqlConfig, clusterMapConfig.clusterMapPort);
    hostname = hostnameHelper.simplifyHostname(clusterMapConfig.clusterMapHostName);
    localDC = clusterMapConfig.clusterMapDatacenterName;
    this.registry = registry;
  }

  /**
   * Return {@link AccountStatsMySqlStore}.
   * @return
   * @throws SQLException
   */
  @Override
  public AccountStatsStore getAccountStatsStore() throws Exception {
    Map<String, List<DbEndpoint>> dcToMySqlDBEndpoints = getDbEndpointsPerDC(accountStatsMySqlConfig.dbInfo);
    List<DbEndpoint> dbEndpoints = dcToMySqlDBEndpoints.get(localDC);
    if (dbEndpoints == null || dbEndpoints.size() == 0) {
      throw new IllegalArgumentException("Empty db endpoints for datacenter: " + localDC);
    }
    dbEndpoints.forEach(ep -> logger.info("DBUrl: {}", ep.getUrl()));
    return new AccountStatsMySqlStore(accountStatsMySqlConfig, buildDataSource(dbEndpoints.get(0)), clusterName,
        hostname, hostnameHelper, registry);
  }

  private HikariDataSource buildDataSource(DbEndpoint dbEndpoint) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(dbEndpoint.getUrl());
    hikariConfig.setUsername(dbEndpoint.getUsername());
    hikariConfig.setPassword(dbEndpoint.getPassword());
    hikariConfig.setMaximumPoolSize(accountStatsMySqlConfig.poolSize);
    hikariConfig.setMinimumIdle(0);
    hikariConfig.setIdleTimeout(accountStatsMySqlConfig.connectionIdleTimeoutMs);
    // Recommended properties for automatic prepared statement caching
    // https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
    hikariConfig.addDataSourceProperty(REWRITE_BATCHED_STATEMENTS_LITERAL,
        String.valueOf(accountStatsMySqlConfig.enableRewriteBatchedStatement));
    hikariConfig.setMetricRegistry(registry);
    return new HikariDataSource(hikariConfig);
  }
}
