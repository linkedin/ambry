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
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlUtils.*;


/**
 * Factory to create a {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreFactory {
  private static final Logger logger = LoggerFactory.getLogger(AccountStatsMySqlStoreFactory.class);
  private static final String REWRITE_BATCHED_STATEMENTS_LITERAL = "rewriteBatchedStatements";

  private final AccountStatsMySqlConfig accountStatsMySqlConfig;
  private final HostnameHelper hostnameHelper;
  private final String localDC;
  private final String clusterName;
  private final String hostname;
  private final MetricRegistry registry;
  private final String localBackupFilePath;

  /**
   * Constructor to create a {@link AccountStatsMySqlStoreFactory}.
   * @param verifiableProperties
   * @param clusterMapConfig
   * @param statsManagerConfig
   * @param registry
   */
  public AccountStatsMySqlStoreFactory(VerifiableProperties verifiableProperties, ClusterMapConfig clusterMapConfig,
      StatsManagerConfig statsManagerConfig, MetricRegistry registry) {
    accountStatsMySqlConfig = new AccountStatsMySqlConfig(verifiableProperties);
    clusterName = clusterMapConfig.clusterMapClusterName;
    hostnameHelper = new HostnameHelper(accountStatsMySqlConfig, clusterMapConfig.clusterMapPort);
    hostname = hostnameHelper.simplifyHostname(clusterMapConfig.clusterMapHostName);
    localDC = clusterMapConfig.clusterMapDatacenterName;
    localBackupFilePath = statsManagerConfig.outputFilePath;
    this.registry = registry;
  }

  /**
   * Return {@link AccountStatsMySqlStore}.
   * @return
   * @throws SQLException
   */
  public AccountStatsMySqlStore getAccountStatsMySqlStore() throws SQLException {
    Map<String, List<DbEndpoint>> dcToMySqlDBEndpoints = getDbEndpointsPerDC(accountStatsMySqlConfig.dbInfo);
    List<DbEndpoint> dbEndpoints = dcToMySqlDBEndpoints.get(localDC);
    if (dbEndpoints == null || dbEndpoints.size() == 0) {
      throw new IllegalArgumentException("Empty db endpoints for datacenter: " + localDC);
    }
    if (accountStatsMySqlConfig.enableRewriteBatchedStatement) {
      dbEndpoints = setRewriteBatchedStatements(dbEndpoints);
    }
    try {
      dbEndpoints.forEach(ep -> logger.info("DBUrl: {}", ep.getUrl()));
      return new AccountStatsMySqlStore(accountStatsMySqlConfig, dbEndpoints, localDC, clusterName, hostname,
          localBackupFilePath, hostnameHelper, registry);
    } catch (SQLException e) {
      logger.error("Account Stats MySQL store creation failed", e);
      throw e;
    }
  }

  /**
   * Set rewriteBatchedStatements for the db url if not set yet.
   * @param endpoints The list of {@link DbEndpoint}s.
   * @return The modified lit of {@link DbEndpoint}s.
   */
  private List<DbEndpoint> setRewriteBatchedStatements(List<DbEndpoint> endpoints) {
    List<DbEndpoint> result = new ArrayList<>();
    for (DbEndpoint ep : endpoints) {
      if (ep.getUrl().contains(REWRITE_BATCHED_STATEMENTS_LITERAL)) {
        result.add(ep);
      } else {
        String sep = ep.getUrl().contains("?") ? "&" : "?";
        String newUrl = new StringBuilder(ep.getUrl()).append(sep)
            .append(REWRITE_BATCHED_STATEMENTS_LITERAL)
            .append("=true")
            .toString();
        result.add(new DbEndpoint(newUrl, ep.getDatacenter(), ep.isWriteable(), ep.getUsername(), ep.getPassword()));
      }
    }
    return result;
  }
}
