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
package com.github.ambry.server.mysql;

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


public class AccountStatsMySqlStoreFactory {
  private static final Logger logger = LoggerFactory.getLogger(AccountStatsMySqlStoreFactory.class);

  private final AccountStatsMySqlConfig accountStatsMySqlConfig;
  private final String localDC;
  private final String clustername;
  private final String hostname;
  private final MetricRegistry registry;
  private final String localBackupFilePath;

  public AccountStatsMySqlStoreFactory(VerifiableProperties verifiableProperties, ClusterMapConfig clusterMapConfig,
      StatsManagerConfig statsManagerConfig, MetricRegistry registry) {
    accountStatsMySqlConfig = new AccountStatsMySqlConfig(verifiableProperties);
    clustername = clusterMapConfig.clusterMapClusterName;
    String[] domainNamesToRemove = accountStatsMySqlConfig.domainNamesToRemove.split(",");

    String fullyQualifiedDomainName = clusterMapConfig.clusterMapHostName;
    int port = clusterMapConfig.clusterMapPort;
    for (String domainName : domainNamesToRemove) {
      if (domainName.charAt(0) != '.') {
        domainName = "." + domainName;
      }
      fullyQualifiedDomainName = fullyQualifiedDomainName.replace(domainName, "");
    }
    hostname = String.format("%s_%d", fullyQualifiedDomainName, port);
    localDC = clusterMapConfig.clusterMapDatacenterName;
    localBackupFilePath = statsManagerConfig.outputFilePath;
    this.registry = registry;
  }

  public AccountStatsMySqlStore getAccountStatsMySqlStore() throws SQLException {
    Map<String, List<DbEndpoint>> dcToMySqlDBEndpoints = getDbEndpointsPerDC(accountStatsMySqlConfig.dbInfo);
    // Flatten to List (TODO: does utility method need to return map?)
    List<DbEndpoint> dbEndpoints = new ArrayList<>();
    dcToMySqlDBEndpoints.values().forEach(endpointList -> dbEndpoints.addAll(endpointList));
    try {
      return new AccountStatsMySqlStore(dbEndpoints, localDC, clustername, hostname, localBackupFilePath, registry);
    } catch (SQLException e) {
      logger.error("Account Stats MySQL store creation failed", e);
      throw e;
    }
  }
}
