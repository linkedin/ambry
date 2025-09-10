/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.repair;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.MysqlRepairRequestsDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.utils.Time;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use Mysql implement the RepairRequestsDbFactory
 */
public class MysqlRepairRequestsDbFactory implements RepairRequestsDbFactory {
  private static final Logger logger = LoggerFactory.getLogger(MysqlRepairRequestsDb.class);

  private final MysqlRepairRequestsDbConfig config;
  private final MetricRegistry metrics;
  private final DataSource dataSource;
  private final Time time;

  public MysqlRepairRequestsDbFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      String localDatacenter, Time time) throws Exception {
    this.config = new MysqlRepairRequestsDbConfig(verifiableProperties);
    this.metrics = metricRegistry;
    this.time = time;

    // Get the db endpoint for this local data center.
    if (config.dbInfo == null || config.dbInfo.isEmpty()) {
      logger.warn("RepairRequestsDB: didn't specify the db information.");
      throw new SQLException("RepairRequestsDB: didn't specify the db information.");
    }
    List<MySqlUtils.DbEndpoint> dbs = MySqlUtils.getDbEndpointsPerDC(config.dbInfo)
        .values()
        .stream()
        .flatMap(List::stream)
        .filter(MySqlUtils.DbEndpoint::isWriteable)
        .filter(endpoint -> localDatacenter.equals(endpoint.getDatacenter()))
        .collect(Collectors.toList());
    if (dbs == null || dbs.size() == 0) {
      logger.warn("RepairRequestsDB: doesn't have the valid db. " + config.dbInfo);
      throw new SQLException("RepairRequestsDB: doesn't have the valid db. " + config.dbInfo);
    }
    MySqlUtils.DbEndpoint dbEndpoint = dbs.get(0);
    dataSource = buildDataSource(dbEndpoint);
  }

  @Override
  public MysqlRepairRequestsDb getRepairRequestsDb() {
    return new MysqlRepairRequestsDb(dataSource, config, metrics, time);
  }

  /**
   * @param dbEndpoint struct containing JDBC connection information.
   * @return the {@link HikariDataSource} for the {@link MySqlUtils.DbEndpoint}.
   */
  public HikariDataSource buildDataSource(MySqlUtils.DbEndpoint dbEndpoint) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(dbEndpoint.getUrlWithSSL(config.sslConfig));
    hikariConfig.setUsername(dbEndpoint.getUsername());
    hikariConfig.setPassword(dbEndpoint.getPassword());
    hikariConfig.setMaximumPoolSize(config.localPoolSize);
    // Recommended properties for automatic prepared statement caching
    // https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
    hikariConfig.setMetricRegistry(metrics);
    return new HikariDataSource(hikariConfig);
  }
}
