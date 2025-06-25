/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.named;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;


public class MySqlNamedBlobDbIntergrationBase {
  private static final String LOCAL_DC = "dc1";
  // Please note that we are using mock time for time travel. Need to reset time (call setCurrentMilliseconds) when the
  // tests depends on time sequences
  protected static final MockTime time = new MockTime(System.currentTimeMillis());
  protected final InMemAccountService accountService;
  protected final PartitionId partitionId;
  protected final boolean enableHardDelete;
  protected final int listSqlOption;
  protected final MySqlNamedBlobDbConfig config;
  protected final MySqlNamedBlobDb namedBlobDb;
  protected final MySqlNamedBlobDbFactory namedBlobDbFactory;

  public MySqlNamedBlobDbIntergrationBase(boolean enableHardDelete, int listSqlOption,
      MySqlNamedBlobDbFactory namedBlobDbFactory) throws Exception {
    this.enableHardDelete = enableHardDelete;
    this.listSqlOption = listSqlOption;
    this.namedBlobDbFactory = namedBlobDbFactory;
    Properties properties = createProperties(listSqlOption);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    config = new MySqlNamedBlobDbConfig(verifiableProperties);
    accountService = new InMemAccountService(false, false);
    for (int i = 0; i < 5; i++) {
      accountService.createAndAddRandomAccount();
    }
    MockClusterMap clusterMap = new MockClusterMap();
    partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    namedBlobDbFactory =
        new MySqlNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), accountService, time, "");
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
  }

  @Before
  public void beforeTest() throws Exception {
    cleanup();
  }

  @After
  public void afterTest() throws Exception {
    if (namedBlobDb != null) {
      namedBlobDb.close();
    }
  }

  protected Properties createProperties(int listSqlOptions) throws Exception {
    Properties properties = Utils.loadPropsFromResource("mysql.properties");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, LOCAL_DC);
    if (enableHardDelete) {
      properties.setProperty(MySqlNamedBlobDbConfig.ENABLE_HARD_DELETE, Boolean.toString(true));
    }
    properties.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION, Integer.toString(listSqlOptions));
    return properties;
  }

  /**
   * Get a sample blob ID.
   * @param account the account of the blob.
   * @param container the container of the blob.
   * @return the base64 blob ID.
   */
  protected String getBlobId(Account account, Container container) {
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, account.getId(), container.getId(),
        partitionId, false, BlobId.BlobDataType.SIMPLE).getID();
  }

  /**
   * Empties the accounts and containers tables.
   * @throws SQLException throw any SQL related exception
   */
  private void cleanup() throws SQLException {
    for (DataSource dataSource : namedBlobDb.getDataSources().values()) {
      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.executeUpdate("DELETE FROM named_blobs_v2");
        }
      }
    }
  }
}
