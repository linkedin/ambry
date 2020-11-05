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
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.mysql.ContainerDao;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.account.mysql.MySqlDataAccessor;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseTest {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseTest.class);
  private static String PROPSFILE = "propsFile";

  public static void main(String args[]) {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> propsFileOpt =
        parser.accepts(PROPSFILE, "Properties file path").withRequiredArg().describedAs(PROPSFILE).ofType(String.class);
    OptionSet optionSet = parser.parse(args);
    String propsFilePath = optionSet.valueOf(propsFileOpt);
    if (propsFilePath == null) {
      System.err.println("Missing argument: " + PROPSFILE);
      System.exit(1);
    }
    try {
      Properties properties = Utils.loadProps(propsFilePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      perfTest(verifiableProperties);
    } catch (Exception e) {
      logger.error("Perf test failed", e);
    }
  }

  private static void perfTest(VerifiableProperties verifiableProperties) throws Exception {
    MySqlDataAccessor dataAccessor =
        new MySqlAccountStoreFactory(verifiableProperties, new MetricRegistry()).getMySqlAccountStore()
            .getMySqlDataAccessor();
    ContainerDao containerDao = new ContainerDao(dataAccessor);
    // Use high account id to avoid conflict
    short startAccountId = 30000;
    int numAccounts = 10;
    int numContainers = 1000;

    cleanup(dataAccessor.getDatabaseConnection(true), startAccountId);

    ContainerBuilder builder = new ContainerBuilder((short) 0, "", Container.ContainerStatus.ACTIVE, "Test", (short) 0);
    long t0 = System.currentTimeMillis();
    int containersAdded = 0;
    for (short accountId = startAccountId; accountId < startAccountId + numAccounts; accountId++) {
      for (short containerId = 1; containerId <= numContainers; containerId++) {
        builder.setId(containerId).setName("Container-" + containerId).setTtlRequired(true);
        containerDao.addContainer(accountId, builder.build());
        containersAdded++;
      }
    }
    long t1 = System.currentTimeMillis();
    long insertTime = t1 - t0;
    logger.info("Added {} containers in {} ms", containersAdded, insertTime);

    // Query containers since t0 (should be all)
    List<Container> allContainers = containerDao.getNewContainers(t0);
    long t2 = System.currentTimeMillis();
    logger.info("Queried {} containers in {} ms", allContainers.size(), t2 - t1);
    // Query containers since t2 (should be none)
    allContainers = containerDao.getNewContainers(t2);
    long t3 = System.currentTimeMillis();
    logger.info("Queried {} containers in {} ms", allContainers.size(), t3 - t2);
  }

  private static void cleanup(Connection dbConnection, short startAccountId) throws SQLException {
    Statement statement = dbConnection.createStatement();
    int numDeleted = statement.executeUpdate(
        "delete from " + ContainerDao.CONTAINER_TABLE + " where " + ContainerDao.ACCOUNT_ID + " >= " + startAccountId);
    logger.info("Deleted {} containers", numDeleted);
  }
}
