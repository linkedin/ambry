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
 */

package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.mysql.AccountDao;
import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Test;

import static com.github.ambry.config.MySqlAccountServiceConfig.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class MySqlAccountServiceMigrationIntegrationTest {
  private final MySqlAccountStoreFactory mockMySqlAccountStoreFactory;
  private final Properties mySqlConfigProps;
  private final MockNotifier<String> mockNotifier = new MockNotifier();
  private MySqlAccountStore mySqlAccountStore;
  private MySqlAccountStore mySqlAccountStoreNew;
  private MySqlAccountServiceConfig accountServiceConfig;
  private MySqlAccountService mySqlAccountService;

  public MySqlAccountServiceMigrationIntegrationTest() throws Exception {
    mySqlConfigProps = Utils.loadPropsFromResource("mysql.properties");
    mySqlConfigProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlConfigProps.setProperty(UPDATE_DISABLED, "false");
    mySqlConfigProps.setProperty(ENABLE_NEW_DATABASE_FOR_MIGRATION, "true");
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    mySqlAccountStore = spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
        new MetricRegistry()).getMySqlAccountStore());
    mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(mySqlAccountStore);
    mySqlAccountStoreNew = spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
        new MetricRegistry()).getMySqlAccountStoreNew());
    when(mockMySqlAccountStoreFactory.getMySqlAccountStoreNew()).thenReturn(mySqlAccountStoreNew);
    // Start with empty database
    cleanup();
    mySqlAccountService = getAccountService();
  }

  private MySqlAccountService getAccountService() throws Exception {
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    AccountServiceMetricsWrapper accountServiceMetrics = new AccountServiceMetricsWrapper(new MetricRegistry());
    // Don't initialize account store here as it may have preinitialized data
    return new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory,
        mockNotifier);
  }

  @Test
  public void testUpdateAccounts() throws Exception {
    // add accounts in old db.
    Container testContainer1 =
        new ContainerBuilder((short) 1, "testContainer1", Container.ContainerStatus.ACTIVE, "testContainer1",
            (short) 1).build();
    Container testContainer2 =
        new ContainerBuilder((short) 2, "testContainer2", Container.ContainerStatus.ACTIVE, "testContainer2",
            (short) 2).build();
    Container testContainer3 =
        new ContainerBuilder((short) 3, "testContainer3", Container.ContainerStatus.ACTIVE, "testContainer3",
            (short) 2).build();
    List<Container> containers = new ArrayList<>();
    containers.add(testContainer2);
    containers.add(testContainer3);

    Account testAccount1 = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer1)).build();
    Account testAccount2 =
        new AccountBuilder((short) 2, "testAccount2", Account.AccountStatus.ACTIVE).containers(containers).build();

    List<Account> accounts = new ArrayList<>();
    accounts.add(testAccount1);
    accounts.add(testAccount2);

    mySqlConfigProps.setProperty(ENABLE_NEW_DATABASE_FOR_MIGRATION, "false");
    mySqlAccountService = getAccountService();
    mySqlAccountService.updateAccounts(accounts);

    // add accounts in new db
    mySqlConfigProps.setProperty(ENABLE_NEW_DATABASE_FOR_MIGRATION, "true");
    mySqlAccountService = getAccountService();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount1));
    assertEquals("Mismatch in account retrieved by ID", testAccount1,
        mySqlAccountService.getAccountById(testAccount1.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount1,
        mySqlAccountService.getAccountByName(testAccount1.getName()));

    // update test account1, should be able to get from both old and new db.
    testContainer1 = new ContainerBuilder(testContainer1).setMediaScanDisabled(true).setCacheable(true).build();
    testAccount1 = new AccountBuilder(testAccount1).addOrUpdateContainer(testContainer1).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount1));
    assertEquals("Mismatch in account retrieved by ID", testAccount1,
        mySqlAccountService.getAccountById(testAccount1.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount1,
        mySqlAccountService.getAccountByName(testAccount1.getName()));
    assertEquals("Mismatch in account retrieved by ID", testAccount1,
        mySqlAccountService.getAccountByIdNew(testAccount1.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount1,
        mySqlAccountService.getAccountByNameNew(testAccount1.getName()));

    // get testAccount2, should get from old db only.
    Account account2 = mySqlAccountService.getAccountById(testAccount2.getId());
    assertEquals("Mismatch in account retrieved by ID", testAccount2, account2);
    assertNull(mySqlAccountService.getAccountByIdNew(testAccount2.getId()));

    //update containers for testAccount2, should only update the container in old db, and do not throw exception.
    Container updatedContainer =
        new ContainerBuilder(testContainer2).setStatus(Container.ContainerStatus.DELETE_IN_PROGRESS).build();
    mySqlAccountService.updateContainers(testAccount2.getName(), Collections.singletonList(updatedContainer));
    assertEquals("Mismatch in container retrieved by id", updatedContainer,
        mySqlAccountService.getContainerById(testAccount2.getId(), testContainer2.getId()));
    assertEquals("Mismatch in container retrieved by name", updatedContainer,
        mySqlAccountService.getContainerByName(testAccount2.getName(), testContainer2.getName()));
    assertNull(mySqlAccountService.getAccountByIdNew(testAccount2.getId()));

    // add testAccount2 to new db, enable the config for enableGetFromNewDbOnly
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount2));
    mySqlConfigProps.setProperty(ENABLE_GET_FROM_NEW_DB_ONLY, "true");
    mySqlAccountService = getAccountService();
    assertEquals("Mismatch in account retrieved by name",
        mySqlAccountService.getAccountByNameOld(testAccount1.getName()),
        mySqlAccountService.getAccountByName(testAccount1.getName()));
    assertEquals("Mismatch in account retrieved by id", mySqlAccountService.getAccountByIdOld(testAccount1.getId()),
        mySqlAccountService.getAccountById(testAccount1.getId()));
    assertEquals("Mismatch in account retrieved by name",
        mySqlAccountService.getAccountByNameOld(testAccount2.getName()),
        mySqlAccountService.getAccountByName(testAccount2.getName()));
    assertEquals("Mismatch in account retrieved by id", mySqlAccountService.getAccountByIdOld(testAccount2.getId()),
        mySqlAccountService.getAccountById(testAccount2.getId()));
  }

  private void cleanup() throws SQLException {
    Connection dbConnection = mySqlAccountStore.getMySqlDataAccessor().getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    statement.executeUpdate("DELETE FROM " + AccountDao.ACCOUNT_TABLE);
    statement.executeUpdate("DELETE FROM " + AccountDao.CONTAINER_TABLE);
    Connection dbConnectionNew = mySqlAccountStoreNew.getMySqlDataAccessor().getDatabaseConnection(true);
    Statement statementNew = dbConnectionNew.createStatement();
    statementNew.executeUpdate("DELETE FROM " + AccountDao.ACCOUNT_TABLE);
    statementNew.executeUpdate("DELETE FROM " + AccountDao.CONTAINER_TABLE);
  }
}
