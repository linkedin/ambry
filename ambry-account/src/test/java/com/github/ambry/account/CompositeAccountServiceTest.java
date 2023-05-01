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
import com.github.ambry.config.CompositeAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link CompositeAccountService}.
 */
public class CompositeAccountServiceTest {

  AccountService primaryAccountService = mock(HelixAccountService.class);
  AccountService secondaryAccountService = mock(MySqlAccountService.class);
  AccountServiceMetrics metrics = new AccountServiceMetrics(new MetricRegistry(), false);
  Properties props = new Properties();
  AccountService compositeAccountService;
  Container testContainer =
      new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build();
  Account testAccount =
      new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).addOrUpdateContainer(testContainer).build();

  public CompositeAccountServiceTest() {
    props.setProperty(CompositeAccountServiceConfig.SAMPLING_PERCENTAGE_FOR_GET_CONSISTENCY_CHECK, "100");
    props.setProperty(CompositeAccountServiceConfig.PRIMARY_ACCOUNT_SERVICE_FACTORY,
        "com.github.ambry.account.HelixAccountServiceFactory");
    props.setProperty(CompositeAccountServiceConfig.SECONDARY_ACCOUNT_SERVICE_FACTORY,
        "com.github.ambry.account.MySqlAccountServiceFactory");
    compositeAccountService = new CompositeAccountService(primaryAccountService, secondaryAccountService, metrics,
        new CompositeAccountServiceConfig(new VerifiableProperties(props)));
  }

  /**
   * Test composite getAccountById() with both sources returning the same result.
   */
  @Test
  public void testGetAccountByIdBothSuccess() {
    when(primaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    assertEquals("Unexpected response", testAccount, compositeAccountService.getAccountById(testAccount.getId()));
    verify(primaryAccountService).getAccountById(testAccount.getId());
    verify(secondaryAccountService).getAccountById(testAccount.getId());
    assertEquals("Expected zero inconsistency", 0, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountById() with both sources returning different results.
   */
  @Test
  public void testGetAccountByIdResultsDifferent() {
    when(primaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountById(anyShort())).thenReturn(null);
    assertEquals("Expected response from primary", testAccount,
        compositeAccountService.getAccountById(testAccount.getId()));
    verify(primaryAccountService).getAccountById(testAccount.getId());
    verify(secondaryAccountService).getAccountById(testAccount.getId());
    assertEquals("Expected one inconsistency", 1, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountByName() with both sources returning the same result.
   */
  @Test
  public void testGetAccountByNameBothSuccess() {
    when(primaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    assertEquals("Unexpected response", testAccount, compositeAccountService.getAccountByName(testAccount.getName()));
    verify(primaryAccountService).getAccountByName(testAccount.getName());
    verify(secondaryAccountService).getAccountByName(testAccount.getName());
    assertEquals("Expected zero inconsistency", 0, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountByName() with both sources returning different results.
   */
  @Test
  public void testGetAccountByNameResultsDifferent() {
    when(primaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountByName(any())).thenReturn(
        new AccountBuilder(testAccount).status(Account.AccountStatus.INACTIVE).build());
    assertEquals("Expected response from primary", testAccount,
        compositeAccountService.getAccountByName(testAccount.getName()));
    verify(primaryAccountService).getAccountByName(testAccount.getName());
    verify(secondaryAccountService).getAccountByName(testAccount.getName());
    assertEquals("Expected one inconsistency", 1, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainer(accountName, containerName) with both sources returning the same result.
   */
  @Test
  public void testGetContainerByNameBothSuccess() throws AccountServiceException {
    Container testContainer = testAccount.getAllContainers().iterator().next();
    when(primaryAccountService.getContainerByName(any(), any())).thenReturn(testContainer);
    when(secondaryAccountService.getContainerByName(any(), any())).thenReturn(testContainer);
    assertEquals("Unexpected response", testContainer,
        compositeAccountService.getContainerByName(testAccount.getName(), testContainer.getName()));
    verify(primaryAccountService).getContainerByName(testAccount.getName(), testContainer.getName());
    verify(secondaryAccountService).getContainerByName(testAccount.getName(), testContainer.getName());
    assertEquals("Expected zero inconsistency", 0, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainer(accountName, containerName) with both sources returning different results.
   */
  @Test
  public void testGetContainerByNameResultsDifferent() throws AccountServiceException {
    Container testContainer = testAccount.getAllContainers().iterator().next();
    when(primaryAccountService.getContainerByName(any(), any())).thenReturn(testContainer);
    when(secondaryAccountService.getContainerByName(any(), any())).thenReturn(null);
    assertEquals("Unexpected response", testContainer,
        compositeAccountService.getContainerByName(testAccount.getName(), testContainer.getName()));
    verify(primaryAccountService).getContainerByName(testAccount.getName(), testContainer.getName());
    verify(secondaryAccountService).getContainerByName(testAccount.getName(), testContainer.getName());
    assertEquals("Expected one inconsistency", 1, metrics.getAccountInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainersByStatus() with both sources returning the same result.
   */
  @Test
  public void testGetContainerByStatusBothSuccess() {
    Set<Container> activeContainers = new HashSet<>(Collections.singletonList(testContainer));
    when(primaryAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(activeContainers);
    when(secondaryAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(activeContainers);
    assertEquals("Unexpected response", activeContainers,
        compositeAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE));
    verify(primaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    verify(secondaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
  }

  /**
   * Test composite getContainersByStatus() with both sources returning different results.
   */
  @Test
  public void testGetContainersByStatusResultsDifferent() {
    Set<Container> primaryResult = new HashSet<>(Collections.singletonList(testContainer));
    Set<Container> secondaryResult = new HashSet<>();
    when(primaryAccountService.getContainersByStatus(any())).thenReturn(primaryResult);
    when(secondaryAccountService.getContainersByStatus(any())).thenReturn(secondaryResult);
    assertEquals("", primaryResult, compositeAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE));
    verify(primaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    verify(secondaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
  }

  /**
   * Test composite updateAccounts() with both sources returning the same result.
   */
  @Test
  public void testUpdateAccountsBothSuccess() throws AccountServiceException {
    Account updatedTestAccount = new AccountBuilder(testAccount).status(Account.AccountStatus.INACTIVE).build();
    Collection<Account> accountsToUpdate = Collections.singletonList(updatedTestAccount);
    compositeAccountService.updateAccounts(accountsToUpdate);
    verify(primaryAccountService).updateAccounts(accountsToUpdate);
    verify(secondaryAccountService).updateAccounts(accountsToUpdate);
  }

  /**
   * Test composite updateAccounts() with either of the sources throwing exception.
   */
  @Test
  public void testUpdateAccountsException() throws Exception {
    Account updatedTestAccount = new AccountBuilder(testAccount).status(Account.AccountStatus.INACTIVE).build();
    Collection<Account> accountsToUpdate = Collections.singletonList(updatedTestAccount);
    // exception in primary should be thrown
    doThrow(new AccountServiceException("", AccountServiceErrorCode.InternalError)).when(primaryAccountService)
        .updateAccounts(any());
    TestUtils.assertException(AccountServiceException.class,
        () -> compositeAccountService.updateAccounts(accountsToUpdate), null);
    verify(primaryAccountService).updateAccounts(accountsToUpdate);
    verify(secondaryAccountService, never()).updateAccounts(any());

    // exception in secondary should be swallowed
    reset(primaryAccountService, secondaryAccountService);
    doThrow(new AccountServiceException("", AccountServiceErrorCode.InternalError)).when(secondaryAccountService)
        .updateAccounts(any());
    TestUtils.assertException(AccountServiceException.class,
        () -> compositeAccountService.updateAccounts(accountsToUpdate), null);
    verify(primaryAccountService).updateAccounts(accountsToUpdate);
    verify(secondaryAccountService).updateAccounts(accountsToUpdate);
  }

  /**
   * Test composite updateContainers() with both sources returning the same result.
   */
  @Test
  public void testUpdateContainersBothSuccess() throws Exception {
    Collection<Container> updatedContainers = Collections.singletonList(
        new ContainerBuilder(testContainer).setStatus(Container.ContainerStatus.INACTIVE).build());
    when(primaryAccountService.updateContainers(testAccount.getName(), updatedContainers)).thenReturn(
        updatedContainers);
    when(secondaryAccountService.updateContainers(testAccount.getName(), updatedContainers)).thenReturn(
        updatedContainers);
    assertEquals("Unexpected response", updatedContainers,
        compositeAccountService.updateContainers(testAccount.getName(), updatedContainers));
    verify(primaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
    verify(secondaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
  }

  /**
   * Test composite updateContainers() with either of the sources returning throwing exception.
   */
  @Test
  public void testUpdateContainersException() throws Exception {
    Collection<Container> updatedContainers = Collections.singletonList(
        new ContainerBuilder(testContainer).setStatus(Container.ContainerStatus.INACTIVE).build());

    // exception in primary should be thrown
    when(primaryAccountService.updateContainers(any(), any())).thenThrow(
        new AccountServiceException("", AccountServiceErrorCode.InternalError));
    when(secondaryAccountService.updateContainers(any(), any())).thenReturn(updatedContainers);
    TestUtils.assertException(AccountServiceException.class,
        () -> compositeAccountService.updateContainers(testAccount.getName(), updatedContainers), null);
    verify(primaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
    verify(secondaryAccountService, never()).updateContainers(any(), any());

    // exception in secondary should be swallowed
    reset(primaryAccountService, secondaryAccountService);
    when(primaryAccountService.updateContainers(any(), any())).thenReturn(updatedContainers);
    when(secondaryAccountService.updateContainers(any(), any())).thenThrow(
        new AccountServiceException("", AccountServiceErrorCode.InternalError));
    TestUtils.assertException(AccountServiceException.class,
        () -> compositeAccountService.updateContainers(testAccount.getName(), updatedContainers), null);
    verify(primaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
    verify(secondaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
  }

  @Test
  public void testCompareAccountMetadata() throws AccountServiceException {
    Account a1 = testAccount;
    Container c1 = testContainer;
    Container c2 = new ContainerBuilder((short) 2, "c2", Container.ContainerStatus.ACTIVE, "c2", (short) 1).build();
    Container c2Updated = new ContainerBuilder(c2).setStatus(Container.ContainerStatus.INACTIVE).build();
    Container c3 = new ContainerBuilder((short) 3, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();

    List<Account> accountsInSecondary =
        Collections.singletonList(new AccountBuilder(a1).addOrUpdateContainer(c2).build());

    List<Account> accountsInPrimary =
        Arrays.asList(new AccountBuilder(a1).addOrUpdateContainer(c2Updated).addOrUpdateContainer(c3).build(),
            new AccountBuilder((short) 2, "a2", Account.AccountStatus.ACTIVE).build());

    when(primaryAccountService.getAllAccounts()).thenReturn(accountsInPrimary);
    when(secondaryAccountService.getAllAccounts()).thenReturn(accountsInSecondary);
    when(secondaryAccountService.getAccountById((short) 1)).thenReturn(accountsInSecondary.iterator().next());
    when(secondaryAccountService.getContainerByName("a1", "c1")).thenReturn(c1);
    when(secondaryAccountService.getContainerByName("a1", "c2")).thenReturn(c2);

    ((CompositeAccountService) compositeAccountService).compareAccountMetadata();
    verify(primaryAccountService, atLeastOnce()).getAllAccounts();
    verify(secondaryAccountService, atLeastOnce()).getAllAccounts();
    // Expects 3 inconsistencies: 1. account a2 missing in secondary, 2. container (a1,c3) missing in secondary, 3. container (a1,c2) different in secondary
    assertEquals("Expected 2 inconsistencies between primary and secondary", 3,
        metrics.accountDataInconsistencyCount.getValue().intValue());
  }
}
