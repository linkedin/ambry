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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class CompositeAccountServiceTest {

  AccountService primaryAccountService = mock(HelixAccountService.class);
  AccountService secondaryAccountService = mock(MySqlAccountService.class);
  AccountServiceMetrics metrics = new AccountServiceMetrics(new MetricRegistry());
  Properties props = new Properties();
  AccountService compositeAccountService;

  public CompositeAccountServiceTest() {
    compositeAccountService = new CompositeAccountService(primaryAccountService, secondaryAccountService, metrics,
        new CompositeAccountServiceConfig(new VerifiableProperties(props)));
  }

  /**
   * Test composite getAccountById() with both sources returning the same result.
   */
  @Test
  public void testGetAccountByIdBothSuccess() {
    Account testAccount = getTestAccount();
    when(primaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    assertEquals("Unexpected response", testAccount, compositeAccountService.getAccountById(testAccount.getId()));
    verify(primaryAccountService).getAccountById(testAccount.getId());
    verify(secondaryAccountService).getAccountById(testAccount.getId());
    assertEquals("Expected zero inconsistency", 0, metrics.getAccountDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountById() with both sources returning different results.
   */
  @Test
  public void testGetAccountByIdResultsDifferent() {
    Account testAccount = getTestAccount();
    when(primaryAccountService.getAccountById(anyShort())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountById(anyShort())).thenReturn(null);
    assertEquals("Expected response from primary", testAccount,
        compositeAccountService.getAccountById(testAccount.getId()));
    verify(primaryAccountService).getAccountById(testAccount.getId());
    verify(secondaryAccountService).getAccountById(testAccount.getId());
    assertEquals("Expected one inconsistency", 1, metrics.getAccountDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountByName() with both sources returning the same result.
   */
  @Test
  public void testGetAccountByNameBothSuccess() {
    Account testAccount = getTestAccount();
    when(primaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    assertEquals("Unexpected response", testAccount, compositeAccountService.getAccountByName(testAccount.getName()));
    verify(primaryAccountService).getAccountByName(testAccount.getName());
    verify(secondaryAccountService).getAccountByName(testAccount.getName());
    assertEquals("Expected zero inconsistency", 0, metrics.getAccountDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getAccountByName() with both sources returning different results.
   */
  @Test
  public void testGetAccountByNameResultsDifferent() {
    Account testAccount = getTestAccount();
    when(primaryAccountService.getAccountByName(any())).thenReturn(testAccount);
    when(secondaryAccountService.getAccountByName(any())).thenReturn(
        new AccountBuilder(testAccount).status(Account.AccountStatus.INACTIVE).build());
    assertEquals("Expected response from primary", testAccount,
        compositeAccountService.getAccountByName(testAccount.getName()));
    verify(primaryAccountService).getAccountByName(testAccount.getName());
    verify(secondaryAccountService).getAccountByName(testAccount.getName());
    assertEquals("Expected one inconsistency", 1, metrics.getAccountDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainer(accountName, containerName) with both sources returning the same result.
   * @throws AccountServiceException
   */
  @Test
  public void testGetContainerByNameBothSuccess() throws AccountServiceException {
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
    when(primaryAccountService.getContainer(any(), any())).thenReturn(testContainer);
    when(secondaryAccountService.getContainer(any(), any())).thenReturn(testContainer);
    assertEquals("Unexpected response", testContainer,
        compositeAccountService.getContainer(testAccount.getName(), testContainer.getName()));
    verify(primaryAccountService).getContainer(testAccount.getName(), testContainer.getName());
    verify(secondaryAccountService).getContainer(testAccount.getName(), testContainer.getName());
    assertEquals("Expected zero inconsistency", 0, metrics.getContainerDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainer(accountName, containerName) with both sources returning different results.
   */
  @Test
  public void testGetContainerByNameResultsDifferent() throws AccountServiceException {
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
    when(primaryAccountService.getContainer(any(), any())).thenReturn(testContainer);
    when(secondaryAccountService.getContainer(any(), any())).thenReturn(null);
    assertEquals("Unexpected response", testContainer,
        compositeAccountService.getContainer(testAccount.getName(), testContainer.getName()));
    verify(primaryAccountService).getContainer(testAccount.getName(), testContainer.getName());
    verify(secondaryAccountService).getContainer(testAccount.getName(), testContainer.getName());
    assertEquals("Expected one inconsistency", 1, metrics.getContainerDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainersByStatus() with both sources returning the same result.
   */
  @Test
  public void testGetContainerByStatusBothSuccess() {
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
    Set<Container> activeContainers = new HashSet<>(Collections.singletonList(testContainer));
    when(primaryAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(activeContainers);
    when(secondaryAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(activeContainers);
    assertEquals("Unexpected response", activeContainers,
        compositeAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE));
    verify(primaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    verify(secondaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    assertEquals("Expected zero inconsistency", 0, metrics.getContainerDataInconsistencyCount.getCount());
  }

  /**
   * Test composite getContainersByStatus() with both sources returning different results.
   */
  @Test
  public void testGetContainersByStatusResultsDifferent() {
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
    Set<Container> primaryResult = new HashSet<>(Collections.singletonList(testContainer));
    Set<Container> secondaryResult = new HashSet<>();
    when(primaryAccountService.getContainersByStatus(any())).thenReturn(primaryResult);
    when(secondaryAccountService.getContainersByStatus(any())).thenReturn(secondaryResult);
    assertEquals("", primaryResult, compositeAccountService.getContainersByStatus(Container.ContainerStatus.ACTIVE));
    verify(primaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    verify(secondaryAccountService).getContainersByStatus(Container.ContainerStatus.ACTIVE);
    assertEquals("Expected one inconsistency", 1, metrics.getContainerDataInconsistencyCount.getCount());
  }

  /**
   * Test composite updateAccounts() with both sources returning the same result.
   */
  @Test
  public void testUpdateAccountsBothSuccess() throws AccountServiceException {
    Account testAccount = getTestAccount();
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
    Account testAccount = getTestAccount();
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
    compositeAccountService.updateAccounts(accountsToUpdate);
    verify(primaryAccountService).updateAccounts(accountsToUpdate);
    verify(secondaryAccountService).updateAccounts(accountsToUpdate);
  }

  /**
   * Test composite updateContainers() with both sources returning the same result.
   */
  @Test
  public void testUpdateContainersBothSuccess() throws Exception {
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
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
    Account testAccount = getTestAccount();
    Container testContainer = testAccount.getAllContainers().iterator().next();
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
    assertEquals("Unexpected response", updatedContainers,
        compositeAccountService.updateContainers(testAccount.getName(), updatedContainers));
    verify(primaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
    verify(secondaryAccountService).updateContainers(testAccount.getName(), updatedContainers);
  }

  private Account getTestAccount() {
    Container testContainer =
        new ContainerBuilder((short) 1, "test container", Container.ContainerStatus.ACTIVE, "test container", (short) 1)
            .build();
    return new AccountBuilder((short) 1, "test account", Account.AccountStatus.ACTIVE).addOrUpdateContainer(
        testContainer).build();
  }
}
