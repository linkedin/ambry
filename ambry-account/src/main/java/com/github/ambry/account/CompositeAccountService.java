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

import com.github.ambry.config.CompositeAccountServiceConfig;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * An implementation of {@link AccountService} that combines two sources. The "primary" acts as the source of
 * truth for users of this class, but results obtained are compared against the "secondary" source. This class can be
 * useful for safe migrations between account service implementations using different backing stores.
 */
public class CompositeAccountService implements AccountService {

  private static final Logger logger = LoggerFactory.getLogger(CompositeAccountService.class);
  private static final String ACCOUNT_DATA_CONSISTENCY_CHECKER_PREFIX = "account-data-consistency-checker";
  private final AccountService primaryAccountService;
  private final AccountService secondaryAccountService;
  private final AccountServiceMetrics accountServiceMetrics;
  private final CompositeAccountServiceConfig config;
  private final ScheduledExecutorService scheduler;
  private static long getAccountRequestCount = 0;
  private static long getContainerRequestCount = 0;

  public CompositeAccountService(AccountService primaryAccountService, AccountService secondaryAccountService,
      AccountServiceMetrics accountServiceMetrics, CompositeAccountServiceConfig config) {
    this.primaryAccountService = primaryAccountService;
    this.secondaryAccountService = secondaryAccountService;
    this.accountServiceMetrics = accountServiceMetrics;
    this.config = config;
    scheduler = Utils.newScheduler(1, ACCOUNT_DATA_CONSISTENCY_CHECKER_PREFIX, false);
    scheduler.scheduleAtFixedRate(this::compareAccountMetadata, config.consistencyCheckerIntervalMinutes,
        config.consistencyCheckerIntervalMinutes, TimeUnit.MINUTES);
  }

  @Override
  public Account getAccountById(short accountId) {
    Account primaryResult = primaryAccountService.getAccountById(accountId);
    Account secondaryResult = secondaryAccountService.getAccountById(accountId);
    if ((++getAccountRequestCount % 100 < config.samplingPercentageForGetAccountConsistencyCheck)
        && primaryResult != null && !primaryResult.equals(secondaryResult)) {
      logger.warn("Inconsistency detected between primary and secondary for accountId ={}", accountId);
      accountServiceMetrics.getAccountDataInconsistencyCount.inc();
    }
    return primaryResult;
  }

  @Override
  public Account getAccountByName(String accountName) {
    Account primaryResult = primaryAccountService.getAccountByName(accountName);
    Account secondaryResult = secondaryAccountService.getAccountByName(accountName);
    if (++getAccountRequestCount % 100 < config.samplingPercentageForGetAccountConsistencyCheck && primaryResult != null
        && !primaryResult.equals(secondaryResult)) {
      logger.warn("Inconsistency detected between primary and secondary for accountName ={}", accountName);
      accountServiceMetrics.getAccountDataInconsistencyCount.inc();
    }
    return primaryResult;
  }

  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    primaryAccountService.updateAccounts(accounts);
    try {
      secondaryAccountService.updateAccounts(accounts);
      // Not comparing updated accounts list as it could increase latency of update operation.
    } catch (Exception e) {
      logger.error("Update accounts failed for secondary source", e);
      accountServiceMetrics.updateAccountInconsistencyCount.inc();
    }
  }

  @Override
  public Collection<Account> getAllAccounts() {
    return primaryAccountService.getAllAccounts();
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return primaryAccountService.addAccountUpdateConsumer(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return primaryAccountService.removeAccountUpdateConsumer(accountUpdateConsumer);
  }

  @Override
  public Collection<Container> updateContainers(String accountName, Collection<Container> containers)
      throws AccountServiceException {
    Collection<Container> primaryResult = primaryAccountService.updateContainers(accountName, containers);
    try {
      secondaryAccountService.updateContainers(accountName, containers);
      // Not comparing updated containers list as it could increase latency of update operation.
    } catch (Exception e) {
      logger.error("Update containers failed for secondary source", e);
      accountServiceMetrics.updateContainerInconsistencyCount.inc();
    }
    return primaryResult;
  }

  @Override
  public Container getContainer(String accountName, String containerName) throws AccountServiceException {
    Container primaryResult = primaryAccountService.getContainer(accountName, containerName);
    try {
      Container secondaryResult = secondaryAccountService.getContainer(accountName, containerName);
      if (++getContainerRequestCount % 100 < config.samplingPercentageForGetContainerConsistencyCheck
          && primaryResult != null && !primaryResult.equals(secondaryResult)) {
        logger.warn("Inconsistency detected between primary and secondary for accountName ={}, containerName = {}",
            accountName, containerName);
        accountServiceMetrics.getContainerDataInconsistencyCount.inc();
      }
    } catch (Exception e) {
      accountServiceMetrics.getContainerDataInconsistencyCount.inc();
      logger.error("get container failed for secondary for accountName={}, containerName={}", accountName,
          containerName, e);
    }
    return primaryResult;
  }

  @Override
  public Set<Container> getContainersByStatus(Container.ContainerStatus containerStatus) {
    Set<Container> primaryResult = primaryAccountService.getContainersByStatus(containerStatus);
    Set<Container> secondaryResult = secondaryAccountService.getContainersByStatus(containerStatus);
    if (!primaryResult.equals(secondaryResult)) {
      logger.warn(
          "Inconsistency detected between primary and secondary for containers with status ={}, primary ={}, secondary = {}",
          containerStatus, primaryResult, secondaryResult);
      accountServiceMetrics.getContainerDataInconsistencyCount.inc();
    }
    return primaryResult;
  }

  @Override
  public void selectInactiveContainersAndMarkInZK(StatsSnapshot statsSnapshot) {
    primaryAccountService.selectInactiveContainersAndMarkInZK(statsSnapshot);
    try {
      secondaryAccountService.selectInactiveContainersAndMarkInZK(statsSnapshot);
    } catch (Exception e) {
      logger.warn("Marking containers as inactive failed for secondary", e);
      accountServiceMetrics.updateContainerInconsistencyCount.inc();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      primaryAccountService.close();
    } finally {
      secondaryAccountService.close();
    }
    shutDownExecutorService(scheduler, config.consistencyCheckerShutdownTimeoutMinutes, TimeUnit.MINUTES);
  }

  /**
   * Compares and logs differences (if any) in Account metadata stored in primary and secondary sources
   */
  private void compareAccountMetadata() {
    Set<Account> primaryAccounts = new HashSet<>(primaryAccountService.getAllAccounts());
    Set<Account> secondaryAccounts = new HashSet<>(secondaryAccountService.getAllAccounts());
    if (!primaryAccounts.equals(secondaryAccounts)) {
      accountServiceMetrics.accountMetadataInconsistencyCount.inc();
      logger.debug("Inconsistency detected between primary and secondary at {} for accounts ={}",
          SystemTime.getInstance().milliseconds(), primaryAccounts.removeAll(secondaryAccounts));
    }
  }
}
