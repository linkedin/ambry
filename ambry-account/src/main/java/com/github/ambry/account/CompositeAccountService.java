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
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private final AccountServiceMetrics metrics;
  private final CompositeAccountServiceConfig config;
  private final ScheduledExecutorService scheduler;
  private static final Random random = new Random();
  private int accountsMismatchCount = 0;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  public CompositeAccountService(AccountService primaryAccountService, AccountService secondaryAccountService,
      AccountServiceMetrics metrics, CompositeAccountServiceConfig config) {
    this.primaryAccountService = primaryAccountService;
    this.secondaryAccountService = secondaryAccountService;
    this.metrics = metrics;
    this.config = config;
    scheduler = Utils.newScheduler(1, ACCOUNT_DATA_CONSISTENCY_CHECKER_PREFIX, false);
    scheduler.scheduleAtFixedRate(this::compareAccountMetadata, config.consistencyCheckerIntervalMinutes,
        config.consistencyCheckerIntervalMinutes, TimeUnit.MINUTES);
    metrics.trackAccountDataInconsistency(this);
  }

  @Override
  public Account getAccountById(short accountId) {
    Account primaryResult = primaryAccountService.getAccountById(accountId);
    if (shouldCompare()) {
      try {
        Account secondaryResult = secondaryAccountService.getAccountById(accountId);
        if (primaryResult != null && !primaryResult.equals(secondaryResult)) {
          logger.warn("Inconsistency detected between primary and secondary for accountId ={}", accountId);
          metrics.getAccountInconsistencyCount.inc();
        }
      } catch (Exception e) {
        logger.error("Get account failed for secondary for account id = {}", accountId, e);
        metrics.secondaryGetAccountErrorCount.inc();
      }

    }
    return primaryResult;
  }

  @Override
  public Account getAccountByName(String accountName) {
    Account primaryResult = primaryAccountService.getAccountByName(accountName);
    if (shouldCompare()) {
      try {
        Account secondaryResult = secondaryAccountService.getAccountByName(accountName);
        if (primaryResult != null && !primaryResult.equals(secondaryResult)) {
          logger.warn("Inconsistency detected between primary and secondary for accountName ={}", accountName);
          metrics.getAccountInconsistencyCount.inc();
        }
      } catch (Exception e) {
        logger.error("Failed to get account in secondaryAccountService for account name = {}", accountName, e);
        metrics.secondaryGetAccountErrorCount.inc();
      }

    }
    return primaryResult;
  }

  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    primaryAccountService.updateAccounts(accounts);

    executor.submit(() -> {
      try {
        secondaryAccountService.updateAccounts(accounts);
      } catch (Exception e) {
        logger.error("Failed to update accounts in secondaryAccountService", e);
        metrics.secondaryUpdateAccountErrorCount.inc();
      }
    });
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
    executor.submit(() -> {
      try {
        Collection<Container> secondaryResult = secondaryAccountService.updateContainers(accountName, containers);
        if (primaryResult != null && !primaryResult.equals(secondaryResult)) {
          logger.warn("Inconsistent container list detected between primary and secondary for accountName ={}", accountName);
          metrics.updateContainersInconsistencyCount.inc();
        }
      } catch (Exception e) {
          logger.error("Failed to update containers in secondaryAccountService for account: " + accountName, e);
          metrics.secondaryUpdateContainerErrorCount.inc();
      }
    });
    return primaryResult;
  }

  @Override
  public Container getContainerByName(String accountName, String containerName) throws AccountServiceException {
    Container primaryResult = primaryAccountService.getContainerByName(accountName, containerName);
    if (shouldCompare()) {
      try {
        Container secondaryResult = secondaryAccountService.getContainerByName(accountName, containerName);
        if (primaryResult != null && !primaryResult.equals(secondaryResult)) {
          logger.warn("Inconsistency detected between primary and secondary for accountName ={}, containerName = {}",
              accountName, containerName);
          metrics.getContainerInconsistencyCount.inc();
        }
      } catch (Exception e) {
        metrics.secondaryGetContainerErrorCount.inc();
        logger.error("Failed to get container in secondaryAccountService for accountName={}, containerName={}", accountName,
            containerName, e);
      }
    }
    return primaryResult;
  }

  @Override
  public Set<Container> getContainersByStatus(Container.ContainerStatus containerStatus) {
    Set<Container> primaryResult = primaryAccountService.getContainersByStatus(containerStatus);
    if (shouldCompare()) {
      try {
        Set<Container> secondaryResult = secondaryAccountService.getContainersByStatus(containerStatus);
        if (primaryResult != null && !primaryResult.equals(secondaryResult)) {
          logger.warn(
                  "Inconsistency detected between primary and secondary for containers with status ={}, primary ={}, secondary = {}",
                  containerStatus, primaryResult, secondaryResult);
          metrics.getContainerInconsistencyCount.inc();
        }
      } catch (Exception e) {
        metrics.secondaryGetContainerErrorCount.inc();
        logger.error("Failed to get container in secondaryAccountService for container with status ={}", containerStatus, e);
      }
    }
    return primaryResult;
  }

  @Override
  public void selectInactiveContainersAndMarkInStore(AggregatedAccountStorageStats aggregatedAccountStorageStats) {
    primaryAccountService.selectInactiveContainersAndMarkInStore(aggregatedAccountStorageStats);
    secondaryAccountService.selectInactiveContainersAndMarkInStore(aggregatedAccountStorageStats);
  }

  @Override
  public void close() throws IOException {
    shutDownExecutorService(scheduler, config.consistencyCheckerShutdownTimeoutMinutes, TimeUnit.MINUTES);
    try {
      primaryAccountService.close();
    } catch (Exception e) {
      logger.error("Close failed for primary source", e);
    }
    try {
      secondaryAccountService.close();
    } catch (Exception e) {
      logger.error("Close failed for secondary source", e);
    }
  }

  /**
   * Compares and logs differences (if any) in Account metadata stored in primary and secondary sources
   */
  void compareAccountMetadata() {
    accountsMismatchCount =
        AccountUtils.compareAccounts(primaryAccountService.getAllAccounts(), secondaryAccountService.getAllAccounts());
  }

  /**
   * Checks if we should compare GET results from primary and secondary sources.
   * @return true if next value from random is less than sampling percentage.
   */
  private boolean shouldCompare() {
    return random.nextInt(100) < config.samplingPercentageForGetConsistencyCheck;
  }

  /**
   * @return number of mismatch accounts and containers between primary and secondary sources.
   */
  public int getAccountsMismatchCount() {
    return accountsMismatchCount;
  }
}
