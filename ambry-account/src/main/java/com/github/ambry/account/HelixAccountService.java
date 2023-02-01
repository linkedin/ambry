/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.Notifier;
import com.github.ambry.commons.TopicListener;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.router.Router;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.AccountUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * <p>
 *   An implementation of {@link AccountService} that employs a {@link HelixPropertyStore} as its underlying storage.
 *   It has two different {@link AccountMetadataStore} implementations. Use {@link HelixAccountServiceConfig#useNewZNodePath}
 *   to control which one to use. When the configuration is true, it uses the {@link RouterStore}. Otherwise, it uses
 *   {@link LegacyMetadataStore}.
 *   In both {@link AccountMetadataStore}, the latest full {@link Account} metadata will be cached locally, so serving an
 *   {@link Account} query will not incur any calls to remote {@code ZooKeeper} or {@code AmbryServer}.
 * </p>
 * <p>
 *   When a {@link HelixAccountService} starts up, it will automatically fetch a full set of {@link Account} metadata
 *   and store them in its local cache. It also takes a {@link Notifier}, and subscribes to a {@link #ACCOUNT_METADATA_CHANGE_TOPIC}.
 *   Each time when receiving a {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE}, it will fetch the updated full
 *   {@link Account} metadata, and refresh its local cache. After every successful operation for updating a collection of
 *   {@link Account}s, it publishes a {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE} for {@link #ACCOUNT_METADATA_CHANGE_TOPIC}
 *   through the {@link Notifier}. If the remote {@link Account} metadata is corrupted or has conflict, {@link HelixAccountService}
 *   will not update its local cache.
 * </p>
 * <p>
 *   Previously, {@link HelixAccountService} only keeps a backup when there is an update {@link Account} HTTP request
 *   received by this instance. It doesn't backup mutations made by other instances. Since HTTP requests to update
 *   {@link Account} are rare, latest backup file often holds a out-of-date view of the {@link Account} metadata at lots
 *   of instances. In order to keep backup file up-to-date, in the new implementation, each mutation to the {@link Account}
 *   metadata will be persisted with {@link BackupFileManager}. This is achieved by the fact the {@link HelixAccountService} will
 *   publish each mutation to {@code ZooKeeper} and upon receiving the mutation message, all the {@link HelixAccountService}
 *   instances will fetch the latest {@link Account} metadata. And when it does, it also persists the latest {@link Account}
 *   metadata in the backup.
 *
 *   Every time a mutation of account metadata is performed, a new backup file will be created for locally, in a directory
 *   specified by the {@link HelixAccountServiceConfig#backupDir}. A backup file is created when the helix listener on
 *   {@link #ACCOUNT_METADATA_CHANGE_TOPIC} is notified. And backup's filename could contain version and last modified time
 *   information of the znode that stores the account metadata.
 *   The flow to update account looks like
 *   <ol>
 *     <li>{@link #updateAccounts(Collection)} is invoked</li>
 *     <li>{@link AccountMetadataStore} merges the incoming account mutation request with the current account set and write it back to helix</li>
 *     <li>{@link Notifier} publish this action to helix by writing to {@link #ACCOUNT_METADATA_CHANGE_TOPIC}</li>
 *     <li>Listeners listening on {@link #ACCOUNT_METADATA_CHANGE_TOPIC} fetches the new account metadata set</li>
 *     <li>Listeners persists this new set as a backup file through {@link BackupFileManager}</li>
 *   </ol>
 *   There are limited number of backup files can be persisted. Once the number is exceeded, {@link BackupFileManager} would
 *   start removing the oldest backup file. The number can be configured through {@link HelixAccountServiceConfig#maxBackupFileCount}.
 * </p>
 * <p>
 *   The full set of the {@link Account} metadata are stored in a single {@link ZNRecord} in {@link LegacyMetadataStore}
 *   or a single blob in {@link RouterStore}, as a simple map from a string account id to the {@link Account} content in
 *   json as a string.
 * </p>
 * <p>
 *   Limited by {@link HelixPropertyStore}, the total size of {@link Account} data stored on a single {@link ZNRecord}
 *   cannot exceed 1MB. If the serialized json string exceeded 1MB limitation, it will then be compressed before saving
 *   back to zookeeper.
 * </p>
 */
public class HelixAccountService extends AbstractAccountService {

  private final AtomicBoolean open = new AtomicBoolean(true);
  private final BackupFileManager backupFileManager;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final ScheduledExecutorService scheduler;
  private final HelixAccountServiceConfig config;
  private final AccountMetadataStore accountMetadataStore;
  private static final Logger logger = LoggerFactory.getLogger(HelixAccountService.class);
  private final AtomicReference<Router> router = new AtomicReference<>();

  /**
   * <p>
   *   Constructor. It fetches the remote account data in {@code ZooKeeper} and caches locally during initialization,
   *   and updates the remote account data during updating accounts. If a non-null {@link Notifier} is provided, it
   *   will listen to the changes of the remote accounts, and reactively updates its local cache. It als sends message
   *   to notify other listeners after each update made to the remote accounts.
   * </p>
   * <p>
   *   This call is blocking until it fetches all the {@link Account} metadata from {@link HelixPropertyStore}.
   * </p>
   * @param helixStore A {@link HelixPropertyStore} used by the {@code HelixAccountService}. Cannot be {@code null}.
   * @param accountServiceMetrics {@link AccountServiceMetrics} to report metrics. Cannot be {@code null}.
   * @param notifier A {@link Notifier} that will be used to publish message after updating {@link Account}s, and
   *                 listen to {@link Account} change messages. Can be {@code null}.
   * @param scheduler A {@link ScheduledExecutorService} that will run thread to update accounts in background.
   *                  {@code null} to disable background updating.
   * @param config The configs for {@code HelixAccountService}.
   * @throws IOException if backup directory creation was needed but failed.
   */
  HelixAccountService(HelixPropertyStore<ZNRecord> helixStore, AccountServiceMetrics accountServiceMetrics,
      Notifier<String> notifier, ScheduledExecutorService scheduler, HelixAccountServiceConfig config)
      throws IOException {
    super(config, Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null"), notifier);
    this.helixStore = Objects.requireNonNull(helixStore, "helixStore cannot be null");
    this.scheduler = scheduler;
    this.config = config;
    this.backupFileManager =
        new BackupFileManager(this.accountServiceMetrics, config.backupDir, config.maxBackupFileCount);
    if (config.useNewZNodePath) {
      accountMetadataStore = new RouterStore(this.accountServiceMetrics, backupFileManager, helixStore, router, false,
          config.totalNumberOfVersionToKeep, config);
      // postpone initializeFetchAndSchedule to setupRouter function.
    } else {
      accountMetadataStore = new LegacyMetadataStore(this.accountServiceMetrics, backupFileManager, helixStore, config);
      initialFetchAndSchedule();
    }
  }

  /**
   * set the router to the given one. This is a blocking call. This function would block until it fetches the
   * {@link Account} metadata from ambry server.
   * @param router The router to set.
   * @throws IllegalStateException when the router already set up.
   */
  public void setupRouter(final Router router) throws IllegalStateException {
    if (!this.router.compareAndSet(null, router)) {
      throw new IllegalStateException("Router already initialized");
    } else if (config.useNewZNodePath) {
      initialFetchAndSchedule();
    }
  }

  /**
   * A synchronized function to fetch account metadata from {@link AccountMetadataStore} and update the in memory cache.
   * @param isCalledFromListener True is this function is invoked in the {@link TopicListener}.
   */
  private synchronized void fetchAndUpdateCache(boolean isCalledFromListener) {
    Collection<Account> accounts = accountMetadataStore.fetchAccountMetadata();
    if (accounts == null) {
      logger.debug("No account collection returned");
    } else {
      logger.trace("Start parsing remote account data");
      AccountInfoMap newAccountInfoMap = new AccountInfoMap(accounts);
      AccountInfoMap oldAccountInfoMap = accountInfoMapRef.getAndSet(newAccountInfoMap);

      //Notify modified accounts to consumers
      Collection<Account> updatedAccounts = newAccountInfoMap.getAccounts()
          .stream()
          .filter(newAccount -> !newAccount.equals(oldAccountInfoMap.getAccountById(newAccount.getId())))
          .collect(Collectors.toSet());
      notifyAccountUpdateConsumers(updatedAccounts, isCalledFromListener);
    }
  }

  /**
   * This is a blocking call that would fetch the {@link Account} metadata and update the cache. Then schedule this logic
   * at a fixed rate.
   */
  private void initialFetchAndSchedule() {
    Runnable updater = () -> {
      try {
        fetchAndUpdateCache(false);
      } catch (Exception e) {
        logger.error("Exception occurred when fetching remote account data", e);
        accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
      }
    };
    updater.run();

    // If fetching account metadata failed, no matter for what reason, we use the data from local backup file.
    // The local backup should be reasonably up-to-date.
    //
    // The caveat is that when a machine used to run ambry-frontend but then got decommissioned for a long time,
    // it will have a very old account metadata in the backup. If we reschedule ambry-frontend process in this particular
    // machine, and it fails to read the account metadata from AccountMetadataStore, then we would load stale data.
    //
    // One way to avoid this problem is to load latest account metadata, but not more than a month, from backup.

    // accountInfoMapRef's reference is empty doesn't mean that fetchAndUpdateCache failed, it would just be that there
    // is no account metadata for the time being. Theoretically local storage shouldn't have any backup files. So
    // backup.getLatestAccounts should return null. And in case we have a very old backup file just mentioned above, a threshold
    // would solve the problem.
    if (accountInfoMapRef.get().isEmpty() && config.enableServeFromBackup && !backupFileManager.isEmpty()) {
      long aMonthAgo = System.currentTimeMillis() / 1000 - TimeUnit.DAYS.toSeconds(30);
      Collection<Account> accounts = backupFileManager.getLatestAccounts(aMonthAgo);
      if (accounts != null) {
        AccountInfoMap newAccountInfoMap = new AccountInfoMap(accounts);
        accountInfoMapRef.set(newAccountInfoMap);

        // Notify accounts to consumers
        notifyAccountUpdateConsumers(accountInfoMapRef.get().getAccounts(), false);
      }
    }

    if (scheduler != null) {
      int initialDelay = new Random().nextInt(config.updaterPollingIntervalMs + 1);
      scheduler.scheduleAtFixedRate(updater, initialDelay, config.updaterPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info(
          "Background account updater will fetch accounts from remote starting {} ms from now and repeat with interval={} ms",
          initialDelay, config.updaterPollingIntervalMs);
    }

    maybeSubscribeChangeTopic(true);
  }

  /**
   * {@inheritDoc}
   * <p>
   *   This call is blocking until it completes the operation of updating account metadata to {@link HelixPropertyStore}.
   * </p>
   * <p>
   *   There is a slight chance that an {@link Account} could be updated successfully, but its value was
   *   set based on an outdated {@link Account}. This can be fixed after {@code generationId} is introduced
   *   to {@link Account}.
   * </p>
   */
  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    updateAccountsWithAccountMetadataStore(accounts, accountMetadataStore);
  }

  /**
   * Helper function to update {@link Account} metadata.
   * @param accounts The {@link Account} metadata to update.
   * @param accountMetadataStore The {@link AccountMetadataStore}.
   * @throws AccountServiceException when the update operation fails.
   */
  void updateAccountsWithAccountMetadataStore(Collection<Account> accounts, AccountMetadataStore accountMetadataStore)
      throws AccountServiceException {
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      throw new IllegalArgumentException("Empty account collection to update.");
    }
    if (config.updateDisabled) {
      throw new AccountServiceException("Updates have been disabled", AccountServiceErrorCode.UpdateDisabled);
    }
    if (hasDuplicateAccountIdOrName(accounts)) {
      accountServiceMetrics.updateAccountErrorCount.inc();
      throw new AccountServiceException("Duplicate account id or name exist in the accounts to update",
          AccountServiceErrorCode.ResourceConflict);
    }
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating to HelixPropertyStore with accounts={}", accounts);
    boolean hasSucceeded = false;
    // make a pre check for conflict between the accounts to update and the accounts in the local cache. Will fail this
    // update operation for all the accounts if any conflict exists. There is a slight chance that the account to update
    // conflicts with the accounts in the local cache, but does not conflict with those in the helixPropertyStore. This
    // will happen if some accounts are updated but the local cache is not refreshed.
    if (accountInfoMapRef.get().hasConflictingAccount(accounts, config.ignoreVersionMismatch)) {
      logger.debug("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
      accountServiceMetrics.updateAccountErrorCount.inc();
      throw new AccountServiceException("Input accounts conflict with the accounts in local cache",
          AccountServiceErrorCode.ResourceConflict);
    }

    hasSucceeded = accountMetadataStore.updateAccounts(accounts);
    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    if (hasSucceeded) {
      logger.trace("Completed updating accounts, took time={} ms", timeForUpdate);
      accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
      // notify account changes after successfully update.
      if (notifier == null) {
        logger.warn("Notifier is not provided. Cannot notify other entities interested in account data change.");
      } else if (notifier.publish(ACCOUNT_METADATA_CHANGE_TOPIC, FULL_ACCOUNT_METADATA_CHANGE_MESSAGE)) {
        logger.trace("Successfully published message for account metadata change");
      } else {
        logger.error("Failed to send notification for account metadata change");
        accountServiceMetrics.notifyAccountDataChangeErrorCount.inc();
      }
    } else {
      logger.error("Failed updating accounts={}, took {} ms", accounts, timeForUpdate);
      accountServiceMetrics.updateAccountErrorCount.inc();
      throw new AccountServiceException("Failed updating accounts to Helix property store",
          AccountServiceErrorCode.InternalError);
    }
  }

  @Override
  public void close() {
    if (open.compareAndSet(true, false)) {
      maybeUnsubscribeChangeTopic();
      if (scheduler != null) {
        shutDownExecutorService(scheduler, config.updaterShutDownTimeoutMs, TimeUnit.MILLISECONDS);
      }
      helixStore.stop();
      if (notifier != null) {
        notifier.close();
      }
    }
  }

  /**
   * To be used to subscribe to a {@link Notifier} topic. Upon receiving a
   * {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE}, it will check for any account updates.
   * @param topic The topic.
   * @param message The message for the topic.
   */
  @Override
  protected void onAccountChangeMessage(String topic, String message) {
    if (!open.get()) {
      // take no action instead of throwing an exception to silence noisy log messages when a message is received while
      // closing the AccountService.
      return;
    }
    logger.trace("Start to process message={} for topic={}", message, topic);
    try {
      switch (message) {
        case FULL_ACCOUNT_METADATA_CHANGE_MESSAGE:
          logger.trace("Start processing message={} for topic={}", message, topic);
          fetchAndUpdateCache(true);
          logger.trace("Completed processing message={} for topic={}", message, topic);
          break;
        default:
          accountServiceMetrics.unrecognizedMessageErrorCount.inc();
          throw new RuntimeException("Could not understand message=" + message + " for topic=" + topic);
      }
    } catch (Exception e) {
      logger.error("Exception occurred when processing message={} for topic={}.", message, topic, e);
      accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
      accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void checkOpen() {
    if (!open.get()) {
      throw new IllegalStateException("AccountService is closed.");
    }
    if (config.useNewZNodePath && router.get() == null) {
      throw new IllegalStateException("Router not initialized.");
    }
  }

  /**
   * Return {@link AccountServiceMetrics}.
   * @return {@link AccountServiceMetrics}
   */
  AccountServiceMetrics getAccountServiceMetrics() {
    return accountServiceMetrics;
  }

  /**
   * Return {@link BackupFileManager}.
   * @return {@link BackupFileManager}
   */
  BackupFileManager getBackupFileManager() {
    return backupFileManager;
  }
}

