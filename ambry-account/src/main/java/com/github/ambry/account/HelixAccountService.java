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
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.AccountUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * <p>
 *   An implementation of {@link AccountService} that employs a {@link HelixPropertyStore} as its underlying storage.
 *   It's in the middle of transitioning from using old helix path to the new path. In old way, it internally stores
 *   the full set of {@link Account} metadata in a store-relative path defined in {@link LegacyMetadataStore}. In the
 *   new way, it internally stores the list of blob ids that point to different versions of {@link Account} metadata.
 *   In both way, the latest full {@link Account} metadata will be cached locally, so serving an {@link Account} query
 *   will not incur any calls to remote {@code ZooKeeper} or {@code AmbryServer}.
 * </p>
 * <p>
 *   After transition, this implementation would not only save the latest set of {@link Account} metadata, but also save
 *   several previous versions. It keeps a list of blob ids in {@link HelixPropertyStore} and remove the earliest one
 *   when it reaches the limit of the version numbers. There are several benefits from this approach.
 *   <ul>
 *     <li>Reverting changes would be much easier.</li>
 *     <li>There will no be size limit for {@link Account} metadata</li>
 *   </ul>
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
 *   The full set of the {@link Account} metadata are stored in a single {@link ZNRecord} or a single blob in the new way, as a
 *   simple map from a string account id to the {@link Account} content in json as a string.
 * </p>
 * <p>
 *   Limited by {@link HelixPropertyStore}, the total size of {@link Account} data stored on a single {@link ZNRecord}
 *   cannot exceed 1MB before the transition.
 * </p>
 */
public class HelixAccountService extends AbstractAccountService implements AccountService {
  static final String ACCOUNT_METADATA_CHANGE_TOPIC = "account_metadata_change_topic";
  static final String FULL_ACCOUNT_METADATA_CHANGE_MESSAGE = "full_account_metadata_change";

  protected final AtomicBoolean open = new AtomicBoolean(true);
  private final BackupFileManager backupFileManager;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final Notifier<String> notifier;
  private final CopyOnWriteArraySet<Consumer<Collection<Account>>> accountUpdateConsumers = new CopyOnWriteArraySet<>();
  private final ScheduledExecutorService scheduler;
  private final HelixAccountServiceConfig config;
  private final TopicListener<String> changeTopicListener = this::onAccountChangeMessage;
  private final AccountMetadataStore accountMetadataStore;
  private static final Logger logger = LoggerFactory.getLogger(HelixAccountService.class);

  private final AtomicReference<Router> router = new AtomicReference<>();
  private final AccountMetadataStore backFillStore;

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
    super(Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null"));
    this.helixStore = Objects.requireNonNull(helixStore, "helixStore cannot be null");
    this.notifier = notifier;
    this.scheduler = scheduler;
    this.config = config;
    this.backupFileManager = new BackupFileManager(this.accountServiceMetrics, config);
    AccountMetadataStore backFillStore = null;
    if (config.useNewZNodePath) {
      accountMetadataStore = new RouterStore(this.accountServiceMetrics, backupFileManager, helixStore, router, false,
              config.totalNumberOfVersionToKeep);
      // postpone initializeFetchAndSchedule to setupRouter function.
    } else {
      accountMetadataStore = new LegacyMetadataStore(this.accountServiceMetrics, backupFileManager, helixStore);
      initialFetchAndSchedule();
      if (config.backFillAccountsToNewZNode) {
        backFillStore = new RouterStore(this.accountServiceMetrics, backupFileManager, helixStore, router, true,
                config.totalNumberOfVersionToKeep);
      }
    }
    this.backFillStore = backFillStore;
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
   * Return the {@link Notifier}.
   * @return The {@link Notifier}
   */
  Notifier<String> getNotifier() {
    return notifier;
  }

  /**
   * A synchronized function to fetch account metadata from {@link AccountMetadataStore} and update the in memory cache.
   * @param isCalledFromListener True is this function is invoked in the {@link TopicListener}.
   */
  private synchronized void fetchAndUpdateCache(boolean isCalledFromListener) {
    Map<String, String> accountMap = accountMetadataStore.fetchAccountMetadata();
    if (accountMap == null) {
      logger.debug("No account map returned");
    } else {
      logger.trace("Start parsing remote account data");
      AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
      AccountInfoMap oldAccountInfoMap = accountInfoMapRef.getAndSet(newAccountInfoMap);
      notifyAccountUpdateConsumers(newAccountInfoMap, oldAccountInfoMap, isCalledFromListener);
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
    // backup.getLatestAccountMap should return null. And in case we have a very old backup file just mentioned above, a threshold
    // would solve the problem.
    if (accountInfoMapRef.get().isEmpty() && config.enableServeFromBackup && !backupFileManager.isEmpty()) {
      long aMonthAgo = System.currentTimeMillis() / 1000 - TimeUnit.DAYS.toSeconds(30);
      Map<String, String> accountMap = backupFileManager.getLatestAccountMap(aMonthAgo);
      if (accountMap != null) {
        AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
        AccountInfoMap oldAccountInfoMap = accountInfoMapRef.getAndSet(newAccountInfoMap);
        notifyAccountUpdateConsumers(newAccountInfoMap, oldAccountInfoMap, false);
      }
    }

    if (scheduler != null) {
      int initialDelay = new Random().nextInt(config.updaterPollingIntervalMs + 1);
      scheduler.scheduleAtFixedRate(updater, initialDelay, config.updaterPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info(
          "Background account updater will fetch accounts from remote starting {} ms from now and repeat with interval={} ms",
          initialDelay, config.updaterPollingIntervalMs);
    }

    if (notifier != null) {
      notifier.subscribe(ACCOUNT_METADATA_CHANGE_TOPIC, changeTopicListener);
    } else {
      logger.warn("Notifier is null. Account updates cannot be notified to other entities. Local account cache may not "
          + "be in sync with remote account data.");
      accountServiceMetrics.nullNotifierCount.inc();
    }
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
  public boolean updateAccounts(Collection<Account> accounts) {
    return updateAccountsWithAccountMetadataStore(accounts, accountMetadataStore);
  }

  /**
   * Helper function to update {@link Account} metadata.
   * @param accounts The {@link Account} metadata to update.
   * @param accountMetadataStore The {@link AccountMetadataStore}.
   * @return True when the update operation succeeds.
   */
  boolean updateAccountsWithAccountMetadataStore(Collection<Account> accounts, AccountMetadataStore accountMetadataStore) {
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      logger.debug("Empty account collection to update.");
      return false;
    }
    if (config.updateDisabled) {
      logger.info("Updates has been disabled");
      return false;
    }
    if (hasDuplicateAccountIdOrName(accounts)) {
      logger.debug("Duplicate account id or name exist in the accounts to update");
      accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating to HelixPropertyStore with accounts={}", accounts);
    boolean hasSucceeded = false;
    // make a pre check for conflict between the accounts to update and the accounts in the local cache. Will fail this
    // update operation for all the accounts if any conflict exists. There is a slight chance that the account to update
    // conflicts with the accounts in the local cache, but does not conflict with those in the helixPropertyStore. This
    // will happen if some accounts are updated but the local cache is not refreshed.
    if (accountInfoMapRef.get().hasConflictingAccount(accounts)) {
      logger.debug("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
      accountServiceMetrics.updateAccountErrorCount.inc();
    } else {
      hasSucceeded = accountMetadataStore.updateAccounts(accounts);
    }
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
    }
    return hasSucceeded;
  }

  @Override
  public void close() {
    if (open.compareAndSet(true, false)) {
      if (notifier != null) {
        notifier.unsubscribe(ACCOUNT_METADATA_CHANGE_TOPIC, changeTopicListener);
      }
      if (scheduler != null) {
        shutDownExecutorService(scheduler, config.updaterShutDownTimeoutMs, TimeUnit.MILLISECONDS);
      }
      helixStore.stop();
    }
  }

  /**
   * To be used to subscribe to a {@link Notifier} topic. Upon receiving a
   * {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE}, it will check for any account updates.
   * @param topic The topic.
   * @param message The message for the topic.
   */
  private void onAccountChangeMessage(String topic, String message) {
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
          maybeBackFillToNewStore();
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
   * Backfill the newly updated {@link Account} metadata to new zookeeper node based on the configuration. This function
   * doesn't guarantee the success of the operation. It gives up whenever there is failure or exception and wait for
   * next update.
   */
  private void maybeBackFillToNewStore() {
    if (!config.backFillAccountsToNewZNode) {
      return;
    }
    logger.info("Starting backfilling the new state to new store");
    if (backFillStore.updateAccounts(accountInfoMapRef.get().getAccounts())) {
      logger.info("Finish backfilling the new state to new store");
    } else {
      logger.error("Fail to backfill the new state to new store, just skip this one");
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
    if ((config.useNewZNodePath || config.backFillAccountsToNewZNode) && router.get() == null) {
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

