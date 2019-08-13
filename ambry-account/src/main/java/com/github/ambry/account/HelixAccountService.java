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

import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Notifier;
import com.github.ambry.commons.ReadableStreamChannelInputStream;
import com.github.ambry.commons.TopicListener;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import com.google.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
class HelixAccountService implements AccountService {
  static final String ACCOUNT_METADATA_CHANGE_TOPIC = "account_metadata_change_topic";
  static final String FULL_ACCOUNT_METADATA_CHANGE_MESSAGE = "full_account_metadata_change";

  // backup constants
  static final String OLD_STATE_SUFFIX = "old";
  static final String NEW_STATE_SUFFIX = "new";
  static final String SEP = ".";
  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

  final Path backupDirPath;
  final AccountServiceMetrics accountServiceMetrics;

  private final HelixPropertyStore<ZNRecord> helixStore;
  private final Notifier<String> notifier;
  private final AtomicReference<AccountInfoMap> accountInfoMapRef = new AtomicReference<>(new AccountInfoMap(this));
  private final CopyOnWriteArraySet<Consumer<Collection<Account>>> accountUpdateConsumers = new CopyOnWriteArraySet<>();
  private final ScheduledExecutorService scheduler;
  private final HelixAccountServiceConfig config;
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final TopicListener<String> changeTopicListener = this::onAccountChangeMessage;
  private static final Logger logger = LoggerFactory.getLogger(HelixAccountService.class);
  private final AccountMetadataStore accountMetadataStore;

  AtomicReference<Router> router = new AtomicReference<>();

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
    this.helixStore = Objects.requireNonNull(helixStore, "helixStore cannot be null");
    this.accountServiceMetrics = Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null");
    this.notifier = notifier;
    this.scheduler = scheduler;
    this.config = config;

    backupDirPath = config.backupDir.isEmpty() ? null : Files.createDirectories(Paths.get(config.backupDir));
    if (notifier != null) {
      notifier.subscribe(ACCOUNT_METADATA_CHANGE_TOPIC, changeTopicListener);
    } else {
      logger.warn("Notifier is null. Account updates cannot be notified to other entities. Local account cache may not "
          + "be in sync with remote account data.");
      accountServiceMetrics.nullNotifierCount.inc();
    }
    if (config.useNewZNodePath) {
      accountMetadataStore = new RouterStore(this, helixStore, config);
      // postpone initializeFetchAndSchedule to setupRouter function.
    } else {
      accountMetadataStore = new LegacyMetadataStore(this, helixStore, config);
      initialFetchAndSchedule();
    }
  }

  /**
   * set the router to the given one.
   * @param router The router to set.
   * @throws IllegalStateException when the router already set up.
   */
  public void setupRouter(final Router router) throws IllegalStateException {
    if (!this.router.compareAndSet(null, router)) {
      throw new IllegalStateException("Router already initialized");
    } else {
      initialFetchAndSchedule();
    }
  }

  private void initialFetchAndSchedule() {
    Runnable updater = () -> {
      try {
        accountMetadataStore.fetchAndUpdateCache(false);
      } catch (Exception e) {
        logger.error("Exception occurred when fetching remote account data", e);
        accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
      }
    };
    updater.run();
    if (scheduler != null) {
      int initialDelay = new Random().nextInt(config.updaterPollingIntervalMs + 1);
      scheduler.scheduleAtFixedRate(updater, initialDelay, config.updaterPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info(
          "Background account updater will fetch accounts from remote starting {} ms from now and repeat with interval={} ms",
          initialDelay, config.updaterPollingIntervalMs);
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    checkOpen();
    Objects.requireNonNull(accountName, "accountName cannot be null.");
    return accountInfoMapRef.get().getAccountByName(accountName);
  }

  @Override
  public Account getAccountById(short id) {
    checkOpen();
    return accountInfoMapRef.get().getAccountById(id);
  }

  @Override
  public Collection<Account> getAllAccounts() {
    checkOpen();
    return accountInfoMapRef.get().getAccounts();
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to subscribe cannot be null");
    return accountUpdateConsumers.add(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to unsubscribe cannot be null");
    return accountUpdateConsumers.remove(accountUpdateConsumer);
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
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
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
    if (hasConflictWithCache(accounts)) {
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
          accountMetadataStore.fetchAndUpdateCache(true);
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
   * Logs and notifies account update {@link Consumer}s about any new account changes/creations.
   * @param newAccountInfoMap the new {@link AccountInfoMap} that has been set.
   * @param oldAccountInfoMap the {@link AccountInfoMap} that was cached before this change.
   * @param isCalledFromListener {@code true} if the caller is the account update listener, {@@code false} otherwise.
   */
  void notifyAccountUpdateConsumers(AccountInfoMap newAccountInfoMap, AccountInfoMap oldAccountInfoMap,
      boolean isCalledFromListener) {
    Map<Short, Account> idToUpdatedAccounts = new HashMap<>();
    for (Account newAccount : newAccountInfoMap.getAccounts()) {
      if (!newAccount.equals(oldAccountInfoMap.getAccountById(newAccount.getId()))) {
        idToUpdatedAccounts.put(newAccount.getId(), newAccount);
      }
    }
    if (idToUpdatedAccounts.size() > 0) {
      logger.info("Received updates for {} accounts. Received from listener={}. Account IDs={}",
          idToUpdatedAccounts.size(), isCalledFromListener, idToUpdatedAccounts.keySet());
      // @todo In long run, this metric is not necessary.
      if (isCalledFromListener) {
        accountServiceMetrics.accountUpdatesCapturedByScheduledUpdaterCount.inc();
      }
      Collection<Account> updatedAccounts = Collections.unmodifiableCollection(idToUpdatedAccounts.values());
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        long startTime = System.currentTimeMillis();
        try {
          accountUpdateConsumer.accept(updatedAccounts);
          long consumerExecutionTimeInMs = System.currentTimeMillis() - startTime;
          logger.trace("Consumer={} has been notified for account change, took {} ms", accountUpdateConsumer,
              consumerExecutionTimeInMs);
          accountServiceMetrics.accountUpdateConsumerTimeInMs.update(consumerExecutionTimeInMs);
        } catch (Exception e) {
          logger.error("Exception occurred when notifying accountUpdateConsumer={}", accountUpdateConsumer, e);
        }
      }
    } else {
      logger.debug("HelixAccountService is updated with 0 updated account");
    }
  }

  /**
   * update the cache with the given {@link AccountInfoMap}.
   * @param newAccountInfoMap
   * @return the old {@link AccountInfoMap}
   */
  AccountInfoMap updateCache(AccountInfoMap newAccountInfoMap) {
    return accountInfoMapRef.getAndSet(newAccountInfoMap);
  }

  /**
   * Checks a collection of {@link Account}s if there is any conflict between the {@link Account}s to update and the
   * {@link Account}s in the local cache.
   *
   * @param accountsToSet The collection of {@link Account}s to check with the local cache.
   * @return {@code true} if there is at least one {@link Account} in {@code accountsToSet} conflicts with the
   *                      {@link Account}s in the cache, {@code false} otherwise.
   */
  private boolean hasConflictWithCache(Collection<Account> accountsToSet) {
    return hasConflictingAccount(accountsToSet, accountInfoMapRef.get());
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountInfoMap}, according to the Javadoc of {@link AccountService}. Two {@link Account}s can be
   * conflicting with each other if they have different account Ids but the same account name.
   *
   * @param accountsToSet The collection of {@link Account}s to check conflict.
   * @param accountInfoMap A {@link AccountInfoMap} that represents a group of {@link Account}s to check conflict.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  boolean hasConflictingAccount(Collection<Account> accountsToSet, AccountInfoMap accountInfoMap) {
    for (Account account : accountsToSet) {
      // if the account already exists, check that the snapshot version matches the expected value.
      Account accountInMap = accountInfoMap.getAccountById(account.getId());
      if (accountInMap != null && account.getSnapshotVersion() != accountInMap.getSnapshotVersion()) {
        logger.error(
            "Account to update (accountId={} accountName={}) has an unexpected snapshot version in zk (expected={}, encountered={})",
            account.getId(), account.getName(), account.getSnapshotVersion(), accountInMap.getSnapshotVersion());
        return true;
      }
      // check that there are no other accounts that conflict with the name of the account to update
      // (case D and E from the javadoc)
      Account potentialConflict = accountInfoMap.getAccountByName(account.getName());
      if (potentialConflict != null && potentialConflict.getId() != account.getId()) {
        logger.error(
            "Account to update (accountId={} accountName={}) conflicts with an existing record (accountId={} accountName={})",
            account.getId(), account.getName(), potentialConflict.getId(), potentialConflict.getName());
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the {@code HelixAccountService} is open.
   */
  private void checkOpen() {
    if (!open.get()) {
      throw new IllegalStateException("AccountService is closed.");
    }
    if (config.useNewZNodePath && router.get() == null) {
      throw new IllegalStateException("Router not initialized.");
    }
  }

  /**
   * Save the zookeeper state from after the update to disk. This will only save the file if the update succeeded
   * and the old state backup file was successfully reserved.
   * The following file name format will be used: {@code {yyyyMMdd}T{HHmmss}.{unique long}.new}.
   */
  void maybePersistNewState(Pair<String, Path> backupPrefixAndPath, Map<String, String> newState) {
    if (backupPrefixAndPath != null) {
      try {
        Path filepath = backupDirPath.resolve(backupPrefixAndPath.getFirst() + NEW_STATE_SUFFIX);
        writeBackup(filepath, newState);
      } catch (Exception e) {
        logger.error("Could not write new state backup file", e);
        accountServiceMetrics.backupErrorCount.inc();
      }
    }
  }

  /**
   * Save the zookeeper state from before the update to disk.
   * The following file name format will be used: {@code {yyyyMMdd}T{HHmmss}.{unique long}.old}.
   * If there are multiple files with the same timestamp, the unique long will prevent the file names from clashing.
   */
  void maybePersistOldState(Pair<String, Path> backupPrefixAndPath, Map<String, String> oldState) {
    if (backupPrefixAndPath != null) {
      try {
        writeBackup(backupPrefixAndPath.getSecond(), oldState);
      } catch (Exception e) {
        logger.error("Could not write previous state backup file", e);
        accountServiceMetrics.backupErrorCount.inc();
      }
    }
  }

  /**
   * If a backup file has not yet been created, reserve a new one with the following file name format:
   * {@code {yyyyMMdd}T{HHmmss}.{unique long}.old}.
   * @return a {@link Pair} containing the unique filename prefix for this account update and the path to use for
   *         previous state backups.
   * @throws IOException
   */
  Pair<String, Path> reserveBackupFile() throws IOException {
    String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
    for (long n = 0; n < Long.MAX_VALUE; n++) {
      String prefix = timestamp + SEP + n + SEP;
      Path filepath = backupDirPath.resolve(prefix + OLD_STATE_SUFFIX);
      try {
        return new Pair<>(prefix, Files.createFile(filepath));
      } catch (FileAlreadyExistsException e) {
        // retry with a new suffix.
      }
    }
    throw new IOException("Could not create a unique file with timestamp " + timestamp);
  }

  /**
   * @return a {@link JSONArray} containing the content for the local backup.
   * @throws JSONException
   */
  private void writeBackup(Path backupPath, Map<String, String> accountMap) throws IOException, JSONException {
    try (BufferedWriter writer = Files.newBufferedWriter(backupPath)) {
      String sep = "";
      writer.write('[');
      for (String accountString : accountMap.values()) {
        writer.write(sep);
        writer.write(accountString);
        sep = ",";
      }
      writer.write(']');
    }
  }
}

/**
 * <p>
 *   A helper class that represents a collection of {@link Account}s, where the ids and names of the
 *   {@link Account}s are one-to-one mapped. An {@code AccountInfoMap} guarantees no duplicated account
 *   id or name, nor conflict among the {@link Account}s within it.
 * </p>
 * <p>
 *   Based on the properties, a {@code AccountInfoMap} internally builds index for {@link Account}s using both
 *   {@link Account}'s id and name as key.
 * </p>
 */
class AccountInfoMap {
  private final Map<String, Account> nameToAccountMap;
  private final Map<Short, Account> idToAccountMap;
  private final HelixAccountService accountService;

  /**
   * Constructor for an empty {@code AccountInfoMap}.
   */
  AccountInfoMap(HelixAccountService accountService) {
    this.accountService = accountService;
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();
  }

  /**
   * <p>
   *   Constructs an {@code AccountInfoMap} from a group of {@link Account}s. The {@link Account}s exists
   *   in the form of a string-to-string map, where the key is the string form of an {@link Account}'s id,
   *   and the value is the string form of the {@link Account}'s JSON string.
   * </p>
   * <p>
   *   The source {@link Account}s in the {@code accountMap} may duplicate account ids or names, or corrupted
   *   JSON strings that cannot be parsed as valid {@link JSONObject}. In such cases, construction of
   *   {@code AccountInfoMap} will fail.
   * </p>
   * @param accountMap A map of {@link Account}s in the form of (accountIdString, accountJSONString).
   * @throws JSONException If parsing account data in json fails.
   */
  AccountInfoMap(HelixAccountService accountService, Map<String, String> accountMap) throws JSONException {
    this.accountService = accountService;
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();
    for (Map.Entry<String, String> entry : accountMap.entrySet()) {
      String idKey = entry.getKey();
      String valueString = entry.getValue();
      Account account;
      JSONObject accountJson = new JSONObject(valueString);
      if (idKey == null) {
        accountService.accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException(
            "Invalid account record when reading accountMap in ZNRecord because idKey=null");
      }
      account = Account.fromJson(accountJson);
      if (account.getId() != Short.valueOf(idKey)) {
        accountService.accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException(
            "Invalid account record when reading accountMap in ZNRecord because idKey and accountId do not match. idKey="
                + idKey + " accountId=" + account.getId());
      }
      if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
        throw new IllegalStateException(
            "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
      }
      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
  }

  /**
   * Gets {@link Account} by its id.
   * @param id The id to get the {@link Account}.
   * @return The {@link Account} with the given id, or {@code null} if such an {@link Account} does not exist.
   */
  Account getAccountById(Short id) {
    return idToAccountMap.get(id);
  }

  /**
   * Gets {@link Account} by its name.
   * @param name The id to get the {@link Account}.
   * @return The {@link Account} with the given name, or {@code null} if such an {@link Account} does not exist.
   */
  Account getAccountByName(String name) {
    return nameToAccountMap.get(name);
  }

  /**
   * Gets all the {@link Account}s in this {@code AccountInfoMap} in a {@link Collection}.
   * @return A {@link Collection} of all the {@link Account}s in this map.
   */
  Collection<Account> getAccounts() {
    return Collections.unmodifiableCollection(idToAccountMap.values());
  }
}

