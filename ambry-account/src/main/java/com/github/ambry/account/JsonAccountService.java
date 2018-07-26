/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.JsonAccountConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Account service which uses a local JSON file as source for accounts.
 *
 * This account implementation is primarily useful for small deployments and development. The service operates
 * completely in memory. Meaning that the entire JSON file is parsed and loaded in to memory.
 *
 * This implementation uses a thread to check for changes in the JSON file at scheduled intervals. While using a
 * {@link java.nio.file.WatchService} might seem more intuitive the docs of the WatchService specify that the
 * characteristics of the WatchService are highly platform (Linux, Windows, Solaris, etc.) dependent. Going as far as
 * specifying events may under certain conditions not be fired. Therefor having a fallback which at intervals checks if
 * the JSON file has changed will probably always be needed to ensure no changes are missed even if a WatchService
 * would be added.
 */
final class JsonAccountService extends AbstractAccountService {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Location of the account file which this service uses as it's source of accounts.*/
  private final Path accountFile;

  private final ScheduledExecutorService scheduler;

  private final AccountServiceMetrics accountServiceMetrics;

  private final JsonAccountConfig accountConfig;

  /**
   * Constructs an {@code JsonAccountService}. {@code #init} method MUST be called after construction.
   *
   * @param accountFile The path to the JSON file containing all the accounts.
   * @param accountServiceMetrics Metrics instance to log metrics to.
   * @param scheduler Scheduler used to periodically check the local account JSON for changes. When {@code null}
   *                  accounts file will only be loaded once during startup.
   */
  JsonAccountService(Path accountFile, AccountServiceMetrics accountServiceMetrics, ScheduledExecutorService scheduler,
      JsonAccountConfig accountConfig) {
    super(accountServiceMetrics);

    this.accountFile = Objects.requireNonNull(accountFile, "helixStore cannot be null");
    this.accountServiceMetrics = Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null");
    this.accountConfig = Objects.requireNonNull(accountConfig, "accountConfig cannot be null");
    this.scheduler = scheduler;

    logger.info("Starting JSON file account service with file '{}'", accountFile.toString());
  }

  /** Guards against calling {@link #init} multiple times.*/
  private final AtomicBoolean initialized = new AtomicBoolean();

  /**
   * Initializes this instance. Does all work which requires handing out references to this instance itself which can't
   * be done during normal instance construction without letting a reference to an incomplete constructed instance escape.
   */
  void init() {
    if (!initialized.getAndSet(true)) {
      if (scheduler != null) {
        Runnable updater = () -> {
          try {
            processAccountJsonFile();
          } catch (Exception e) {
            logger.error("Exception occurred when loading JSON account data", e);
            accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
          }
        };

        int initialDelay = new Random().nextInt(accountConfig.updaterPollingIntervalMs + 1);
        scheduler.scheduleAtFixedRate(updater, initialDelay, accountConfig.updaterPollingIntervalMs,
            TimeUnit.MILLISECONDS);
        logger.info(
            "Background account updater will fetch accounts from remote starting {} ms from now and repeat with interval={} ms",
            initialDelay, accountConfig.updaterPollingIntervalMs);
      } else {
        processAccountJsonFile();
      }
    }
  }

  @Override
  protected void checkOpen() {
    if (!initialized.get()) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }

  /**
   * Since the JSON file account service gets all it's data from a local JSON file it can't update account data
   * for all nodes. Therefor calling this method will always throw an {@link UnsupportedOperationException}.
   *
   * @param accounts The collection of {@link Account}s to update. Cannot be {@code null}.
   * @return
   * @throws UnsupportedOperationException Always thrown when this method is invoked.
   */
  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    throw new UnsupportedOperationException(
        "Service does not support updating of accountsById via API. Modify account JSON files on all nodes instead.");
  }

  /**
   * Makes the service release all its resources. Be aware that the service is no longer usable after this. This
   * instance can't be restarted and will need to be re-instantiated.
   */
  @Override
  public void close() throws IOException {
    if (watchAccountFileThread != null && !watchAccountFileThread.isInterrupted()) {
      watchAccountFileThread.interrupt();
    }
  }

  /** How often we check if the JSON account file has been modified in milliseconds. */
  private static int fileWatchIntervalInMs = 10 * 1000;
  /** Reference to the thread which checks the account file for changes at regular intervals. Will stop gracefully
   * when an interrupt is send to it.*/
  private volatile Thread watchAccountFileThread;

  /**
   * Updates the {@link #accountInfoMapRef} based on the supplied JSON data.
   *
   * @param accountsJsonString String with JSON data containing all accounts.
   */
  private void updateAccountsMap(String accountsJsonString) {
    lock.lock();

    try {
      AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountsJsonString);
      AccountInfoMap oldAccountInfoMap = accountInfoMapRef.getAndSet(newAccountInfoMap);
      notifyAccountUpdateConsumers(newAccountInfoMap, oldAccountInfoMap, false);
    } finally {
      lock.unlock();
    }

    logger.debug("Finished updating account maps");
  }

  /** Last known modification date of the account JSON file. Guarded by {@link #processAccountJsonLock}. */
  private volatile FileTime accountFilePreviousModified = null;
  /** Unix epoch timestamp in milliseconds when we started processing the account JSON file.
   * Guarded by {@link #processAccountJsonLock}. */
  private volatile long accountJsonProcessingStartTimeMs = -1;
  /**  */
  private final ReentrantLock processAccountJsonLock = new ReentrantLock();

  /**
   * Triggers processing of changed accounts if the accounts JSON file if it has changed. Never throws an exception.
   *
   * Method is allowed to be called to trigger manual account file processing even if the JSON modification thread
   * is running. Method is thread-safe. Blocks until processing is fully completed.
   */
  public void processAccountJsonFile() {
    processAccountJsonLock.lock();

    try {
      accountJsonProcessingStartTimeMs = System.currentTimeMillis();

      logger.trace("Checking if JSON account file has changed.");

      if (!Files.exists(accountFile)) {
        logger.warn("Unable to open JSON account file '{}'", accountFile.toString());
        return;
      }

      FileTime accountFileLastModified = Files.getLastModifiedTime(accountFile);
      if (accountFilePreviousModified == null
          || accountFilePreviousModified.compareTo(accountFileLastModified) < 0) {
        logger.debug("JSON Account file has changed or has never been loaded before. "
                + "Previous last know modified time: {}, new last known modified time: {}", accountFilePreviousModified,
            accountFileLastModified);

        String accountsJsonString = new String(Files.readAllBytes(accountFile), StandardCharsets.UTF_8);
        updateAccountsMap(accountsJsonString);

        accountFilePreviousModified = accountFileLastModified;
      }

      long timeForUpdate = System.currentTimeMillis() - accountJsonProcessingStartTimeMs;
      logger.trace("Completed updating accounts, took {} ms", timeForUpdate);
      accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
    } catch (Exception e) {
      long timeForUpdate = System.currentTimeMillis() - accountJsonProcessingStartTimeMs;
      logger.error("Failed updating accounts, took {} ms\"", timeForUpdate, e);
      accountServiceMetrics.updateAccountErrorCount.inc();
    } finally {
      processAccountJsonLock.unlock();
    }
  }
}
