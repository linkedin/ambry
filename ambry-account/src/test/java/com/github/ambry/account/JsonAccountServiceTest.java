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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.JsonAccountConfig;
import com.github.ambry.config.VerifiableProperties;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link JsonAccountService}.
 */
public class JsonAccountServiceTest {

  private final JsonAccountConfig accountConfig;

  private JsonAccountService accountService;
  /** Default seed used to get reproducible randomness.*/
  private final long DEFAULT_SEED_1 = 81923819321824L;
  /** Additional seed used to get a different result from the {@link #DEFAULT_SEED_1}*/
  private final long DEFAULT_SEED_2 = 2917331828473423431L;

  /** Root of the virtual filesystem to use for testing */
  private FileSystem ambryFs;
  /** Directory in {@code ambryFs} where Ambry's config files live */
  private Path configPath;
  /** Path in {@code ambryFs} to the JSON file containing the accounts. */
  private Path accountJsonFilePath;

  public JsonAccountServiceTest() {
    accountConfig = new JsonAccountConfig(new VerifiableProperties(new Properties()));
  }

  @Before
  public void initialize() throws Exception {
    ambryFs = Jimfs.newFileSystem(Configuration.unix());

    configPath = ambryFs.getPath("/config");
    Files.createDirectories(configPath);

    accountJsonFilePath = configPath.resolve("accounts.json");

    accountService = new JsonAccountService(accountJsonFilePath, new AccountServiceMetrics(new MetricRegistry()), null,
        accountConfig);
    accountService.init();
  }

  @After
  public void cleanUp() throws Exception {
    if (accountService != null) {
      accountService.close();
    }
  }

  /**
   * Generates a account JSON file at {@link #accountJsonFilePath}.
   *
   * @param noAccounts Number of accounts to create in the account JSON file.
   * @param accountIdStart The id to use to start creating accounts.
   * @param randomSeed Seed to use the get "reproducible randomness".
   *
   * @return The accounts created in the account JSON file.
   * @throws IOException Thrown if we can't write to the JSON account file.
   */
  private Collection<Account> generateAccountJsonFile(short noAccounts, short accountIdStart, long randomSeed)
      throws IOException {
    if (noAccounts + accountIdStart > Short.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Account start id plus number of accounts must not be greater then " + Short.MAX_VALUE);
    }

    Random pseudoRandom = new Random(randomSeed);

    JSONArray accountsJsonRoot = new JSONArray();
    // Using a set since caller will probably use this to verify it's own results. So will be doing mostly random
    // lookups.
    Set<Account> generatedAccounts = new HashSet<>(noAccounts);

    for (short x = accountIdStart; x < accountIdStart + noAccounts; x++) {
      Account generatedAccount =
          new AccountBuilder(x, randomString(pseudoRandom, 16), Account.AccountStatus.ACTIVE).build();

      generatedAccounts.add(generatedAccount);
      accountsJsonRoot.put(generatedAccount.toJson(false));
    }

    Files.write(accountJsonFilePath, Collections.singleton(accountsJsonRoot.toString(4)), StandardCharsets.UTF_8);

    return generatedAccounts;
  }

  private static final String randomCharSource = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  /**
   * Generates a string with random alpha numberic characters of specified length.
   *
   * @param random Source of randomness.
   * @param length Length of the String to generate.
   * @return
   */
  private String randomString(Random random, int length) {
    StringBuilder randomString = new StringBuilder(length);

    for (int x = 0; x < length; x++) {
      randomString.append(randomCharSource.charAt(random.nextInt(randomCharSource.length())));
    }

    return randomString.toString();
  }

  /**
   * Test if the service doesn't return results for accounts that don't exist.
   */
  @Test
  public void testNonExistingAccounts() throws Exception {
    Collection<Account> generatedAccounts = generateAccountJsonFile((short) 100, (short) 0, DEFAULT_SEED_1);
    accountService.processAccountJsonFile();

    verifyNonExistingAccounts(generatedAccounts);
  }

  /**
   * Test if the service doesn't return results for accounts that don't exist after changing the accounts JSON file
   * multiple times. Tests if there aren't any left over accounts between JSON changes which should have been removed
   * from the account JSON service.
   */
  @Test
  public void testNonExistingAccountsAfterMultipleChanges() throws Exception {
    for (short x = 0; x < 10; x++) {
      Collection<Account> generatedAccounts = generateAccountJsonFile((short) 100, (short) (x + 5), DEFAULT_SEED_1);
      accountService.processAccountJsonFile();

      verifyNonExistingAccounts(generatedAccounts);
    }
  }

  /**
   * Verify all possible accounts (account id's between 0 and {@link Short.MAX_VALUE} don't exist within the account
   * service.
   *
   * @param allowedAccounts Account which are allowed to exist in the account service.
   */
  private void verifyNonExistingAccounts(Collection<Account> allowedAccounts) {
    for (short x = 0; x < Short.MAX_VALUE; x++) {
      Account retrievedAccount = accountService.getAccountById(x);

      if (retrievedAccount != null) {
        if (!allowedAccounts.contains(retrievedAccount)) {
          fail("Service returned account while we did not expect it to return an account.");
        } else {
          // Ensures the account can't be found twice.
          allowedAccounts.remove(retrievedAccount);
        }
      }
    }
  }

  /**
   * Test if the service returns the right accounts by id and name.
   */
  @Test
  public void testAccountRetrieving() throws Exception {
    Collection<Account> generatedAccounts = generateAccountJsonFile((short) 100, (short) 0, DEFAULT_SEED_1);
    accountService.processAccountJsonFile();

    for (Account generatedAccount : generatedAccounts) {
      Account accountByName = accountService.getAccountByName(generatedAccount.getName());
      Account accountById = accountService.getAccountById(generatedAccount.getId());

      assertEquals("Retrieving account from service by name returned unexpected account.", generatedAccount,
          accountByName);
      assertEquals("Retrieving account from service by id returned unexpected account.", generatedAccount, accountById);
    }
  }

  /**
   * Tests accounts update. Since the JSON account service doesn't support this we check if the proper exception
   * is thrown.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testUpdateAccounts() throws Exception {
    accountService.updateAccounts(Collections.emptyList());
  }

  @Test
  public void testAllAccountRetrieving() throws Exception {
    Collection<Account> generatedAccounts = generateAccountJsonFile((short) 100, (short) 0, DEFAULT_SEED_1);
    accountService.processAccountJsonFile();

    Collection<Account> retrievedAccounts = new HashSet<>(accountService.getAllAccounts());

    for (Account generatedAccount : generatedAccounts) {
      if (!retrievedAccounts.contains(generatedAccount)) {
        fail("Account not present which we expected to be present.");
      } else {
        // Ensures the account can't be found twice.
        retrievedAccounts.remove(generatedAccount);
      }
    }
  }

  /**
   * Test if we receive notifications for newly created accounts.
   */
  @Test
  public void testAccountAddNotification() throws Exception {
    Collection<Account> addedAccounts = generateAccountJsonFile((short) 100, (short) 0, DEFAULT_SEED_1);

    Set<Account> notificationsReceived = new HashSet<>();
    accountService.addAccountUpdateConsumer(accounts -> notificationsReceived.addAll(accounts));

    accountService.processAccountJsonFile();

    assertEquals("Did not receive expected account added notifications.", notificationsReceived, addedAccounts);
  }

  /**
   * Test if we receive notifications for modified accounts.
   */
  @Test
  public void testAccountModifyNotification() throws Exception {
    // Load initial accounts
    generateAccountJsonFile((short) 100, (short) 0, DEFAULT_SEED_1);
    accountService.processAccountJsonFile();

    // Register listeners
    Set<Account> notificationsReceived = new HashSet<>();
    accountService.addAccountUpdateConsumer(accounts -> notificationsReceived.addAll(accounts));

    // Modify some accounts
    Collection<Account> modifiedAccounts = generateAccountJsonFile((short) 25, (short) 25, DEFAULT_SEED_2);
    accountService.processAccountJsonFile();

    // Verify we received the notifications we expected to.
    assertEquals("Received account modification notifications not as expected.", modifiedAccounts,
        notificationsReceived);
  }
}
