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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link InMemoryUnknownAccountService} and {@link InMemoryUnknownAccountServiceFactory}.
 */
public class InMemoryUnknownAccountServiceTest {
  private static final Random random = new Random();
  private AccountService accountService = new InMemoryUnknownAccountServiceFactory(null, null).getAccountService();

  /**
   * Cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  @After
  public void cleanUp() throws Exception {
    if (accountService != null) {
      accountService.close();
    }
  }

  @Test
  public void testAllMethods() throws Exception {
    assertEquals("Wrong account", null, accountService.getAccountById(Utils.getRandomShort(random)));
    assertEquals("Wrong account", InMemoryUnknownAccountService.UNKNOWN_ACCOUNT,
        accountService.getAccountById((short) -1));
    assertEquals("Wrong account", InMemoryUnknownAccountService.UNKNOWN_ACCOUNT,
        accountService.getAccountByName(TestUtils.getRandomString(10)));
    assertEquals("Wrong size of account collection", 1, accountService.getAllAccounts().size());
    // updating the InMemoryUnknownAccountService should fail.
    Account account = new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build();
    assertFalse("Wrong return value from an unsuccessful update operation",
        accountService.updateAccounts(Collections.singletonList(account)));
    assertEquals("Wrong size of account collection", 1, accountService.getAllAccounts().size());
    try {
      accountService.getAllAccounts().add(account);
      fail("Should have thrown.");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    accountService.close();
  }

  /**
   * Tests {@code null} inputs.
   */
  @Test
  public void testNullInputs() {
    try {
      accountService.updateAccounts(null);
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
    try {
      accountService.getAccountByName(null);
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests adding/removing {@link Consumer}.
   */
  @Test
  public void testAddRemoveConsumer() {
    List<Collection<Account>> updatedAccountsReceivedByConsumers = new ArrayList<>();
    // add consumers
    Consumer<Collection<Account>> accountUpdateConsumer = updatedAccounts -> {
      updatedAccountsReceivedByConsumers.add(updatedAccounts);
    };
    accountService.addAccountUpdateConsumer(accountUpdateConsumer);
    Account updatedAccount = new AccountBuilder(InMemoryUnknownAccountService.UNKNOWN_ACCOUNT).name("newName").build();
    accountService.updateAccounts(Collections.singletonList(updatedAccount));
    assertEquals("Wrong number of updated accounts received by consumer.", 0,
        updatedAccountsReceivedByConsumers.size());
    Account newAccount = new AccountBuilder((short) 1, "newAccount", Account.AccountStatus.ACTIVE).build();
    accountService.updateAccounts(Collections.singletonList(newAccount));
    assertEquals("Wrong number of updated accounts received by consumer.", 0,
        updatedAccountsReceivedByConsumers.size());
    accountService.removeAccountUpdateConsumer(accountUpdateConsumer);
  }
}
