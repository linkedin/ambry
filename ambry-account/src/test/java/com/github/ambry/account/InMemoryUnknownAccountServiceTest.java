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

import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.util.Collections;
import java.util.Random;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link InMemoryUnknownAccountService} and {@link InMemoryUnknownAccountServiceFactory}.
 */
public class InMemoryUnknownAccountServiceTest {
  private static final Random random = new Random();
  private AccountService accountService;

  @After
  public void close() throws Exception {
    accountService.close();
  }

  @Test
  public void testAllMethods() throws Exception {
    accountService = new InMemoryUnknownAccountServiceFactory(null, null, null).getAccountService();
    assertEquals("Wrong account", Account.UNKNOWN_ACCOUNT, accountService.getAccountById(Utils.getRandomShort(random)));
    assertEquals("Wrong account", Account.UNKNOWN_ACCOUNT, accountService.getAccountById((short) -1));
    assertEquals("Wrong account", Account.UNKNOWN_ACCOUNT,
        accountService.getAccountByName(UtilsTest.getRandomString(10)));
    assertEquals("Wrong size of account collection", 1, accountService.getAllAccounts().size());
    // updating the InMemoryUnknownAccountService should fail.
    Account account = new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE, null).build();
    assertFalse("Wrong return value from an unsuccessful update operation",
        accountService.updateAccounts(Collections.singletonList(account)));
    assertEquals("Wrong size of account collection", 1, accountService.getAllAccounts().size());
    accountService.close();
  }

  /**
   * Tests {@code null} inputs.
   */
  @Test
  public void testNullInputs() {
    accountService = new InMemoryUnknownAccountServiceFactory(null, null, null).getAccountService();
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
}
