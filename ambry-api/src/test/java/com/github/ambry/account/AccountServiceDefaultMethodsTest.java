/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for the default methods on {@link AccountService}: {@link AccountService#getAccountCount()},
 * {@link AccountService#getContainerCount()}, and {@link AccountService#failIfCacheIsEmpty()}.
 */
public class AccountServiceDefaultMethodsTest {

  /**
   * Empty service → zero counts.
   */
  @Test
  public void emptyServiceReturnsZeroCounts() {
    AccountService service = new FakeAccountService();
    assertEquals(0, service.getAccountCount());
    assertEquals(0, service.getContainerCount());
  }

  /**
   * Populated service → correct counts.
   */
  @Test
  public void populatedServiceReturnsCorrectCounts() {
    FakeAccountService service = new FakeAccountService();
    Container c1 = new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build();
    Container c2 = new ContainerBuilder((short) 2, "c2", Container.ContainerStatus.ACTIVE, "c2", (short) 1).build();
    Container c3 = new ContainerBuilder((short) 1, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 2).build();
    service.addAccount(
        new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).containers(Arrays.asList(c1, c2)).build());
    service.addAccount(
        new AccountBuilder((short) 2, "a2", Account.AccountStatus.ACTIVE).containers(Collections.singleton(c3)).build());

    assertEquals("Expected 2 accounts", 2, service.getAccountCount());
    assertEquals("Expected 3 containers total", 3, service.getContainerCount());
  }

  /**
   * Empty service → failIfCacheIsEmpty() throws.
   */
  @Test
  public void failIfCacheIsEmptyThrowsWhenEmpty() {
    AccountService service = new FakeAccountService();
    try {
      service.failIfCacheIsEmpty();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should mention accounts=0", e.getMessage().contains("accounts=0"));
      assertTrue("Message should mention containers=0", e.getMessage().contains("containers=0"));
    }
  }

  /**
   * Accounts but no containers → failIfCacheIsEmpty() throws.
   */
  @Test
  public void failIfCacheIsEmptyThrowsWhenAccountsPresentButNoContainers() {
    FakeAccountService service = new FakeAccountService();
    service.addAccount(new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).build());
    assertEquals(1, service.getAccountCount());
    assertEquals(0, service.getContainerCount());
    try {
      service.failIfCacheIsEmpty();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should mention containers=0", e.getMessage().contains("containers=0"));
    }
  }

  /**
   * Fully populated cache → failIfCacheIsEmpty() does not throw.
   */
  @Test
  public void failIfCacheIsEmptyPassesWhenCachePopulated() {
    FakeAccountService service = new FakeAccountService();
    Container c = new ContainerBuilder((short) 1, "c", Container.ContainerStatus.ACTIVE, "c", (short) 1).build();
    service.addAccount(
        new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).containers(Collections.singleton(c)).build());
    service.failIfCacheIsEmpty();
  }

  /**
   * Minimal {@link AccountService} implementation that only supports the 6 abstract methods; the rest rely on
   * default implementations in the interface. Used to test the default methods under test.
   */
  private static final class FakeAccountService implements AccountService {
    private final Map<Short, Account> accounts = new HashMap<>();

    void addAccount(Account account) {
      accounts.put(account.getId(), account);
    }

    @Override
    public Account getAccountById(short accountId) {
      return accounts.get(accountId);
    }

    @Override
    public Account getAccountByName(String accountName) {
      return accounts.values().stream().filter(a -> a.getName().equals(accountName)).findFirst().orElse(null);
    }

    @Override
    public void updateAccounts(Collection<Account> toUpdate) {
      for (Account a : toUpdate) {
        accounts.put(a.getId(), a);
      }
    }

    @Override
    public Collection<Account> getAllAccounts() {
      return new ArrayList<>(accounts.values());
    }

    @Override
    public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
      return false;
    }

    @Override
    public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
      return false;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
