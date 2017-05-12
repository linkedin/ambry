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

import java.io.Closeable;
import java.util.List;


/**
 * An {@code AccountService} is a service that can respond to queries and update operations for {@link Account}.
 */
public interface AccountService extends Closeable {

  /**
   * Gets an {@link Account} instance by account name.
   * @param accountName The name of an {@link Account} to get.
   * @return The {@link Account} with the specified name. {@code null} if such an {@link Account} does not exist.
   */
  public Account getAccountByName(String accountName);

  /**
   * Gets an {@link Account} instance by account id.
   * @param accountId The id of an {@link Account} to get.
   * @return The {@link Account} with the specified id. {@code null} if such an {@link Account} does not exist.
   */
  public Account getAccountById(short accountId);

  /**
   * Makes update for a list of {@link Account}s. This will need to write through the updates all the way to the
   * backend storage, if {@code AccountService} relies on some backend storage.
   * @param accounts The list of {@link Account}s to be updated.
   * @return {@code true} if the operation is successful, {@code false} otherwise.
   */
  public boolean updateAccounts(List<Account> accounts);

  /**
   * Gets all the {@link Account}s managed by this {@code AccountService}.
   * @return A list of {@link Account}s.
   */
  public List<Account> getAllAccounts();

}
