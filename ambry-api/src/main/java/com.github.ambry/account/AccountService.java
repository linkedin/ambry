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
import java.util.Collection;
import java.util.function.Consumer;


/**
 * <p>
 *   An {@code AccountService} is a component that can respond to queries for {@link Account} by id or name, and
 *   add/update {@link Account}s for future queries. The {@link Account}s under an {@code AccountService} cannot
 *   have duplicate ids or names, and <em>MUST</em> have their ids and names one-to-one mapped.
 * </p>
 * <p>
 *   Deleting an {@link Account} from {@code AccountService} is not currently supported.
 * </p>
 */
public interface AccountService extends Closeable {

  /**
   * Gets an {@link Account} by its id.
   * @param accountId The id of an {@link Account} to get.
   * @return The {@link Account} with the specified id. {@code null} if such an {@link Account} does not exist.
   */
  public Account getAccountById(short accountId);

  /**
   * Gets an {@link Account} by its name.
   * @param accountName The name of an {@link Account} to get. Cannot be {@code null}.
   * @return The {@link Account} with the specified name. {@code null} if such an {@link Account} does not exist.
   */
  public Account getAccountByName(String accountName);

  /**
   * <p>
   *   Makes update for a collection of {@link Account}s. The update operation can either succeed with all the
   *   {@link Account}s successfully updated, or fail with none of the {@link Account}s updated. Partial update
   *   will not happen.
   * </p>
   * <p>
   *   The caller needs to make sure the {@link Account}s to update do not have duplicate id, nor duplicate name,
   *   otherwise, the update operation will fail.
   * </p>
   * <p>
   *   When updating {@link Account}s, {@code AccountService} will check that there is no conflict between the
   *   {@link Account}s to update and the existing {@link Account}s. Two {@link Account}s can be conflicting with
   *   each other if they have different account Ids but the same account name. If there is any conflict, the
   *   update operation will fail for <em>ALL</em> the {@link Account}s to update. Below lists the possible cases
   *   when there is conflict.
   * </p>
   * <pre>
   * Existing account
   * AccountId     AccountName
   * 1             "a"
   * 2             "b"
   *
   * Account to update
   * Case   AccountId   AccountName   If Conflict    Treatment                    Conflict reason
   * A      1           "a"           no             replace existing record      N/A
   * B      1           "c"           no             replace existing record      N/A
   * C      3           "c"           no             add a new record             N/A
   * D      3           "a"           yes            fail update                  conflicts with existing name.
   * E      1           "b"           yes            fail update                  conflicts with existing name.
   * </pre>
   * @param accounts The collection of {@link Account}s to update. Cannot be {@code null}.
   * @return {@code true} indicates that the accounts have been successfully updated, {@code false} indicates the
   *                      operation has failed, and none of the account has been updated. This is an either succeed-all
   *                      or fail-all operation.
   */
  public boolean updateAccounts(Collection<Account> accounts);

  /**
   * Gets all the {@link Account}s in this {@code AccountService}. The {@link Account}s <em>MUST</em> have their
   * ids and names one-to-one mapped.
   * @return A collection of {@link Account}s.
   */
  public Collection<Account> getAllAccounts();

  /**
   * Adds a {@link Consumer} for newly created or updated {@link Account}s.
   * @param accountUpdateConsumer The {@link Consumer} to add. Cannot be {@code null}.
   * @return {@code true} if the specified {@link Consumer} was not previously added, {@code false} otherwise.
   */
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer);

  /**
   * Removes a previously-added {@link Consumer} from the {@link AccountService}.
   * @param accountUpdateConsumer The {@link Consumer} to remove. Cannot be {@code null}.
   * @return {@code true} if the {@link Consumer} exists and removed, {@code false} if the {@link Consumer} does not
   *          exist.
   */
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer);
}
