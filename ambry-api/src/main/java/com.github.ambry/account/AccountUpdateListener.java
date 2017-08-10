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

import java.util.Collection;


/**
 * A {@code Listener} can subscribe for {@link Account} creation and update through {@link AccountService}.
 */
public interface AccountUpdateListener {

  /**
   * After the {@code Listener} has subscribed the {@link Account} creation or update through {@link AccountService},
   * this method will be called when {@link AccountService} receives new or updated {@link Account}s.
   * @param updatedAccounts The {@link Account}s that have been added or updated.
   */
  public void onUpdate(Collection<Account> updatedAccounts);
}
