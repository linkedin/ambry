/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 * An AccountMetadataStore is a storage to keep and retrieve {@link Account} metadata.
 */
interface AccountMetadataStore {

  /**
   * fetchAndUpdateCache would fetch the latest full set of {@link Account} metadata and update the cache
   * in the {@link HelixAccountService}.
   * @param isCalledFromListener is true when this function is invoked from the listener callback function.
   */
  void fetchAndUpdateCache(boolean isCalledFromListener);

  /**
   * updateAccounts updates the latest full set of {@link Account} metadata and save it in the storage.
   * @param accounts The {@link Account} collection to update.
   * @return false when there is any error.
   */
  boolean updateAccounts(Collection<Account> accounts);
}

