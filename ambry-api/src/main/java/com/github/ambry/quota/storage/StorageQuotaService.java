/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils.InternalKeys;


/**
 * {@link StorageQuotaService} is the component to handles storage quota for different {@link Account} and {@link Container}.
 * It keeps track of the storage usage of different {@link Container}s and decides to throttle the Frontend operations
 * based on the quota and the {@link QuotaMode}.
 * If you just want to keep track of the the usage without throttling the traffic, you can call {@link #setQuotaMode} to change
 * {@link QuotaMode} from {@link QuotaMode#THROTTLING} to {@link QuotaMode#TRACKING}.
 * TODO: add a new method to deal with deleted containers.
 */
public interface StorageQuotaService {

  /**
   * Start the {@link StorageQuotaService} with all the initialization logic.
   * @throws Exception
   */
  void start() throws Exception;

  /**
   * Shutdown the {@link StorageQuotaService}.
   */
  void shutdown();

  /**
   * Return true if the given {@link RestRequest} should be throttled. Since the {@link StorageQuotaService} decide to
   * throttle a request based on the account id and container id, the {@code restRequest} has to carry {@link Account}
   * and {@link Container} by header {@link InternalKeys#TARGET_ACCOUNT_KEY} and {@link InternalKeys#TARGET_CONTAINER_KEY}.
   * @param restRequest The {@link RestRequest} from client.
   * @return True if the given {@link RestRequest} should be throttled.
   */
  boolean shouldThrottle(RestRequest restRequest);

  /**
   * Change the {@link StorageQuotaService}'s quotaMode to the given value. If the quotaMode is {@link QuotaMode#TRACKING}, then {@link StorageQuotaService}
   * should never return true in {@link #shouldThrottle} method.
   * @param quotaMode The new value for {@link QuotaMode}.
   */
  void setQuotaMode(QuotaMode quotaMode);
}
