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
package com.github.ambry.quota;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;


/**
 * {@link StorageQuotaService} is the component to handles storage quota for different {@link Account} and {@link Container}.
 * It keeps track of the storage usage of different {@link Container}s and decides to throttle the Frontend operations
 * based on the quota and the {@link QuotaMode}.
 * If you just want to keep track of the the usage without throttling the traffic, you can call {@link #setQuotaMode} to change
 * {@link QuotaMode} from {@link QuotaMode#Throttling} to {@link QuotaMode#Tracking}.
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
   * Return true if the given {@link QuotaOperation} should be throttled.
   * @param accountId The accountId of this operation.
   * @param containerId The containerId of this operation.
   * @param op The {@link QuotaOperation}.
   * @param size The size of this operation. eg, if the op is Upload, size if the size of the content.
   * @return True is the given {@link QuotaOperation} should be throttled.
   */
  boolean shouldThrottle(short accountId, short containerId, QuotaOperation op, long size);

  /**
   * Change the {@link StorageQuotaService}'s quotaMode to the given value. If the quotaMode is {@link QuotaMode#Tracking}, then {@link StorageQuotaService}
   * should never return true in {@link #shouldThrottle} method.
   * @param quotaMode The new value for {@link QuotaMode}.
   */
  void setQuotaMode(QuotaMode quotaMode);
}
