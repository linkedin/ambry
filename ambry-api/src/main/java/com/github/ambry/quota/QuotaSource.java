/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.rest.RestRequest;
import java.util.Collection;
import java.util.List;


/**
 * QuotaSource is the source of truth of quota for resources. It also tracks the usage of those resources. Resources could
 * be classified into two types: Ambry's Accounts, Containers or Services as represented by {@link QuotaResource}, or system
 * resources like a node's bandwidth.
 * Some implementations might not need to track any system resource. Check {@link QuotaEnforcer} for more details.
 */
public interface QuotaSource {

  /**
   * Method to initialize the {@link QuotaSource}.
   * @throws QuotaException in case of any exception.
   */
  void init() throws QuotaException;

  /**
   * @return {@code true} if the {@link QuotaSource} is initialized and ready to be used. {@code false} otherwise.
   */
  boolean isReady();

  /**
   * Get the {@link Quota} for specified resource and operation.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaName {@link QuotaName} object.
   * @throws QuotaException in case of any exception.
   */
  Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException;

  /**
   * Get the percent quota usage for the specified resource and operation.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaName {@link QuotaName} object.
   * @return usage of quota in percentage.
   * @throws QuotaException in case of any exception.
   */
  float getUsage(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException;

  /**
   * Charge the specified cost against quota for the specified {@link QuotaResource} and {@link QuotaName}.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaName {@link QuotaName} object.
   * @param usageCost cost to charge against quota.
   * @throws QuotaException in case of any exception.
   */
  void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) throws QuotaException;

  /**
   * Get the percent usage of system resources relevant to the {@link QuotaName} specified. The exact definition of
   * system resource is left for the implementation to determine.
   * Return {@literal -1} if the implementation doesn't need to track any system resource.
   * @param quotaName {@link QuotaName} object.
   * @return usage of relevant system resources in percentage.
   * @throws QuotaException in case of any exception.
   */
  float getSystemResourceUsage(QuotaName quotaName) throws QuotaException;

  /**
   * Charge the specified cost against system resources relevant to the {@link QuotaName} specified.
   * @param quotaName {@link QuotaName} object.
   * @param usageCost of relevant system resources.
   * @throws QuotaException in case of any exception.
   */
  void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) throws QuotaException;

  /**
   * Update the quota for newly created {@link List} of {@link QuotaResource}s.
   * @param accounts {@link List} of new created {@link QuotaResource}s.
   */
  void updateNewQuotaResources(Collection<Account> accounts);

  /**
   * Shutdown the {@link QuotaSource} and perform any cleanup.
   */
  void shutdown() throws Exception;
}
