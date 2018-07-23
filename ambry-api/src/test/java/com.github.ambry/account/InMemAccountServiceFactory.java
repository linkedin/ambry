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

import com.github.ambry.config.VerifiableProperties;


/**
 * Factory to create {@link InMemAccountService}. The account services returned are static variables.
 */
public class InMemAccountServiceFactory implements AccountServiceFactory {
  private static final InMemAccountService ONLY_UNKNOWN = new InMemAccountService(true, false);
  private static final InMemAccountService ANY_ACCOUNT_NOTIFY = new InMemAccountService(false, true);
  private static final InMemAccountService ANY_ACCOUNT_NO_NOTIFY = new InMemAccountService(false, false);

  private final boolean returnOnlyUnknown;
  private final boolean notifyConsumers;

  /**
   * Constructor. If the properties contains a field "in.mem.account.service.only.unknown" set to {@code true}, an
   * {@link InMemAccountService} that only returns {@link InMemAccountService#UNKNOWN_ACCOUNT} is returned. Otherwise a fully
   * functional service is returned. These account services are also static (singleton) so the same instance of these
   * services is returned no matter how many times {@link #getAccountService()} is called or different instances of
   * {@link InMemAccountServiceFactory} are created.
   * <p/>
   * If "in.mem.account.service.only.unknown" is {@code false}, then notifications to consumers can be enabled/disabled
   * by setting the value of "in.mem.account.service.notify.consumers" appropriately. Once again, the instance returned
   * is static (singleton).
   * @param verifiableProperties The properties to get a {@link InMemAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry will be discarded
   * @param notifier will be discarded
   */
  public InMemAccountServiceFactory(VerifiableProperties verifiableProperties, Object metricRegistry) {
    returnOnlyUnknown = verifiableProperties.getBoolean("in.mem.account.service.only.unknown", false);
    notifyConsumers =
        !returnOnlyUnknown && verifiableProperties.getBoolean("in.mem.account.service.notify.consumers", true);
  }

  /**
   * Constructor. Each different configuration for these parameters has a singleton {@link AccountService}.
   * @param returnOnlyUnknown on {@link #getAccountService()}, returns an {@link AccountService} that will only return
   *                          {@link InMemAccountService#UNKNOWN_ACCOUNT}.
   *
   * @param notifyConsumers if {@code true}, will notify consumers when accounts are updated. This cannot be
   *                        {@code true} if {@code returnOnlyUnknown} is {@code true}.
   */
  public InMemAccountServiceFactory(boolean returnOnlyUnknown, boolean notifyConsumers) {
    this.returnOnlyUnknown = returnOnlyUnknown;
    this.notifyConsumers = notifyConsumers;
    if (returnOnlyUnknown && notifyConsumers) {
      throw new IllegalArgumentException(
          "Cannot have an account service that returns UNKNOWN only but notifies consumers on account changes");
    }
  }

  @Override
  public InMemAccountService getAccountService() {
    return returnOnlyUnknown ? ONLY_UNKNOWN : notifyConsumers ? ANY_ACCOUNT_NOTIFY : ANY_ACCOUNT_NO_NOTIFY;
  }
}

