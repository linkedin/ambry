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

import com.github.ambry.config.QuotaConfig;
import java.util.List;


/**
 * Factory to instantiate {@link AmbryQuotaManager} class.
 */
public class AmbryQuotaManagerFactory implements QuotaManagerFactory {
  private final QuotaManager quotaManager;

  /**
   * @param quotaConfig {@link QuotaConfig} object.
   * @param addedRequestQuotaEnforcers {@link List} of {@link RequestQuotaEnforcer}s to inject to {@link QuotaManager}. These will be
   *                                        those {@link RequestQuotaEnforcer} classes that cannot be created by config.
   * @param addedHostQuotaEnforcers {@link List} of {@link HostQuotaEnforcer}s to inject to {@link QuotaManager}. These will be
   *                                        those {@link HostQuotaEnforcer} classes that cannot be created by config.
   * @throws ReflectiveOperationException
   */
  public AmbryQuotaManagerFactory(QuotaConfig quotaConfig, List<RequestQuotaEnforcer> addedRequestQuotaEnforcers,
      List<HostQuotaEnforcer> addedHostQuotaEnforcers) throws ReflectiveOperationException {
    quotaManager = new AmbryQuotaManager(quotaConfig, addedHostQuotaEnforcers, addedRequestQuotaEnforcers);
  }

  @Override
  public QuotaManager getQuotaManager() {
    return quotaManager;
  }
}
