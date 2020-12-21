/**
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


/**
 * {@link HostQuotaEnforcerFactory} implementation for test that instantiates {@link RejectHostQuotaEnforcer}.
 */
public class RejectHostQuotaEnforcerFactory implements HostQuotaEnforcerFactory {
  private final RejectHostQuotaEnforcer rejectQuotaEnforcer;

  /**
   * Constructor for {@link RejectHostQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   */
  public RejectHostQuotaEnforcerFactory(QuotaConfig quotaConfig) {
    this.rejectQuotaEnforcer = new RejectHostQuotaEnforcer();
  }

  @Override
  public HostQuotaEnforcer getHostQuotaEnforcer() {
    return rejectQuotaEnforcer;
  }
}
