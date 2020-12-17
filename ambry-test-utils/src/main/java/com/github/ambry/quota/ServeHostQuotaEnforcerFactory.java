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


public class ServeHostQuotaEnforcerFactory implements HostQuotaEnforcerFactory {
  private final ServeHostQuotaEnforcer serveQuotaEnforcer;

  public ServeHostQuotaEnforcerFactory(QuotaConfig quotaConfig) {
    this.serveQuotaEnforcer = new ServeHostQuotaEnforcer();
  }

  @Override
  public HostQuotaEnforcer getHostQuotaEnforcer() {
    return serveQuotaEnforcer;
  }
}
