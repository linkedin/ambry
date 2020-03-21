/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * Factory for {@link AmbryIdSigningService}.
 */
public class AmbryIdSigningServiceFactory implements IdSigningServiceFactory {

  /**
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   */
  public AmbryIdSigningServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
  }

  @Override
  public IdSigningService getIdSigningService() {
    return new AmbryIdSigningService();
  }
}
