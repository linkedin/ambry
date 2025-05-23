/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replica.prioritization.disruption.factory;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replica.prioritization.disruption.DefaultDisruptionService;
import com.github.ambry.replica.prioritization.disruption.DisruptionService;

/**
 * Factory class for creating instances of {@link DisruptionService}.
 * This implementation returns a new instance of {@link DefaultDisruptionService}.
 */
public class DefaultDisruptionFactory implements DisruptionServiceFactory {

  private final VerifiableProperties verifiableProperties;
  private final String datacenterName;

  public DefaultDisruptionFactory(VerifiableProperties verifiableProperties, String datacenterName) {
    this.verifiableProperties = verifiableProperties;
    this.datacenterName = datacenterName;
  }

  @Override
  public DisruptionService getDisruptionService() {
    return new DefaultDisruptionService();
  }
}
