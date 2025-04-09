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

import com.github.ambry.replica.prioritization.disruption.DisruptionService;


/**
 * Factory interface for creating instances of {@link DisruptionService}.
 * This interface allows for the creation of different implementations of
 * {@link DisruptionService} based on the provided configuration.
 */
public interface DisruptionServiceFactory {

  /**
   * @return an instance of {@link DisruptionService}.
   */
  DisruptionService getDisruptionService();
}
