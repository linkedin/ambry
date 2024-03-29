/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

/**
 * A factory for creating instances of {@link CloudDestination}.
 */
public interface CloudDestinationFactory {

  /**
   * @return an instance of {@link CloudDestination} generated by this factory.
   * @throws IllegalStateException if the {@link CloudDestination} instance cannot be created.
   */
  CloudDestination getCloudDestination() throws IllegalStateException, ReflectiveOperationException;
}
