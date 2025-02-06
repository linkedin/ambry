/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf.serverperf;

/**
 * Interface which needs to implemented for Load producer and consumer for
 * Server performance test
 */
public interface LoadProducerConsumer {
  /**
   * This will be called continuously until {@link ShutDownException} is thrown.
   * @throws ShutDownException shutdown exception
   * @throws Exception exception
   */
  void produce() throws Exception;

  /**
   * This will be called continuously until {@link ShutDownException} is thrown.
   * @throws ShutDownException shutdown exception
   * @throws Exception exception
   */
  void consume() throws Exception;
}
