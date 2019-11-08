/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import java.io.IOException;


/**
 * A factory class used to get new instances of a {@link NetworkClient}
 */
public interface NetworkClientFactory {

  /**
   * Construct and return a new {@link NetworkClient}
   * @return return a new {@link NetworkClient}
   * @throws IOException if the {@link NetworkClient} could not be instantiated.
   */
  NetworkClient getNetworkClient() throws IOException;
}
