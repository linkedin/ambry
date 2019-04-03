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

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;


/**
 * Utility class for VCR tests.
 */
public class VcrTestUtil {

  /**
   * Create a {@link VcrServer}.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param cloudDestinationFactory the {@link CloudDestinationFactory} to use.
   * @return the created VCR server.
   */
  public static VcrServer createVcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, CloudDestinationFactory cloudDestinationFactory) {
    return new VcrServer(properties, clusterAgentsFactory, notificationSystem, cloudDestinationFactory);
  }
}
