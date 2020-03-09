/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;


/**
 * Common test helper methods.
 */
public class CommonTestUtils {
  /**
   * Get the current active blob id version via {@link RouterConfig}
   * @return the current active blob id version.
   */
  public static short getCurrentBlobIdVersion() {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "localDC");
    return new RouterConfig(new VerifiableProperties(props)).routerBlobidCurrentVersion;
  }

  /**
   * Set dummy values for the required {@link RouterConfig} properties for use in tests.
   * @param props the {@link Properties} to set the required configs in.
   */
  public static void populateRequiredRouterProps(Properties props) {
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "localDC");
  }
}
