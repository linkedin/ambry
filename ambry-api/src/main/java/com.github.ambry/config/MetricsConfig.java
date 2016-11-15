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
package com.github.ambry.config;

import java.util.ArrayList;
import java.util.List;


/**
 * The configs for metrics
 */
public class MetricsConfig {

  /**
   * The metrics reporter factory classes. This would be a comma separated list of metrics reporter factory
   * classes
   */
  @Config("metrics.reporters.factory.classes")
  @Default("com.github.ambry.metrics.JmxReporterFactory")
  public final String metricsFactoryClasses;

  /**
   * Returns a list of all metrics names from the config file. Useful for
   * getting individual metrics.
   */
  public List<String> getMetricsReporterFactoryClassNames() {
    List<String> trimmedFactoryClasses = new ArrayList<String>();
    if (!metricsFactoryClasses.equals("")) {
      String[] factoryClasses = metricsFactoryClasses.split(",");
      for (String factoryClass : factoryClasses) {
        trimmedFactoryClasses.add(factoryClass.trim());
      }
    }
    return trimmedFactoryClasses;
  }

  public MetricsConfig(VerifiableProperties verifiableProperties) {
    metricsFactoryClasses = verifiableProperties.getString("metrics.reporters.factory.classes",
        "com.github.ambry.metrics.JmxReporterFactory");
  }
}
