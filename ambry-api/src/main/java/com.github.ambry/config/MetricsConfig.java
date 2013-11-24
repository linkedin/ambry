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
  @Default("com.github.ambry.metrics.JmxReporter")
  public final String metricsFactoryClasses;

  /**
   * Returns a list of all metrics names from the config file. Useful for
   * getting individual metrics.
   */
  public List<String> getMetricsReporterFactoryClassNames() {
    List<String> trimmedFactoryClasses = new ArrayList<String>();
    if (!metricsFactoryClasses.equals("")) {
      String[] factoryClasses = metricsFactoryClasses.split(",");
      for (String factoryClass : factoryClasses)
        trimmedFactoryClasses.add(factoryClass.trim());
    }
    return trimmedFactoryClasses;
  }

  public MetricsConfig(VerifiableProperties verifiableProperties) {
    metricsFactoryClasses = verifiableProperties.getString(
            "metrics.reporters.factory.classes", "com.github.ambry.metrics.JmxReporter");
  }
}
