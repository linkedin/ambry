package com.github.ambry.router;

import java.util.Properties;


public class RouterTestUtils {
  public static Properties getCommonRouterProps() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DC1");
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    return properties;
  }
}
