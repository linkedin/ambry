package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Uses the config to return an instance of RestServer.
 */
public class RestServerFactory {
  public static String SERVER_CLASS_KEY = "rest.server";

  public static RestServer getRestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator requestDelegator)
      throws Exception {
    String restServerClassName = verifiableProperties.getString(SERVER_CLASS_KEY, "com.github.ambry.rest.NettyServer");
    return Utils.getObj(restServerClassName, verifiableProperties, metricRegistry, requestDelegator);
  }
}
