package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.NioServer;
import com.github.ambry.utils.Utils;


/**
 * Uses the config to return an instance of NioServer.
 */
public class NioServerFactory {
  public static String NIO_SERVER_CLASS_KEY = "rest.nioserver";

  public static NioServer getNIOServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator requestDelegator)
      throws Exception {
    String nioServerClassName =
        verifiableProperties.getString(NIO_SERVER_CLASS_KEY, "com.github.ambry.rest.NettyServer");
    NioServer nioServer;
    if (requestDelegator != null) {
      // try a constructor with the request delegator
      nioServer = Utils.getObj(nioServerClassName, verifiableProperties, metricRegistry, requestDelegator);
    } else {
      // try a constructor without the request delegator
      nioServer = Utils.getObj(nioServerClassName, verifiableProperties, metricRegistry);
    }
    return nioServer;
  }
}
