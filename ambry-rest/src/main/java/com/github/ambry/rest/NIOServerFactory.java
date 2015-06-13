package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.NioServer;
import com.github.ambry.utils.Utils;


/**
 * Factory for returning different types of NioServer implementations based on config.
 */
public class NioServerFactory {
  public static String NIO_SERVER_CLASS_KEY = "rest.nioserver";

  /**
   * Uses a key defined in the configuration file to return the right type of NioServer
   * @param verifiableProperties
   * @param metricRegistry
   * @param requestDelegator
   * @return
   * @throws Exception
   */
  public static NioServer getNIOServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator requestDelegator)
      throws Exception {
    String nioServerClassName =
        verifiableProperties.getString(NIO_SERVER_CLASS_KEY, "com.github.ambry.rest.NettyServer");
    NioServer nioServer;
    /**
     *  TODO: Since this seems a little ugly, should think about whether RequestDelegator should be a singleton class
     *  TODO: that provides a static method that can be called from within the NioServer instead of providing it as an
     *  TODO: argument here.
     */
    if (requestDelegator != null) {
      // try a constructor with the request delegator.
      nioServer = Utils.getObj(nioServerClassName, verifiableProperties, metricRegistry, requestDelegator);
    } else {
      // try a constructor without the request delegator.
      nioServer = Utils.getObj(nioServerClassName, verifiableProperties, metricRegistry);
    }
    return nioServer;
  }
}
