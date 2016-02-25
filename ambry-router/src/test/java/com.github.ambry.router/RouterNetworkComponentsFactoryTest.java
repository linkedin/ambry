package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.utils.MockTime;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Test class for {@link RouterNetworkComponentsFactory} and {@link RouterNetworkComponents}
 */
public class RouterNetworkComponentsFactoryTest {
  NetworkMetrics metrics;
  NetworkConfig config;
  SSLFactory sslFactory;
  MockTime time;

  @Test
  public void testRouterNetworkComponentsFactory()
      throws Exception {
    NetworkMetrics metrics = new NetworkMetrics(new MetricRegistry());
    NetworkConfig config = new NetworkConfig(new VerifiableProperties(new Properties()));
    SSLFactory sslFactory = null;
    MockTime time = new MockTime();
    RouterNetworkComponentsFactory routerNetworkComponentsFactory =
        new RouterNetworkComponentsFactory(metrics, config, sslFactory, 3, 3, time);
    RouterNetworkComponents components = routerNetworkComponentsFactory.getRouterNetworkComponents();
    Assert.assertNotNull(components.getSelector());
    Assert.assertNotNull(components.getConnectionManager());
  }

  @Test
  public void testRouterNetworkComponentsFactoryWithBadInputs()
      throws Exception {
    NetworkMetrics metrics = new NetworkMetrics(new MetricRegistry());
    NetworkConfig config = new NetworkConfig(new VerifiableProperties(new Properties()));
    SSLFactory sslFactory = null;
    MockTime time = new MockTime();
    try {
      RouterNetworkComponentsFactory routerNetworkComponentsFactory =
          new RouterNetworkComponentsFactory(null, config, sslFactory, 3, 3, time);
      Assert.fail("Router factory should throw when called with invalid input");
    } catch (Exception e) {
    }
    try {
      RouterNetworkComponentsFactory routerNetworkComponentsFactory =
          new RouterNetworkComponentsFactory(metrics, null, sslFactory, 3, 3, time);
      Assert.fail("Router factory should throw when called with invalid input");
    } catch (Exception e) {
    }
  }
}
