package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


public class RouterFactoryTest {
  /**
   * (routerFactoryString, routerString) pair class.
   */
  class FactoryAndRouter {
    String factoryStr;
    String routerStr;

    FactoryAndRouter(String factoryStr, String routerStr) {
      this.factoryStr = factoryStr;
      this.routerStr = routerStr;
    }
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * any router.
   * @return the created VerifiableProperties instance.
   */
  private VerifiableProperties getVerifiableProperties() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DC1");
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    return new VerifiableProperties(properties);
  }

  /**
   * Tests the instantiation of a {@link Router} implementation through its {@link RouterFactory} implementation
   * @throws IOException
   */
  @Test
  public void testRouterFactory()
      throws Exception {
    VerifiableProperties verifiableProperties = getVerifiableProperties();
    List<FactoryAndRouter> factoryAndRouters = new ArrayList<FactoryAndRouter>();
    factoryAndRouters.add(new FactoryAndRouter("com.github.ambry.router.NonBlockingRouterFactory",
        "com.github.ambry.router.NonBlockingRouter"));
    factoryAndRouters.add(new FactoryAndRouter("com.github.ambry.router.CoordinatorBackedRouterFactory",
        "com.github.ambry.router.CoordinatorBackedRouter"));

    for (FactoryAndRouter factoryAndRouter : factoryAndRouters) {
      RouterFactory routerFactory = Utils
          .getObj(factoryAndRouter.factoryStr, verifiableProperties, new MockClusterMap(),
              new LoggingNotificationSystem());
      Router router = routerFactory.getRouter();
      Assert.assertEquals("Did not receive expected Router instance", factoryAndRouter.routerStr,
          router.getClass().getCanonicalName());
      router.close();
    }
  }
}
