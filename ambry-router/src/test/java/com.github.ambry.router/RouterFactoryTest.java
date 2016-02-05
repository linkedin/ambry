package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.RouterConfig;
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
   * Tests the instantiation of a {@link Router} implementation through its {@link RouterFactory} implementation
   * @throws IOException
   */
  @Test
  public void getCoordinatorBackedRouterTest()
      throws Exception {
    Properties properties = RouterTestUtils.getProps();

    class FactoryAndRouter {
      String factoryStr;
      String routerStr;

      FactoryAndRouter(String factoryStr, String routerStr) {
        this.factoryStr = factoryStr;
        this.routerStr = routerStr;
      }
    }
    ;

    List<FactoryAndRouter> factoryAndRouters = new ArrayList<FactoryAndRouter>();
    factoryAndRouters.add(new FactoryAndRouter("com.github.ambry.router.NonBlockingRouterFactory",
        "com.github.ambry.router.NonBlockingRouter"));
    factoryAndRouters.add(new FactoryAndRouter("com.github.ambry.router.CoordinatorBackedRouterFactory",
        "com.github.ambry.router.CoordinatorBackedRouter"));
    factoryAndRouters.add(new FactoryAndRouter("com.github.ambry.router.InMemoryRouterFactory",
        "com.github.ambry.router.InMemoryRouter"));

    for (FactoryAndRouter factoryAndRouter : factoryAndRouters) {
      properties.setProperty("router.factory", factoryAndRouter.factoryStr);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

      RouterConfig routerConfig = new RouterConfig(verifiableProperties);
      RouterFactory routerFactory = Utils.getObj(routerConfig.routerFactory, verifiableProperties, new MockClusterMap(),
          new LoggingNotificationSystem());
      Router router = routerFactory.getRouter();
      Assert.assertEquals("Did not receive expected Router instance", factoryAndRouter.routerStr,
          router.getClass().getCanonicalName());
    }
  }
}
