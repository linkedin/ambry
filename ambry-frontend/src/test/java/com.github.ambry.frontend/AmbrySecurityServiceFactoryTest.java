package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.SecurityService;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests {@link AmbrySecurityServiceFactory}
 */
public class AmbrySecurityServiceFactoryTest {

  /**
   * Tests intantiation of {@link AmbrySecurityServiceFactory}.
   * @throws InstantiationException
   */
  @Test
  public void getAmbrySecurityServiceFactoryTest()
      throws InstantiationException {
    SecurityService securityService =
        new AmbrySecurityServiceFactory(new VerifiableProperties(new Properties()), new MetricRegistry())
            .getSecurityService();
    Assert.assertNotNull(securityService);
  }

  /**
   * Tests instantiation of {@link AmbrySecurityServiceFactory} with bad input.
   */
  @Test
  public void getAmbrySecurityServiceFactoryWithBadInputTest() {
    try {
      new AmbrySecurityServiceFactory(null, new MetricRegistry());
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
