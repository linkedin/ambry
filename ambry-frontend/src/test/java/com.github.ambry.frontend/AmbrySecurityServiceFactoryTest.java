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

  @Test
  public void getAmbrySecurityServiceFactoryTest()
      throws InstantiationException {
    SecurityService securityService = new AmbrySecurityServiceFactory()
        .getSecurityService(new VerifiableProperties(new Properties()), new MetricRegistry());
    Assert.assertNotNull(securityService);
  }
}
