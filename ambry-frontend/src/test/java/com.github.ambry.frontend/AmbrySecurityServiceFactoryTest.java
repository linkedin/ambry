/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
  public void getAmbrySecurityServiceFactoryTest() throws InstantiationException {
    SecurityService securityService = new AmbrySecurityServiceFactory(new VerifiableProperties(new Properties()),
        new MetricRegistry()).getSecurityService();
    Assert.assertNotNull(securityService);
  }
}
