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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests {@link AmbrySecurityServiceFactory}
 */
public class AmbrySecurityServiceFactoryTest {

  /**
   * Tests intantiation of {@link AmbrySecurityServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getAmbrySecurityServiceFactoryTest() throws Exception {
    SecurityService securityService =
        new AmbrySecurityServiceFactory(new VerifiableProperties(new Properties()), new MockClusterMap(), null, null,
            null, null).getSecurityService();
    Assert.assertNotNull(securityService);
  }
}
