/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.ServerSecurityService;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests {@link AmbryServerSecurityServiceFactory}
 */
public class AmbryServerSecurityServiceFactoryTest {

  /**
   * Tests intantiation of {@link AmbryServerSecurityServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getAmbrySecurityServiceFactoryTest() throws Exception {
    ServerSecurityService serverSecurityService =
        new AmbryServerSecurityServiceFactory(new VerifiableProperties(new Properties()),
            null, null).getServerSecurityService();
    Assert.assertNotNull(serverSecurityService);
  }
}
