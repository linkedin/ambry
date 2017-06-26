/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.AuthorizationService;
import com.github.ambry.rest.AuthorizationServiceFactory;
import org.junit.Test;

import static org.junit.Assert.*;


public class NoOpAuthorizationServiceFactoryTest {
  @Test
  public void instantiationTest() throws Exception {
    AuthorizationServiceFactory factory = new NoOpAuthorizationServiceFactory(null, null);
    AuthorizationService authorizationService = factory.getAuthorizationService();
    assertTrue("Wrong type of AuthorizationService from factory",
        authorizationService instanceof NoOpAuthorizationService);
    // ensure that none of these methods throw exceptions.
    authorizationService.hasAccess(null);
    authorizationService.onContainerCreation(null);
    authorizationService.onContainerRemoval(null);
  }
}