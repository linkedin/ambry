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

import com.github.ambry.account.AclService;
import com.github.ambry.account.AclServiceFactory;
import org.junit.Test;

import static org.junit.Assert.*;


public class NoOpAclServiceFactoryTest {
  @Test
  public void instantiationTest() throws Exception {
    AclServiceFactory<Object> factory = new NoOpAclServiceFactory(null, null);
    AclService<Object> aclService = factory.getAclService();
    assertTrue("Wrong type of AclService from factory", aclService instanceof NoOpAclService);
    assertTrue("access should always be granted", aclService.hasAccess(null, null, null));
    // ensure that none of these methods throw exceptions.
    aclService.allowAccess(null, null, null);
    aclService.revokeAccess(null, null, null);
  }
}