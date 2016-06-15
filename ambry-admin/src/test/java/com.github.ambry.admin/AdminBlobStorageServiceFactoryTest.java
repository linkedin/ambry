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
package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.MockRestRequestResponseHandler;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.Router;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link AdminBlobStorageServiceFactory}.
 */
public class AdminBlobStorageServiceFactoryTest {

  /**
   * Tests the instantiation of an {@link AdminBlobStorageService} instance through the
   * {@link AdminBlobStorageServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getAdminBlobStorageServiceTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AdminBlobStorageServiceFactory adminBlobStorageServiceFactory =
        new AdminBlobStorageServiceFactory(verifiableProperties, new MockClusterMap(),
            new MockRestRequestResponseHandler(), new InMemoryRouter(verifiableProperties));
    BlobStorageService adminBlobStorageService = adminBlobStorageServiceFactory.getBlobStorageService();
    assertNotNull("No BlobStorageService returned", adminBlobStorageService);
    assertEquals("Did not receive an AdminBlobStorageService instance",
        AdminBlobStorageService.class.getCanonicalName(), adminBlobStorageService.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link AdminBlobStorageServiceFactory} with bad input.
   * @throws Exception
   */
  @Test
  public void getAdminBlobStorageServiceFactoryWithBadInputTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    RestResponseHandler restResponseHandler = new MockRestRequestResponseHandler();
    Router router = new InMemoryRouter(verifiableProperties);

    // VerifiableProperties null.
    try {
      new AdminBlobStorageServiceFactory(null, clusterMap, restResponseHandler, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new AdminBlobStorageServiceFactory(verifiableProperties, null, restResponseHandler, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestResponseHandler null.
    try {
      new AdminBlobStorageServiceFactory(verifiableProperties, clusterMap, null, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // Router null.
    try {
      new AdminBlobStorageServiceFactory(verifiableProperties, clusterMap, restResponseHandler, null);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
