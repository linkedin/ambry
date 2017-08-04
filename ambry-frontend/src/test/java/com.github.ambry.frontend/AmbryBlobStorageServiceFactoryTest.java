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

import com.github.ambry.account.MockNotifier;
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

import static org.junit.Assert.*;


/**
 * Unit tests for {@link AmbryBlobStorageServiceFactory}.
 */
public class AmbryBlobStorageServiceFactoryTest {

  /**
   * Tests the instantiation of an {@link AmbryBlobStorageService} instance through the
   * {@link AmbryBlobStorageServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getAmbryBlobStorageServiceTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AmbryBlobStorageServiceFactory ambryBlobStorageServiceFactory =
        new AmbryBlobStorageServiceFactory(verifiableProperties, new MockClusterMap(),
            new MockRestRequestResponseHandler(), new InMemoryRouter(verifiableProperties, new MockClusterMap()),
            new MockNotifier());
    BlobStorageService ambryBlobStorageService = ambryBlobStorageServiceFactory.getBlobStorageService();
    assertNotNull("No BlobStorageService returned", ambryBlobStorageService);
    assertEquals("Did not receive an AmbryBlobStorageService instance",
        AmbryBlobStorageService.class.getCanonicalName(), ambryBlobStorageService.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link AmbryBlobStorageServiceFactory} with bad input.
   * @throws Exception
   */
  @Test
  public void getAmbryBlobStorageServiceFactoryWithBadInputTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    RestResponseHandler restResponseHandler = new MockRestRequestResponseHandler();
    Router router = new InMemoryRouter(verifiableProperties, clusterMap);

    // VerifiableProperties null.
    try {
      new AmbryBlobStorageServiceFactory(null, clusterMap, restResponseHandler, router, new MockNotifier());
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, null, restResponseHandler, router, new MockNotifier());
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestResponseHandler null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, null, router, new MockNotifier());
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // Router null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, restResponseHandler, null,
          new MockNotifier());
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
