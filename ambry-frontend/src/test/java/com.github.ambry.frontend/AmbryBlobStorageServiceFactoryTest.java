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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.MockRestRequestResponseHandler;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.Router;
import java.util.Properties;
import org.json.JSONObject;
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
    JSONObject jsonObject =
        new JSONObject().put("POST", "http://uploadUrl:15158").put("GET", "http://downloadUrl:15158");
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    properties.setProperty(FrontendConfig.URL_SIGNER_ENDPOINTS, jsonObject.toString());
    properties.setProperty("clustermap.cluster.name", "Cluster-Name");
    properties.setProperty("clustermap.datacenter.name", "Datacenter-Name");
    properties.setProperty("clustermap.host.name", "localhost");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AmbryBlobStorageServiceFactory ambryBlobStorageServiceFactory =
        new AmbryBlobStorageServiceFactory(verifiableProperties, new MockClusterMap(),
            new MockRestRequestResponseHandler(), new InMemoryRouter(verifiableProperties, new MockClusterMap()),
            new InMemAccountService(false, true));
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
    AccountService accountService = new InMemAccountService(false, true);

    // VerifiableProperties null.
    try {
      new AmbryBlobStorageServiceFactory(null, clusterMap, restResponseHandler, router, accountService);
      fail("Instantiation should have failed because VerifiableProperties was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, null, restResponseHandler, router, accountService);
      fail("Instantiation should have failed because ClusterMap was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // RestResponseHandler null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, null, router, accountService);
      fail("Instantiation should have failed because RestResponseHandler was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // Router null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, restResponseHandler, null, accountService);
      fail("Instantiation should have failed because Router was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // AccountService null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, restResponseHandler, router, null);
      fail("Instantiation should have failed because AccountService was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }
  }
}
