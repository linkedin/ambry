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
import com.github.ambry.rest.RestRequestService;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.Router;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link FrontendRestRequestServiceFactory}.
 */
public class FrontendRestRequestServiceFactoryTest {

  /**
   * Tests the instantiation of an {@link FrontendRestRequestService} instance through the
   * {@link FrontendRestRequestServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getFrontendRestRequestServiceTest() throws Exception {
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

    FrontendRestRequestServiceFactory frontendRestRequestServiceFactory =
        new FrontendRestRequestServiceFactory(verifiableProperties, new MockClusterMap(),
            new InMemoryRouter(verifiableProperties, new MockClusterMap()), new InMemAccountService(false, true));
    RestRequestService ambryRestRequestService = frontendRestRequestServiceFactory.getRestRequestService();
    assertNotNull("No RestRequestService returned", ambryRestRequestService);
    assertEquals("Did not receive an FrontendRestRequestService instance",
        FrontendRestRequestService.class.getCanonicalName(), ambryRestRequestService.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link FrontendRestRequestServiceFactory} with bad input.
   * @throws Exception
   */
  @Test
  public void getFrontendRestRequestServiceFactoryWithBadInputTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    Router router = new InMemoryRouter(verifiableProperties, clusterMap);
    AccountService accountService = new InMemAccountService(false, true);

    // VerifiableProperties null.
    try {
      new FrontendRestRequestServiceFactory(null, clusterMap, router, accountService);
      fail("Instantiation should have failed because VerifiableProperties was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new FrontendRestRequestServiceFactory(verifiableProperties, null, router, accountService);
      fail("Instantiation should have failed because ClusterMap was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // Router null.
    try {
      new FrontendRestRequestServiceFactory(verifiableProperties, clusterMap, null, accountService);
      fail("Instantiation should have failed because Router was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }

    // AccountService null.
    try {
      new FrontendRestRequestServiceFactory(verifiableProperties, clusterMap, router, null);
      fail("Instantiation should have failed because AccountService was null");
    } catch (NullPointerException e) {
      // expected. Nothing to do.
    }
  }
}
