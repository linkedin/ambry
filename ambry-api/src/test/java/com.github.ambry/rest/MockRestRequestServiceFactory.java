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
package com.github.ambry.rest;

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;


/**
 * Implementation of {@link RestRequestServiceFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockRestRequestService} and returns a new
 * instance on {@link #getRestRequestService()}.
 */
public class MockRestRequestServiceFactory implements RestRequestServiceFactory {
  private final VerifiableProperties verifiableProperties;
  private final Router router;

  public MockRestRequestServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap, Router router,
      AccountService accountService) {
    this.verifiableProperties = verifiableProperties;
    this.router = router;
  }

  /**
   *
   * Returns a new instance of {@link MockRestRequestService}.
   * @return a new instance of {@link MockRestRequestService}.
   */
  @Override
  public RestRequestService getRestRequestService() {
    return new MockRestRequestService(verifiableProperties, router);
  }
}
