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
 * Implementation of {@link BlobStorageServiceFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockBlobStorageService} and returns a new
 * instance on {@link #getBlobStorageService()}.
 */
public class MockBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final VerifiableProperties verifiableProperties;
  private final RestResponseHandler restResponseHandler;
  private final Router router;

  public MockBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      RestResponseHandler restResponseHandler, Router router, AccountService accountService) {
    this.verifiableProperties = verifiableProperties;
    this.restResponseHandler = restResponseHandler;
    this.router = router;
  }

  /**
   *
   * Returns a new instance of {@link MockBlobStorageService}.
   * @return a new instance of {@link MockBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new MockBlobStorageService(verifiableProperties, restResponseHandler, router);
  }
}
