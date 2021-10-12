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
package com.github.ambry.router;

import com.github.ambry.config.KMSConfig;
import com.github.ambry.rest.RestRequest;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;


/**
 * MockKeyManagementService to assist in testing exception cases
 */
class MockKeyManagementService extends SingleKeyManagementService {
  AtomicReference<GeneralSecurityException> exceptionToThrow = new AtomicReference<>();
  AtomicReference<RestRequest> currentRestRequest = new AtomicReference<>();

  MockKeyManagementService(KMSConfig KMSConfig, String defaultKey) throws GeneralSecurityException {
    super(KMSConfig, defaultKey);
  }

  @Override
  public SecretKeySpec getKey(RestRequest restRequest, short accountId, short containerId)
      throws GeneralSecurityException {
    currentRestRequest.set(restRequest);
    if (exceptionToThrow.get() != null) {
      throw exceptionToThrow.get();
    } else {
      return super.getKey(restRequest, accountId, containerId);
    }
  }

  /**
   * Return the restRequest that just passed to this service.
   * @return
   */
  public RestRequest getCurrentRestRequest() {
    return currentRestRequest.get();
  }
}
