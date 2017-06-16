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

import com.github.ambry.account.Container;
import com.github.ambry.rest.AuthorizationService;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import java.util.Collection;


/**
 * An {@link AuthorizationService} that never denies access.
 */
class NoOpAuthorizationService implements AuthorizationService {

  /**
   * Always allows access.
   * @param request the request that contains the target resource and information to establish requester identity
   * @throws RestServiceException
   */
  @Override
  public void hasAccess(RestRequest request) throws RestServiceException {
  }

  @Override
  public void onContainerCreation(Collection<Container> containers) {
  }

  @Override
  public void onContainerRemoval(Collection<Container> containers) {
  }
}
