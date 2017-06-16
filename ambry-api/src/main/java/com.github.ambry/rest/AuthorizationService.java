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

package com.github.ambry.rest;

import com.github.ambry.account.Container;
import java.util.Collection;


/**
 * A service that handles authorization of requests. This will be provided to the {@link SecurityService} for making
 * access decisions that might require the help of an external service. The call to {@link #hasAccess(RestRequest)}
 * should not require expensive network calls, since it will potentially be used on the critical path. Instead,
 * the {@link AuthorizationService} should maintain a cache of ACLs for the resources it manages permissions on.
 */
public interface AuthorizationService {

  /**
   * Makes an access decision based on the information provided in a {@link RestRequest}.
   * @param request the request that contains the target resource and information to establish requester identity
   * @throws RestServiceException if access to a resource is denied.
   */
  void hasAccess(RestRequest request) throws RestServiceException;

  /**
   * Called to inform the authorization layer that containers have been added and ACLs need to be cached for these
   * containers. This should be called on startup with the full list of containers to ensure that ACLs are cached
   * for the existing containers.
   * @param containers the {@link Container}s that were added.
   */
  void onContainerCreation(Collection<Container> containers);

  /**
   * Called to inform the authorization layer that containers have been removed and ACLs no longer have to be cached
   * for these containers.
   * @param containers the {@link Container}s that were removed.
   */
  void onContainerRemoval(Collection<Container> containers);
}
