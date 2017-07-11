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
import com.github.ambry.router.Callback;


/**
 * An {@link AclService} that never denies access.
 */
class NoOpAclService implements AclService<Object> {

  /**
   * {@inheritDoc}
   * Always allow access.
   * @param principal the requester principal (identity).
   * @param resource the {@link Resource} to check for access to.
   * @param operation the {@link Operation} to perform on the resource.
   * @return {@code true}
   */
  @Override
  public boolean hasAccess(Object principal, Resource resource, Operation operation) {
    return true;
  }

  @Override
  public void allowAccess(Object principal, Resource resource, Operation operation, Callback<Void> callback) {
    callback.onCompletion(null, new UnsupportedOperationException("allowAccess not supported for NoOpAclService"));
  }

  @Override
  public void revokeAccess(Object principal, Resource resource, Operation operation, Callback<Void> callback) {
    callback.onCompletion(null, new UnsupportedOperationException("revokeAccess not supported for NoOpAclService"));
  }
}
