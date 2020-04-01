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

package com.github.ambry.account;

import com.github.ambry.router.Callback;
import java.io.Closeable;


/**
 * A service that handles authorization of requests. The call to {@link #hasAccess(Object, Resource, Operation)} should
 * be fast, since it will potentially be used on the critical path.
 * @param <P> the type for the principal. This is generic to allow for different requester authentication schemes.
 */
public interface AclService<P> extends Closeable {

  /**
   * Makes a resource access decision.
   * @param principal the requester principal (identity).
   * @param resource the {@link Resource} to check for access to.
   * @param operation the {@link Operation} to perform on the resource.
   * @return {@code true} if the principal is allowed to perform the specified operation on the target resource.
   */
  boolean hasAccess(P principal, Resource resource, Operation operation);

  /**
   * Allow the provided principal to perform an {@link Operation} on a {@link Resource}.
   * @param principal the principal to add a rule for.
   * @param resource the {@link Resource} to add the rule for.
   * @param operation the {@link Operation} to allow the principal to perform on the {@link Resource}.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   */
  void allowAccess(P principal, Resource resource, Operation operation, Callback<Void> callback);

  /**
   * Prevent the provided principal from performing an {@link Operation} on a {@link Resource}.
   * @param principal the principal to add a rule for.
   * @param resource the {@link Resource} to add the rule for.
   * @param operation the {@link Operation} to allow the principal to perform on the {@link Resource}.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   */
  void revokeAccess(P principal, Resource resource, Operation operation, Callback<Void> callback);

  /**
   * The type of operation to perform on a {@link Resource}.
   */
  enum Operation {
    /**
     * Create new content within a resource. For example, put a new blob in a container.
     */
    CREATE,

    /**
     * Read content from a resource. For example, get a blob in a container.
     */
    READ,

    /**
     * Update existing content within a resource.
     * For example, mutate a blob in a container (note: this is not something currently supported by Ambry).
     */
    UPDATE,

    /**
     * Delete existing content from a resource. For example, delete a blob from a container.
     */
    DELETE,
  }

  /**
   * An interface that represents an ACLed resource. The resource must provide a resource type and a unique ID for the
   * specific resource that the {@link AclService} can use.
   */
  interface Resource {
    /**
     * @return the type of resource this is. This should be the same for all resources of a given type
     * (i.e. a container).
     */
    String getResourceType();

    /**
     * @return a unique identifier for this specific resource. This should ideally be URL safe for flexibility when
     * implementing {@link AclService}.
     */
    String getResourceId();
  }
}
