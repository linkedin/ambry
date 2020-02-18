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

/**
 * An {@link AclService.Resource} that represents the blobs in a {@link Container}.
 *
 */
public class ContainerBlobsResource implements AclService.Resource {
  public static final String RESOURCE_TYPE = "ContainerBlobs";
  private static final String SEPARATOR = "_";

  private final String resourceId;

  /**
   * Construct the resource from a container.
   * @param container the {@link Container}
   */
  public ContainerBlobsResource(Container container) {
    resourceId = container.getParentAccountId() + SEPARATOR + container.getId();
  }

  /**
   * {@inheritDoc}
   * @return A type name for this resource: {@code ContainerBlobs}
   */
  @Override
  public String getResourceType() {
    return RESOURCE_TYPE;
  }

  /**
   * {@inheritDoc}
   * @return a unique identifier for this container: {@code {parent-account-id}_{container-id}}
   */
  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerBlobsResource that = (ContainerBlobsResource) o;

    return resourceId.equals(that.resourceId);
  }

  @Override
  public int hashCode() {
    return resourceId.hashCode();
  }

  @Override
  public String toString() {
    return RESOURCE_TYPE + ":" + resourceId;
  }
}
