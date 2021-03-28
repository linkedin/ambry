/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

/**
 * Resource for which quota is specified for enforced.
 */
public class QuotaResource {
  private final String resourceId; // unique identifier for Ambry account, container or host.
  private final QuotaResourceType quotaResourceType;

  /**
   * Constructor for {@link QuotaResource}.
   * @param resourceId Id if the resource.
   * @param quotaResourceType {@link QuotaResourceType} object specifying the type of resource.
   */
  public QuotaResource(String resourceId, QuotaResourceType quotaResourceType) {
    this.resourceId = resourceId;
    this.quotaResourceType = quotaResourceType;
  }

  /**
   * @return the resourceId.
   */
  public String getResourceId() {
    return resourceId;
  }

  /**
   * @return the {@link QuotaResourceType}.
   */
  public QuotaResourceType getQuotaResourceType() {
    return quotaResourceType;
  }

  /**
   * Type of Ambry resource for which quota can be applied.
   */
  public enum QuotaResourceType {
    AMBRY_ACCOUNT, AMBRY_CONTAINER
  }
}
