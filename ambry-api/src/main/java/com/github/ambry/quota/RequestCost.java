/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 * Class representing cost of a request for a resource.
 * @param <T> the type of the cost value.
 */
public class RequestCost<T> {
  private final T cost;
  private final QuotaResource quotaResource;

  /**
   * Constructor of {@link RequestCost}.
   * @param cost cost of the request.
   * @param quotaResource {@link QuotaResource} specifying the resource for which cost should be applied.
   */
  public RequestCost(T cost, QuotaResource quotaResource) {
    this.cost = cost;
    this.quotaResource = quotaResource;
  }

  /**
   * @return cost of the request.
   */
  public T getCost() {
    return cost;
  }
}
