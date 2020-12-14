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
  private final QuotaOperation quotaOperation;

  /**
   * Constructor of {@link RequestCost}.
   * @param cost cost of the request.
   * @param quotaResource {@link QuotaResource} specifying the resource for which cost should be applied.
   * @param quotaOperation {@link QuotaOperation} object.
   */
  public RequestCost(T cost, QuotaResource quotaResource, QuotaOperation quotaOperation) {
    this.cost = cost;
    this.quotaResource = quotaResource;
    this.quotaOperation = quotaOperation;
  }

  /**
   * @return cost of the request.
   */
  public T getCost() {
    return cost;
  }

  /**
   * @return QuotaResource object.
   */
  public QuotaResource getQuotaResource() {
    return quotaResource;
  }

  /**
   * @return QuotaOperation object.
   */
  public QuotaOperation getQuotaOperation() {
    return quotaOperation;
  }
}
