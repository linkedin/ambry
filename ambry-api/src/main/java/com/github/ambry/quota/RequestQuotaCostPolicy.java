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

import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * Interface to define the policy to calculate cost of a request against resource quotas.
 * This can be used for quota enforcement and quota usage tracking.
 */
public interface RequestQuotaCostPolicy {

  /**
   * Calculates the quota charge incurred to serve the specified {@link RestRequest} for blob or chunk of the specified {@code size}.
   * @param restRequest {@link RestRequest} served.
   * @param size size of the blob or chunk.
   * @return Map of cost metrics and actual cost value.
   */
  Map<String, Double> calculateRequestQuotaCharge(RestRequest restRequest, long size);
}
