/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import java.util.Map;


/**
 * Interface for the policy to calculate cost of a request that determines the cost incurred to serve customer requests.
 * This information will be used by Ambry's cost model.
 */
public interface AmbryCostModelPolicy {

  /**
   * Calculates the cost incurred to serve the specified {@link RestRequest} for blob specified by {@link BlobInfo}.
   * @param restRequest {@link RestRequest} served.
   * @param responseChannel {@link RestResponseChannel} object.
   * @param blobInfo {@link BlobInfo} of the blob in request.
   * @return Map of cost metrics and actual cost value.
   */
  Map<String, Double> calculateRequestCost(RestRequest restRequest, RestResponseChannel responseChannel,
      BlobInfo blobInfo);
}
