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

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.List;


/**
 * Interface to apply application specific policy to make overall recommendation based on {@link QuotaRecommendation}s.
 */
public interface QuotaRecommendationMergePolicy {
  HttpResponseStatus ACCEPT_HTTP_STATUS = HttpResponseStatus.OK;
  HttpResponseStatus THROTTLE_HTTP_STATUS = HttpResponseStatus.TOO_MANY_REQUESTS;

  /**
   * Provide overall recommendation by merging {@link QuotaRecommendation}s from all quota enforcers.
   * @param quotaRecommendations {@link List} of {@link QuotaRecommendation}s.
   * @return ThrottlingRecommendation object containing overall quota usage and recommendation.
   */
  ThrottlingRecommendation mergeEnforcementRecommendations(List<QuotaRecommendation> quotaRecommendations);
}
