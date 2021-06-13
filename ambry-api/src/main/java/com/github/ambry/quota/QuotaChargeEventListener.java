/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Listener for each data chunk upload to or download from Ambry.
 */
public interface QuotaChargeEventListener {

  /**
   * Build {@link QuotaChargeEventListener} to handle quota compliance of requests.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param shouldThrottle flag indicating if request should be throttled after charging. Requests like updatettl, delete etc need not be throttled.
   * @return QuotaChargeEventListener object.
   */
  static QuotaChargeEventListener buildQuotaChargeEventListener(RestRequest restRequest, QuotaManager quotaManager,
      boolean shouldThrottle) {
    return () -> {
      ThrottlingRecommendation throttlingRecommendation = quotaManager.charge(restRequest);
      if (throttlingRecommendation.shouldThrottle() && shouldThrottle
          && quotaManager.getQuotaConfig().throttlingMode == QuotaMode.THROTTLING) {
        throw new RouterException("RequestQuotaExceeded", RouterErrorCode.TooManyRequests);
      }
    };
  }

  void onQuotaChargeEvent() throws RouterException;
}
