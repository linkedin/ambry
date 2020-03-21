/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.config;

import com.github.ambry.commons.Criteria;
import com.github.ambry.commons.PerformanceIndex;
import com.github.ambry.commons.Thresholds;
import com.github.ambry.rest.RestMethod;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;


/**
 * Configuration parameters required to help evaluate performance of Ambry(i.e latency, health).
 */
public class PerformanceConfig {

  public static final String NON_SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR =
      "performance.non.success.rest.request.total.time.threshold";
  public static final String SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR =
      "performance.success.rest.request.total.time.threshold";
  private static final String DEFAULT_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR =
      "{\"PUT\": " + Long.MAX_VALUE + ",\"GET\": " + Long.MAX_VALUE + ",\"POST\": " + Long.MAX_VALUE + ",\"HEAD\": "
          + Long.MAX_VALUE + ",\"DELETE\": " + Long.MAX_VALUE + "}";

  /**
   * The round trip time (millisecond) threshold for each type of rest request with non-success response(3xx or 4xx status code).
   * The request is considered "Unsatisfied" if total time exceeds this threshold.
   */
  @Config(NON_SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR)
  @Default(DEFAULT_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR)
  private final String nonSuccessRestRequestTotalTimeThresholds;

  /**
   * The round trip time (millisecond) threshold for DELETE/HEAD/PUT request with success response(2xx status code).
   * The request is considered "Unsatisfied" if total time exceeds this threshold.
   */
  @Config(SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR)
  @Default(DEFAULT_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR)
  private final String successRestRequestTotalTimeThresholds;

  /**
   * The time to first byte (millisecond) threshold for success GET request(2xx status code). The request is considered
   * "Unsatisfied" if the time exceeds this threshold.
   */
  @Config("performance.success.get.time.to.first.byte.threshold")
  @Default("Long.MAX_VALUE")
  public final long successGetTimeToFirstByteThreshold;

  /**
   * The average bandwidth (bytes/sec) threshold for success GET request(2xx status code). The request is considered
   * "Unsatisfied" if actual bandwidth falls below this threshold.
   */
  @Config("performance.success.get.average.bandwidth.threshold")
  @Default("0")
  public final long successGetAverageBandwidthThreshold;

  /**
   * The average bandwidth (bytes/sec) threshold for success POST request(2xx status code). The request is considered
   * "Unsatisfied" if actual bandwidth falls below this threshold.
   */
  @Config("performance.success.post.average.bandwidth.threshold")
  @Default("0")
  public final long successPostAverageBandwidthThreshold;

  public final EnumMap<RestMethod, Thresholds> successRequestThresholds = new EnumMap<>(RestMethod.class);
  public final EnumMap<RestMethod, Thresholds> nonSuccessRequestThresholds = new EnumMap<>(RestMethod.class);

  public PerformanceConfig(VerifiableProperties verifiableProperties) {
    // Get related thresholds
    successGetTimeToFirstByteThreshold =
        verifiableProperties.getLong("performance.success.get.time.to.first.byte.threshold", Long.MAX_VALUE);
    successGetAverageBandwidthThreshold =
        verifiableProperties.getLong("performance.success.get.average.bandwidth.threshold", 0);
    // Post related thresholds
    successPostAverageBandwidthThreshold =
        verifiableProperties.getLong("performance.success.post.average.bandwidth.threshold", 0);
    nonSuccessRestRequestTotalTimeThresholds =
        verifiableProperties.getString(NON_SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR,
            DEFAULT_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR);
    successRestRequestTotalTimeThresholds =
        verifiableProperties.getString(SUCCESS_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR,
            DEFAULT_REST_REQUEST_TOTAL_TIME_THRESHOLD_STR);
    JSONObject successThresholdObject = new JSONObject(successRestRequestTotalTimeThresholds);
    JSONObject nonSuccessThresholdObject = new JSONObject(nonSuccessRestRequestTotalTimeThresholds);

    for (RestMethod method : EnumSet.of(RestMethod.GET, RestMethod.POST, RestMethod.DELETE, RestMethod.HEAD,
        RestMethod.PUT)) {
      Map<PerformanceIndex, Criteria> successPerfIndexMap = new HashMap<>();
      Map<PerformanceIndex, Criteria> nonSuccessPerfIndexMap = new HashMap<>();
      if (method == RestMethod.GET) {
        successPerfIndexMap.put(PerformanceIndex.AverageBandwidth,
            new Criteria(successGetAverageBandwidthThreshold, Criteria.BoundType.LowerBound));
        successPerfIndexMap.put(PerformanceIndex.TimeToFirstByte,
            new Criteria(successGetTimeToFirstByteThreshold, Criteria.BoundType.UpperBound));
      } else if (method == RestMethod.POST) {
        successPerfIndexMap.put(PerformanceIndex.AverageBandwidth,
            new Criteria(successPostAverageBandwidthThreshold, Criteria.BoundType.LowerBound));
      } else {
        successPerfIndexMap.put(PerformanceIndex.RoundTripTime,
            new Criteria(successThresholdObject.optLong(method.name(), Long.MAX_VALUE), Criteria.BoundType.UpperBound));
      }
      nonSuccessPerfIndexMap.put(PerformanceIndex.RoundTripTime,
          new Criteria(nonSuccessThresholdObject.optLong(method.name(), Long.MAX_VALUE),
              Criteria.BoundType.UpperBound));
      successRequestThresholds.put(method, new Thresholds(successPerfIndexMap));
      nonSuccessRequestThresholds.put(method, new Thresholds(nonSuccessPerfIndexMap));
    }
  }
}
