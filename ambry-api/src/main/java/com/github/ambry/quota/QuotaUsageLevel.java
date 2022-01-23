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
 * Quota usage levels.
 * The limit for each type of level should be determined by {@link QuotaRecommendationMergePolicy} implementation.
 * NOTE that the order of these enums should not be changed since their relative position is used for comparison.
 */
public enum QuotaUsageLevel {

  /** The quota usage is within healthy usage limits. */
  HEALTHY,

  /** The quota usage is approaching limit. */
  WARNING,

  /** Usage at this level is fast approaching the limit at which requests will be throttled. */
  CRITICAL,

  /** The quota usage is at or above limit and will be throttled */
  EXCEEDED
}
