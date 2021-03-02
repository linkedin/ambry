/**
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
 * Quota warning levels.
 * The limit for each type of warning should be determined by {@link ThrottlePolicy} implementation.
 */
public enum QuotaWarningLevel {
  HEALTHY, /** The quota usage is within healthy usage limits. */
  WARNING, /** The quota usage is approaching limit. */
  CRITICAL, /** Usage at this level is fast approaching fatal level at which requests will be throttled. */
  FATAL; /** The quota usage is at or above limit and will be throttled */

  /**
   * @return QuotaWarningLevel corresponding to the int specified.
   */
  public static QuotaWarningLevel fromInt(int ordinal) {
    return QuotaWarningLevel.values()[ordinal];
  }
}
