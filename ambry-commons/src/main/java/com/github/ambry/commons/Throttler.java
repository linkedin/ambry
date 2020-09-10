/**
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
package com.github.ambry.commons;

public abstract class Throttler {
  private final ThrottlingMode throttlingMode;

  /**
   * Constructor for {@link Throttler}.
   * @param throttlingMode The {@link ThrottlingMode} mode in which the throttler will operate.
   */
  public Throttler(ThrottlingMode throttlingMode) {
    this.throttlingMode = throttlingMode;
  }

  /**
   * Get the {@link ThrottlingMode} of the current {@link Throttler}.
   * @return {@link ThrottlingMode} object.
   */
  public ThrottlingMode getThrottlingMode() {
    return throttlingMode;
  }

  /**
   * Decide if the request needs to be throttled.
   * @return true if request should be throttled, false otherwise.
   */
  public abstract boolean throttle();
}
