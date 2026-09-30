/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.throttle;

/**
 * Operating mode for {@link HostLevelThrottler}.
 *
 * <ul>
 *   <li>{@code OFF}     — throttler is disabled; every request passes. No accounting, no metrics.</li>
 *   <li>{@code TRACK}   — performs per-(method, account, container) fair-share accounting and emits
 *                        "wouldThrottle" meters but never returns {@code shouldThrottle=true}.</li>
 *   <li>{@code ENFORCE} — same accounting as {@code TRACK}, and throttles over-share callers when a trigger fires.</li>
 * </ul>
 */
public enum ThrottleMode {
  OFF, TRACK, ENFORCE
}
