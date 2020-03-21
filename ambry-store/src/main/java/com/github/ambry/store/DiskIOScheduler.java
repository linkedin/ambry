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

package com.github.ambry.store;

import com.github.ambry.utils.Throttler;
import java.util.HashMap;
import java.util.Map;


/**
 * Helps schedule I/O operations.
 * This is meant to be used by
 * 1. Application reads/writes from/to the log.
 * 2. Hard delete
 * 3. Compaction
 * The initial implementation simply returns MAX_VALUE for the application and uses the throttler for the other 2.
 * In the future this will have functions to submit feedback so that more intelligent decisions can be made.
 */
class DiskIOScheduler {
  private final Map<String, Throttler> throttlers;

  /**
   * Create a {@link DiskIOScheduler}.
   * @param throttlers the {@link Throttler}s to use for each job type.
   */
  DiskIOScheduler(Map<String, Throttler> throttlers) {
    this.throttlers = throttlers != null ? throttlers : new HashMap<String, Throttler>();
  }

  /**
   * Return the size of I/O permissible based on the parameters provided.
   * @param jobType the type of the job requesting an I/O slice.
   * @param jobId the ID of the job requesting an I/O slice.
   * @param usedSinceLastCall the amount of capacity used since the last call to this function.
   * @return the I/O slice available for use.
   */
  long getSlice(String jobType, String jobId, long usedSinceLastCall) {
    Throttler throttler = throttlers.get(jobType);
    if (throttler != null) {
      try {
        throttler.maybeThrottle(usedSinceLastCall);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Throttler call interrupted", e);
      }
    }
    return Long.MAX_VALUE;
  }

  /**
   * Disables the DiskIOScheduler i.e. there will be no more blocking calls
   */
  void disable() {
    for (Throttler throttler : throttlers.values()) {
      throttler.disable();
    }
  }
}

