/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonPropertyOrder;


/**
 * A wrapper model object that contains a {@link StatsSnapshot} and a {@link StatsHeader} with metadata about the
 * {@link StatsSnapshot}.
 */
@JsonPropertyOrder({"header", "snapshot"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class StatsWrapper {
  private StatsHeader header;
  private StatsSnapshot snapshot;

  public StatsWrapper(StatsHeader header, StatsSnapshot snapshot) {
    this.snapshot = snapshot;
    this.header = header;
  }

  public StatsWrapper() {
    // empty constructor for Jackson deserialization
  }

  public StatsHeader getHeader() {
    return header;
  }

  public StatsSnapshot getSnapshot() {
    return snapshot;
  }
}
