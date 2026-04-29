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
package com.github.ambry.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Centralized factory for Jackson {@link ObjectMapper} instances.
 *
 * Every instance returned by {@link #newObjectMapper()} is pre-configured with
 * {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} disabled so that new JSON fields
 * introduced during rolling deployments do not cause deserialization failures on older instances.
 *
 * Callers may further configure the returned instance (e.g. {@code registerModule},
 * {@code addMixIn}, {@code enable(SerializationFeature.INDENT_OUTPUT)}); per-instance
 * configuration is preserved.
 */
public final class JsonUtil {

  private JsonUtil() {
    // utility class
  }

  /**
   * Create a new {@link ObjectMapper} with safe defaults.
   *
   * @return a freshly created ObjectMapper (callers may further configure it)
   */
  public static ObjectMapper newObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }
}
