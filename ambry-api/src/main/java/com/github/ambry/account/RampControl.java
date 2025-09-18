/*
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
package com.github.ambry.account;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RampControl {
  public static final String SECONDARY_ENABLED_KEY = "secondaryEnabled";
  @JsonProperty(SECONDARY_ENABLED_KEY)
  private final boolean secondaryEnabled;

  @JsonCreator
  public RampControl(@JsonProperty(SECONDARY_ENABLED_KEY) boolean secondaryEnabled) {
    this.secondaryEnabled = secondaryEnabled;
  }

  public boolean isSecondaryEnabled() {
    return secondaryEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RampControl that = (RampControl) o;
    return secondaryEnabled == that.secondaryEnabled;
  }

  @Override
  public int hashCode() {
    return Boolean.hashCode(secondaryEnabled);
  }
}
