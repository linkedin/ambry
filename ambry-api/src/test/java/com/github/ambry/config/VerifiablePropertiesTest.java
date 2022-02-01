/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for methods on {@link VerifiableProperties} class.
 */
public class VerifiablePropertiesTest {

  @Test
  public void testGetFloatInRange() {
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    String propertyName = "TEST";
    float defaultVal = 5.0f;
    // test for default value.
    Float result = verifiableProperties.getFloatInRange(propertyName, defaultVal, 0.0f, 10.0f);
    Assert.assertEquals(result, defaultVal, 0.001);

    // test outside range value.
    properties.setProperty(propertyName, "11.0");
    try {
      verifiableProperties.getFloatInRange(propertyName, defaultVal, 0.0f, 10.0f);
      Assert.fail("Value outside range should fail.");
    } catch (IllegalArgumentException ignored) {
    }

    // test invalid float value.
    properties.setProperty(propertyName, "string");
    try {
      verifiableProperties.getFloatInRange(propertyName, defaultVal, 0.0f, 10.0f);
      Assert.fail("Value outside range should fail.");
    } catch (NumberFormatException ignored) {
    }

    // test valid float value.
    properties.setProperty(propertyName, "9.0");
    result = verifiableProperties.getFloatInRange(propertyName, defaultVal, 0.0f, 10.0f);
    Assert.assertEquals(result, 9.0f, 0.001);
  }
}
