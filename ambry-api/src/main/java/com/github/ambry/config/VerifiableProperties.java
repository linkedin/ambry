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
package com.github.ambry.config;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Verifiable properties for configs
 */
public class VerifiableProperties {

  private final HashSet<String> referenceSet = new HashSet<String>();
  private final Properties props;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  public VerifiableProperties(Properties props) {
    this.props = props;
  }

  public boolean containsKey(String name) {
    return props.containsKey(name);
  }

  public String getProperty(String name) {
    String value = props.getProperty(name);
    referenceSet.add(name);
    return value;
  }

  /**
   * Read a required integer property value or throw an exception if no such property is found
   */
  public int getInt(String name) {
    return Integer.parseInt(getString(name));
  }

  public int getIntInRange(String name, int start, int end) {
    if (!containsKey(name)) {
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
    return getIntInRange(name, -1, start, end);
  }

  /**
   * Read an integer from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the integer value
   */
  public int getInt(String name, int defaultVal) {
    return getIntInRange(name, defaultVal, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public Integer getInteger(String name, Integer defaultVal) {
    Integer v;
    if (containsKey(name)) {
      v = Integer.parseInt(getProperty(name));
    } else {
      v = defaultVal;
    }
    return v;
  }

  public Short getShort(String name, Short defaultVal) {
    return getShortInRange(name, defaultVal, Short.MIN_VALUE, Short.MAX_VALUE);
  }

  /**
   * Read an integer from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @param start The start of the range in which the value must fall (inclusive)
   * @param end The end of the range in which the value must fall
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the integer value
   */
  public int getIntInRange(String name, int defaultVal, int start, int end) {
    int v = containsKey(name) ? Integer.parseInt(getProperty(name)) : defaultVal;
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(
          name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
    }
  }

  public Short getShortInRange(String name, Short defaultVal, Short start, Short end) {
    Short v = containsKey(name) ? Short.parseShort(getProperty(name)) : defaultVal;
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(
          name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
    }
  }

  /**
   * Read a short from the properties instance. Throw an exception if the value is not among the allowed values
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @param allowedValues The array of allowed values for this property.
   * @return the Short value
   */
  public Short getShortFromAllowedValues(String name, Short defaultVal, Short[] allowedValues) {
    List<Short> allowedValuesList = Arrays.asList(allowedValues);
    Short v = containsKey(name) ? Short.parseShort(getProperty(name)) : defaultVal;
    if (allowedValuesList.contains(v)) {
      return v;
    } else {
      throw new IllegalArgumentException(
          name + " has value " + v + " which is not among the allowed values: " + allowedValuesList);
    }
  }

  public Double getDoubleInRange(String name, Double defaultVal, Double start, Double end) {
    Double v = containsKey(name) ? Double.valueOf(Double.parseDouble(getProperty(name))) : defaultVal;
    // use big decimal for double comparison
    BigDecimal startDecimal = new BigDecimal(start);
    BigDecimal endDecimal = new BigDecimal(end);
    BigDecimal value = new BigDecimal(v);
    if (value.compareTo(startDecimal) >= 0 && value.compareTo(endDecimal) <= 0) {
      return v;
    } else {
      throw new IllegalArgumentException(
          name + " has value " + v + " which is not in range " + start + "-" + end + ".");
    }
  }

  /**
   * Read a required long property value or throw an exception if no such property is found
   */
  public long getLong(String name) {
    return Long.parseLong(getString(name));
  }

  /**
   * Read an long from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the long value
   */
  public long getLong(String name, long defaultVal) {
    return getLongInRange(name, defaultVal, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   * Read an long from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @param start The start of the range in which the value must fall (inclusive)
   * @param end The end of the range in which the value must fall
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the long value
   */
  public long getLongInRange(String name, long defaultVal, long start, long end) {
    long v = containsKey(name) ? Long.parseLong(getProperty(name)) : defaultVal;
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(
          name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
    }
  }

  /**
   * Get a required argument as a double
   * @param name The property name
   * @return the value
   * @throw IllegalArgumentException If the given property is not present
   */
  public double getDouble(String name) {
    return Double.parseDouble(getString(name));
  }

  /**
   * Get an optional argument as a double
   * @param name The property name
   * @default The default value for the property if not present
   */
  public double getDouble(String name, double defaultVal) {
    return containsKey(name) ? getDouble(name) : defaultVal;
  }

  /**
   * Read a boolean value from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the boolean value
   */
  public boolean getBoolean(String name, boolean defaultVal) {
    if (!containsKey(name)) {
      return defaultVal;
    } else {
      String v = getProperty(name);
      if (v.compareTo("true") == 0 || v.compareTo("false") == 0) {
        return Boolean.parseBoolean(v);
      } else {
        throw new IllegalArgumentException(name + " has value " + v + " which is not true or false.");
      }
    }
  }

  public boolean getBoolean(String name) {
    return Boolean.parseBoolean(getString(name));
  }

  /**
   * Get a string property, or, if no such property is defined, return the given default value
   */
  public String getString(String name, String defaultVal) {
    return containsKey(name) ? getProperty(name) : defaultVal;
  }

  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  public String getString(String name) {
    if (!containsKey(name)) {
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    } else {
      return getProperty(name);
    }
  }

  public void verify() {
    logger.info("Verifying properties");
    Enumeration keys = props.propertyNames();
    while (keys.hasMoreElements()) {
      Object key = keys.nextElement();
      if (!referenceSet.contains(key)) {
        logger.warn("Property {} is not valid", key);
      } else {
        logger.info("Property {} is overridden to {}", key, props.getProperty(key.toString()));
      }
    }
  }

  public String toString() {
    return props.toString();
  }
}

