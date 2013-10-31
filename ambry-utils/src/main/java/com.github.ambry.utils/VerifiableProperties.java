package com.github.ambry.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

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

  boolean containsKey(String name) {
    return props.containsKey(name);
  }

  String getProperty(String name) {
    String value = props.getProperty(name);
    referenceSet.add(name);
    return value;
  }

  /**
   * Read a required integer property value or throw an exception if no such property is found
   */
  int getInt(String name) {
    return Integer.parseInt(getString(name));
  }

  int getIntInRange(String name, int start, int end) {
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
  int getInt(String name, int defaultVal) {
    return getIntInRange(name, defaultVal, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  Short getShort(String name, Short defaultVal) {
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
  int getIntInRange(String name, int defaultVal, int start, int end) {
    int v = 0;
    if(containsKey(name))
      v = Integer.parseInt(getProperty(name));
    else
      v = defaultVal;
    if (v >= start && v <= end)
      return v;
    else
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
  }

  Short getShortInRange(String name, Short defaultVal, Short start, Short end) {
    Short v = 0;
    if(containsKey(name))
      v = Short.parseShort(getProperty(name));
    else
      v = defaultVal;
    if (v >= start && v <= end)
      return v;
    else
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
  }

  /**
   * Read a required long property value or throw an exception if no such property is found
   */
  long getLong(String name) {
    return Long.parseLong(getString(name));
  }

  /**
   * Read an long from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the long value
   */
  long getLong(String name, long defaultVal) {
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
  long getLongInRange(String name, long defaultVal, long start, long end) {
    long v = 0;
    if(containsKey(name))
      v = Long.parseLong(getProperty(name));
    else
      return defaultVal;
    if (v >= start && v <= end)
      return v;
    else
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end + ".");
  }

  /**
   * Get a required argument as a double
   * @param name The property name
   * @return the value
   * @throw IllegalArgumentException If the given property is not present
   */
   double getDouble(String name) {
     return Double.parseDouble(getString(name));
   }

  /**
   * Get an optional argument as a double
   * @param name The property name
   * @default The default value for the property if not present
   */
  double getDouble(String name, double defaultVal) {
    if(containsKey(name))
      return getDouble(name);
    else
      return defaultVal;
  }

  /**
   * Read a boolean value from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the boolean value
   */
  boolean getBoolean(String name, boolean defaultVal) {
    String v = "";
    if(!containsKey(name))
      return defaultVal;
    else {
      v = getProperty(name);
      if (v.compareTo("true") == 0 || v.compareTo("false") == 0)
        return Boolean.parseBoolean(v);
      else
        throw new IllegalArgumentException(name + " has value " + v + " which is not true or false.");
    }
  }

  boolean getBoolean(String name) {
    return Boolean.parseBoolean(getString(name));
  }

  /**
   * Get a string property, or, if no such property is defined, return the given default value
   */
  String getString(String name, String defaultVal) {
    if(containsKey(name))
      return getProperty(name);
    else
      return defaultVal;
  }

  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  String getString(String name) {
    if (!containsKey(name))
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    else
      return getProperty(name);
  }

  public void verify() {
    logger.info("Verifying properties");
    Enumeration keys = props.propertyNames();
    while (keys.hasMoreElements()) {
      Object key = keys.nextElement();
      if (!referenceSet.contains(key))
        logger.warn("Property {} is not valid", key);
      else
        logger.info("Property {} is overridden to {}",key, props.getProperty(key.toString()));
    }
  }

  public String toString() {
    return props.toString();
  }
}

