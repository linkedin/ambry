package com.github.ambry.rest;

import com.github.ambry.config.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Holds common configuration that might be required by all implementations of rest servers
 */
public abstract class RestServerConfig {
  // config keys
  protected Logger logger = LoggerFactory.getLogger(getClass());

  protected RestServerConfig(VerifiableProperties verifiableProperties) {
  }
}
