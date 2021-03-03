/*
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
package com.github.ambry.accountstats;

import com.github.ambry.config.AccountStatsMySqlConfig;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A helper class to preprocess hostname before writing to or querying from database.
 */
public class HostnameHelper {
  private static final Logger logger = LoggerFactory.getLogger(HostnameHelper.class);
  private final String[] domainNamesToRemove;
  private final int port;

  /**
   * Constructor to create a {@link HostnameHelper}.
   * @param config The {@link AccountStatsMySqlConfig}.
   * @param port The port registered in clustermap.
   */
  public HostnameHelper(AccountStatsMySqlConfig config, int port) {
    domainNamesToRemove = config.domainNamesToRemove.split(",");
    this.port = port;
    logger.info("Domain names to remove from configuration: " + config.domainNamesToRemove);
    logger.info("Will remove these domain names from host: " + Arrays.toString(domainNamesToRemove));
  }

  /**
   * Simplify and shorten hostname. If the given hostname is fully qualified domain name, like "ambry1.prod.github.com",
   * and the {@link AccountStatsMySqlConfig#domainNamesToRemove} includes ".github.com", then the hostname will be
   * changed to "ambry1.prod_12345". "12345" is the port number passed to the constructor.
   * @param hostname The hostname to simplify
   * @return The simplified hostname.
   */
  public String simplifyHostname(String hostname) {
    return simplifyHostname(hostname, this.port);
  }

  /**
   * Simplify and shorten hostname. If the given hostname is fully qualified domain name, like "ambry1.prod.github.com",
   * and the {@link AccountStatsMySqlConfig#domainNamesToRemove} includes ".github.com", then the hostname will be
   * changed to "ambry1.prod_12345". "12345" is the given {@code port}.
   * @param hostname The hostname to simplify
   * @param port The port number.
   * @return The simplified hostname.
   */
  public String simplifyHostname(String hostname, int port) {
    for (String domainName : domainNamesToRemove) {
      if (domainName != null && !(domainName = domainName.trim()).isEmpty()) {
        if (domainName.charAt(0) != '.') {
          domainName = "." + domainName;
        }
        hostname = hostname.replace(domainName, "");
      }
    }
    return String.format("%s_%d", hostname, port);
  }
}
