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
package com.github.ambry.server.mysql;

import com.github.ambry.config.AccountStatsMySqlConfig;


/**
 * A helper class to preprocess hostname before writing to or querying from database.
 */
public class HostnameHelper {
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
  }

  /**
   * Simplify and shorten hostname. If the given hostname is fully qualified domain name, like "ambry1.prod.github.com",
   * and the {@link AccountStatsMySqlConfig#domainNamesToRemove} includes ".github.com", then the hostname will be
   * changed to "ambry1.prod_12345". "12345" is the port number.
   * @param hostname The hostname to simplify
   * @return The simplified hostname.
   */
  public String simplifyHostname(String hostname) {
    for (String domainName : domainNamesToRemove) {
      if (domainName.charAt(0) != '.') {
        domainName = "." + domainName;
      }
      hostname = hostname.replace(domainName, "");
    }
    return String.format("%s_%d", hostname, port);
  }
}
