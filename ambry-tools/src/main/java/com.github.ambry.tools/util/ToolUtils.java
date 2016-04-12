/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.util;

import java.util.ArrayList;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * util functions for ambry tool
 */
public final class ToolUtils {
  public static void validateSSLOptions(OptionSet options, OptionParser parser,
      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePathOpt, ArgumentAcceptingOptionSpec<String> sslKeystoreTypeOpt,
      ArgumentAcceptingOptionSpec<String> sslTruststorePathOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePasswordOpt, ArgumentAcceptingOptionSpec<String> sslKeyPasswordOpt,
      ArgumentAcceptingOptionSpec<String> sslTruststorePasswordOpt)
      throws Exception {
    String sslEnabledDatacenters = options.valueOf(sslEnabledDatacentersOpt);
    if (sslEnabledDatacenters.length() != 0) {
      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(sslKeystorePathOpt);
      listOpt.add(sslKeystoreTypeOpt);
      listOpt.add(sslKeystorePasswordOpt);
      listOpt.add(sslKeyPasswordOpt);
      listOpt.add(sslTruststorePathOpt);
      listOpt.add(sslTruststorePasswordOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("If sslEnabledDatacenters is not empty, missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          throw new Exception("Lack of SSL arguments " + opt);
        }
      }
    }
  }

  public static Properties createSSLProperties(String sslEnabledDatacenters, String sslKeystorePath,
      String sslKeyStoreType, String sslKeystorePassword, String keyPassword, String sslTruststorePath,
      String sslTruststorePassword, String sslCipherSuites) {
    Properties props = new Properties();
    props.put("ssl.context.protocol", "TLS");
    props.put("ssl.context.provider", "SunJSSE");
    props.put("ssl.enabled.protocols", "TLSv1.2");
    props.put("ssl.endpoint.identification.algorithm", "HTTPS");
    props.put("ssl.client.authentication", "required");
    props.put("ssl.keymanager.algorithm", "PKIX");
    props.put("ssl.trustmanager.algorithm", "PKIX");
    props.put("ssl.keystore.type", sslKeyStoreType);
    props.put("ssl.keystore.path", sslKeystorePath);
    props.put("ssl.keystore.password", sslKeystorePassword);
    props.put("ssl.key.password", keyPassword);
    props.put("ssl.truststore.type", "JKS");
    props.put("ssl.truststore.path", sslTruststorePath);
    props.put("ssl.truststore.password", sslTruststorePassword);
    props.put("ssl.cipher.suites", sslCipherSuites);
    props.put("ssl.enabled.datacenters", sslEnabledDatacenters);
    return props;
  }

  public static Properties createConnectionPoolProperties() {
    Properties props = new Properties();
    props.put("connectionpool.read.buffer.size.bytes", "20000000");
    props.put("connectionpool.write.buffer.size.bytes", "20000000");
    props.put("connectionpool.read.timeout.ms", "10000");
    props.put("connectionpool.connect.timeout.ms", "2000");
    return props;
  }
}
