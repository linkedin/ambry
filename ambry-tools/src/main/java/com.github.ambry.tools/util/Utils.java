package com.github.ambry.tools.util;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * util functions for ambry tool
 */
public final class Utils {
  public static void validateSSLOptions(OptionSet options, OptionParser parser,
      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePathOpt, ArgumentAcceptingOptionSpec<String> sslTruststorePathOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePasswordOpt,
      ArgumentAcceptingOptionSpec<String> sslKeyPasswordOpt,
      ArgumentAcceptingOptionSpec<String> sslTruststorePasswordOpt)
      throws IOException {
    String sslEnabledDatacenters = options.valueOf(sslEnabledDatacentersOpt);
    if (sslEnabledDatacenters.length() != 0) {
      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(sslKeystorePathOpt);
      listOpt.add(sslKeystorePasswordOpt);
      listOpt.add(sslKeyPasswordOpt);
      listOpt.add(sslTruststorePathOpt);
      listOpt.add(sslTruststorePasswordOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(sslKeystorePathOpt)) {
          System.err.println("If sslEnabledDatacentersOpt is not empty, missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }
    }
  }

  public static Properties createSSLProperties(String sslEnabledDatacenters, String sslKeystorePath,
      String sslKeystorePassword, String keyPassword, String sslTruststorePath, String sslTruststorePassword) {
    Properties props = new Properties();
    props.put("ssl.context.protocol", "TLS");
    props.put("ssl.context.provider", "SunJSSE");
    props.put("ssl.enabled.protocols", "TLSv1.2");
    props.put("ssl.endpoint.identification.algorithm", "HTTPS");
    props.put("ssl.client.authentication", "required");
    props.put("ssl.keymanager.algorithm", "PKIX");
    props.put("ssl.trustmanager.algorithm", "PKIX");
    props.put("ssl.keystore.type", "JKS");
    props.put("ssl.keystore.path", sslKeystorePath);
    props.put("ssl.keystore.password", sslKeystorePassword);
    props.put("ssl.key.password", keyPassword);
    props.put("ssl.truststore.type", "JKS");
    props.put("ssl.truststore.path", sslTruststorePath);
    props.put("ssl.truststore.password", sslTruststorePassword);
    props.put("ssl.cipher.suites", "TLS_RSA_WITH_AES_128_CBC_SHA");
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
