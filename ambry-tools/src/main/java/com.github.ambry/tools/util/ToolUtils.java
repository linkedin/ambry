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
package com.github.ambry.tools.util;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * util functions for Ambry tools
 */
public final class ToolUtils {
  public static void validateSSLOptions(OptionSet options, OptionParser parser,
      ArgumentAcceptingOptionSpec<String> sslEnabledDatacentersOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePathOpt, ArgumentAcceptingOptionSpec<String> sslKeystoreTypeOpt,
      ArgumentAcceptingOptionSpec<String> sslTruststorePathOpt,
      ArgumentAcceptingOptionSpec<String> sslKeystorePasswordOpt, ArgumentAcceptingOptionSpec<String> sslKeyPasswordOpt,
      ArgumentAcceptingOptionSpec<String> sslTruststorePasswordOpt) throws Exception {
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
    props.put("clustermap.ssl.enabled.datacenters", sslEnabledDatacenters);
    return props;
  }

  /**
   * Adds cluster map properties with dummy values for tools to function
   * @param properties the {@link Properties} that need to be updated
   */
  public static void addClusterMapProperties(Properties properties) {
    if (properties.getProperty("clustermap.cluster.name") == null) {
      properties.setProperty("clustermap.cluster.name", "dev");
    }
    if (properties.getProperty("clustermap.datacenter.name") == null) {
      properties.setProperty("clustermap.datacenter.name", "DataCenter");
    }
    if (properties.getProperty("clustermap.host.name") == null) {
      properties.setProperty("clustermap.host.name", "localhost");
    }
  }

  /**
   * Ensure that the given argument list has all the required arguments. If not, exit.
   * @param requiredArgs the list of required arguments.
   * @param actualArgs the set of actual arguments.
   * @param parser the {@link OptionParser} used to parse arguments.
   * @throws IOException if there is a problem writing out usage information.
   */
  public static void ensureOrExit(List<OptionSpec> requiredArgs, OptionSet actualArgs, OptionParser parser)
      throws IOException {
    for (OptionSpec opt : requiredArgs) {
      if (!actualArgs.has(opt)) {
        System.err.println("Missing required argument \"" + opt + "\"");
        parser.printHelpOn(System.err);
        System.exit(1);
      }
    }
  }

  /**
   * Ensures that the given argument list has only and exactly the options in the given list and nothing else.
   * @param exactExpectedOptions the exact options expected
   * @param actualOptions the actual options received.
   * @param parser the {@link OptionParser} used to parse arguments.
   * @throws IOException if there is a problem writing out usage information.
   */
  public static void ensureExactOrExit(List<OptionSpec<?>> exactExpectedOptions, List<OptionSpec<?>> actualOptions,
      OptionParser parser) throws IOException {
    if (!new HashSet<>(exactExpectedOptions).equals(new HashSet<>(actualOptions))) {
      System.err.println("***Incompatible options, expected exactly: " + exactExpectedOptions);
      parser.printHelpOn(System.err);
      System.exit(1);
    }
  }

  /**
   * Fetches the properties from Property file and generates the {@link VerifiableProperties}
   * @param args String array containing the arguments passed in
   * @return the {@link VerifiableProperties} generated from the properties in the property file
   * @throws IOException
   */
  public static VerifiableProperties getVerifiableProperties(String[] args) throws IOException {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> propsFileOpt = parser.accepts("propsFile", "Properties file path")
        .withRequiredArg()
        .describedAs("propsFile")
        .ofType(String.class);

    OptionSet options = parser.parse(args);
    String propsFilePath = options.valueOf(propsFileOpt);
    if (propsFilePath == null) {
      parser.printHelpOn(System.err);
      throw new IllegalArgumentException("Missing required arg: propsFile");
    } else {
      Properties properties = Utils.loadProps(propsFilePath);
      addClusterMapProperties(properties);
      return new VerifiableProperties(properties);
    }
  }

  /**
   * Open a file for editing in a terminal text editor. Wait for the editor process to finish before returning.
   * This should only be called from scripts that are run in a terminal as it requires user input.
   * If the {@code EDITOR} environment variable is set, that editor is used, otherwise it defaults to vim.
   * @param filePath the file to edit.
   * @throws IOException
   * @throws InterruptedException
   */
  public static void editFile(Path filePath) throws IOException, InterruptedException {
    if (System.console() == null) {
      throw new IllegalStateException("Script must be run from a terminal to support opening a file in an editor");
    }

    String editor = System.getenv("EDITOR");
    List<String> command = new ArrayList<>();
    if (editor == null) {
      command.add("vim");
    } else {
      // If the EDITOR variable contains extra arguments, we have to split the string into tokens.
      StringTokenizer st = new StringTokenizer(editor);
      while (st.hasMoreTokens()) {
        command.add(st.nextToken());
      }
    }
    command.add(filePath.toAbsolutePath().toString());

    Process process = new ProcessBuilder(command).redirectInput(ProcessBuilder.Redirect.INHERIT)
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .start();
    process.waitFor();
  }

  /**
   * Provide a yes/no prompt to the user for interactive scripts.
   * @param question the question to include in the prompt.
   * @return {@code true} if the user responded with "yes" or "y", or {@code false} if the user responded with "no" or
   *         "n".
   */
  public static boolean yesNoPrompt(String question) {
    if (System.console() == null) {
      throw new IllegalStateException("Script must be run from a terminal to support user prompts");
    }
    while (true) {
      String line = System.console().readLine("%s (y[es]/n[o]): ", question);
      switch (line.toLowerCase()) {
        case "yes":
        case "y":
          return true;
        case "no":
        case "n":
          return false;
        default:
          System.out.println("Please enter either yes, y, no, or n");
      }
    }
  }
}
