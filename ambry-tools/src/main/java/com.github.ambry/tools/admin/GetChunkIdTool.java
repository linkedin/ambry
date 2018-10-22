/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;
import com.github.ambry.store.StoreKey;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


public class GetChunkIdTool {
  private static final Logger logger = LoggerFactory.getLogger(GetChunkIdTool.class);

  public static void main(String args[]) throws Exception {
    BasicConfigurator.configure();
    InvocationOptions options = new InvocationOptions(args);
    Properties properties = Utils.loadProps(options.routerPropsFilePath);
    String blobIdStrs = readStringFromFile(options.blobIdsFilePath);
    String[] blobIdEntries = blobIdStrs.split("\\r?\\n");

    ToolUtils.addClusterMapProperties(properties);
    logger.info("Properties are created!");
    String routerFactoryClass = (String) properties.get("rest.server.router.factory");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            options.hardwareLayoutFilePath, options.partitionLayoutFilePath)).getClusterMap();
    logger.info("ClusterMap is created!");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    RouterFactory routerFactory =
        Utils.getObj(routerFactoryClass, verifiableProperties, clusterMap, new LoggingNotificationSystem(), null, null);
    logger.info("RouterFactory is created!");
    Router router = routerFactory.getRouter();
    logger.info("Router is created!");
    FileWriter chunkFileWriter = new FileWriter(options.chunkBlobFile);
    FileWriter simpleFileWriter = new FileWriter(options.simpleBlobFile);
    FileWriter errorFileWriter = new FileWriter(options.errorFile);
    try (PrintWriter chunkWriter = new PrintWriter(chunkFileWriter);
        PrintWriter simpleWriter = new PrintWriter(simpleFileWriter);
        PrintWriter errorWriter = new PrintWriter(errorFileWriter)) {
      for (String entry : blobIdEntries) {
        String[] fields = entry.split("\\t");
        String blobId = fields[0];
        Future<GetBlobResult> getBlobResultFuture = router.getBlob(blobId,
            new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds)
                .getOption(GetOption.Include_All)
                .build(), null);
        try {
          GetBlobResult getBlobResult = getBlobResultFuture.get();
          if (getBlobResult != null && getBlobResult.getBlobChunkIds() != null) {
            int cnt = 1;
            List<StoreKey> chunkIds = getBlobResult.getBlobChunkIds();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(blobId).append("\t");
            for (StoreKey id : chunkIds) {
              stringBuilder.append(id).append(cnt < chunkIds.size() ? "," : "");
              ++cnt;
            }
            stringBuilder.append("\n");
            chunkWriter.print(stringBuilder.toString());
          } else {
            simpleWriter.print(blobId);
          }
        } catch (ExecutionException e) {
          errorWriter.print(blobId + ": " + e.getCause().getMessage());
          errorWriter.print("\n\n");
        }
      }
      router.close();
    }

    logger.info(
        "Composite blob and ChunkIds are written into file: {} ; Simple BlobIds are written into file: {} ; BlobIds encountered errors are kept in file: {}",
        options.chunkBlobFile, options.simpleBlobFile, options.errorFile);
  }

  private static class InvocationOptions {
    public final String hardwareLayoutFilePath;
    public final String partitionLayoutFilePath;
    public final String routerPropsFilePath;
    public final String blobIdsFilePath;
    public final String chunkBlobFile;
    public final String simpleBlobFile;
    public final String errorFile;
    public final String hostName;
    public final int port;
    public final boolean enabledVerboseLogging;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args the command line argument list.
     * @throws InstantiationException if all required arguments were not provided.
     * @throws IOException if help text could not be printed.
     */
    public InvocationOptions(String args[]) throws InstantiationException, IOException {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> hardwareLayoutFilePath =
          parser.accepts("hardwareLayoutFilePath", "Path to hardware layout file")
              .withRequiredArg()
              .describedAs("hardwareLayoutFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> partitionLayoutFilePath =
          parser.accepts("partitionLayoutFilePath", "Path to partition layout file")
              .withRequiredArg()
              .describedAs("partitionLayoutFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> routerPropsFilePathOpt =
          parser.accepts("routerPropsFilePath", "Path to router properties file")
              .withRequiredArg()
              .describedAs("routerPropsFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> blobIdsFilePath =
          parser.accepts("blobIdsFilePath", "Path to composite blob Ids file")
              .withRequiredArg()
              .describedAs("blobIdsFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> chunkBlobFile =
          parser.accepts("chunkBlobFile", "The file to record composite blob Ids and its chunk Ids")
              .withRequiredArg()
              .describedAs("chunkBlobFile")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> simpleBlobFile =
          parser.accepts("simpleBlobFile", "The file to record simple blobs")
              .withRequiredArg()
              .describedAs("simpleBlobFile")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> errorFile =
          parser.accepts("errorFile", "The file to record blob Ids which encountered errors when fetching chunkIds")
              .withRequiredArg()
              .describedAs("errorFile")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> hostNameOpt =
          parser.accepts("hostName", "The hostname against which requests are to be made")
              .withOptionalArg()
              .describedAs("hostName")
              .defaultsTo("localhost")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<Integer> portNumberOpt =
          parser.accepts("port", "The port number to be used while contacting the host")
              .withOptionalArg()
              .describedAs("port")
              .ofType(Integer.class)
              .defaultsTo(6667);
      ArgumentAcceptingOptionSpec<Boolean> enableVerboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging if set to true")
              .withOptionalArg()
              .describedAs("enableVerboseLogging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
      requiredArgs.add(hardwareLayoutFilePath);
      requiredArgs.add(partitionLayoutFilePath);
      requiredArgs.add(routerPropsFilePathOpt);
      requiredArgs.add(blobIdsFilePath);
      requiredArgs.add(chunkBlobFile);
      requiredArgs.add(simpleBlobFile);
      requiredArgs.add(errorFile);

      OptionSet options = parser.parse(args);
      if (hasRequiredOptions(requiredArgs, options)) {
        this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
        logger.trace("Hardware layout file path: {}", this.hardwareLayoutFilePath);
        this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
        logger.trace("Partition layout file path: {}", this.partitionLayoutFilePath);
        this.routerPropsFilePath = options.valueOf(routerPropsFilePathOpt);
        logger.trace("Router/ClusterMap config file path: {}", this.routerPropsFilePath);
        this.blobIdsFilePath = options.valueOf(blobIdsFilePath);
        logger.trace("Blob Ids file path: {}", this.blobIdsFilePath);
        this.chunkBlobFile = options.valueOf(chunkBlobFile);
        logger.trace("Output composite blob file name: {}", this.chunkBlobFile);
        this.simpleBlobFile = options.valueOf(simpleBlobFile);
        logger.trace("Output simple blob file name: {}", this.simpleBlobFile);
        this.errorFile = options.valueOf(errorFile);
        logger.trace("Error file name: {}", this.errorFile);
      } else {
        parser.printHelpOn(System.err);
        throw new InstantiationException("Did not receive all required arguments for starting RestServer");
      }
      this.hostName = options.valueOf(hostNameOpt);
      this.port = options.valueOf(portNumberOpt);
      this.enabledVerboseLogging = options.valueOf(enableVerboseLoggingOpt);
    }

    /**
     * Checks if all required arguments are present. Prints the ones that are not.
     * @param requiredArgs the list of required arguments.
     * @param options the list of received options.
     * @return whether required options are present.
     */
    private boolean hasRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options) {
      boolean haveAll = true;
      for (OptionSpec opt : requiredArgs) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument " + opt);
          haveAll = false;
        }
      }
      return haveAll;
    }
  }
}
