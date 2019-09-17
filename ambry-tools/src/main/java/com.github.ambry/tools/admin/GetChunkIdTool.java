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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouter;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.router.RouterFactory;
import com.github.ambry.router.RouterUtils;
import com.github.ambry.store.StoreKey;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private static final String PARTITION_FILE = "/Users/yinzhang/partitionOnLor1.txt";
  private static Map<Byte, String> idToDatacenter = new HashMap<>();

  static {
    idToDatacenter.put((byte) 1, "prod-ltx1");
    idToDatacenter.put((byte) 2, "prod-lva1");
    idToDatacenter.put((byte) 3, "prod-lsg1");
    idToDatacenter.put((byte) 4, "prod-lor1");
  }

  public static void main(String args[]) throws Exception {
    BasicConfigurator.configure();
    InvocationOptions options = new InvocationOptions(args);
    Properties toolProperties = Utils.loadProps(options.toolConfigFilePath);
    ToolConfig toolConfig = new ToolConfig(new VerifiableProperties(toolProperties));
    Properties routerProperties = Utils.loadProps(options.routerConfigFilePath);
    String blobIdStrs = readStringFromFile(toolConfig.blobIdsFilePath);
    boolean processBlobIdOnly = toolConfig.typeOfOperation == Operation.ProcessBlobId;

    String[] blobIdEntries = blobIdStrs.split("\\r?\\n");
    ToolUtils.addClusterMapProperties(routerProperties);
    logger.info("Properties are created!");
    String routerFactoryClass = (String) routerProperties.get("rest.server.router.factory");
    String targetHostname = (String) routerProperties.get("router.target.hostname");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(routerProperties));
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            toolConfig.hardwareLayoutFilePath, toolConfig.partitionLayoutFilePath)).getClusterMap();
    logger.info("ClusterMap is created!");
    VerifiableProperties verifiableProperties = new VerifiableProperties(routerProperties);
    Router router = null;
    if (!processBlobIdOnly) {
      RouterFactory routerFactory =
          Utils.getObj(routerFactoryClass, verifiableProperties, clusterMap, new LoggingNotificationSystem(), null,
              null);
      logger.info("RouterFactory is created!");
      router = routerFactory.getRouter();
      logger.info("Router is created!");
    }
    FileWriter chunkFileWriter = new FileWriter(toolConfig.chunkBlobFile);
    FileWriter simpleFileWriter = new FileWriter(toolConfig.simpleBlobFile, true);
    FileWriter errorFileWriter = new FileWriter(toolConfig.errorFile);
    try (PrintWriter chunkWriter = new PrintWriter(chunkFileWriter);
        PrintWriter simpleWriter = new PrintWriter(simpleFileWriter);
        PrintWriter errorWriter = new PrintWriter(errorFileWriter)) {
      CountDownLatch deleteCountDown = null;
      for (String entry : blobIdEntries) {
        String blobIdStr = null;
        String hostnameForDeletion = null;
        long expectSize = -1;
        String[] fields = entry.split("\\t");
        if (fields.length == 1 && entry.startsWith("\"")) {
          int index = entry.indexOf(',');
          blobIdStr = entry.substring(1, index - 1);
          String property = entry.substring(index + 2, entry.length() - 1);
          String[] blobFields = property.split(",");
          expectSize = Long.valueOf(blobFields[7].split(":")[1]);
        } else {
          blobIdStr = fields[0];
          if (fields.length > 1) {
            hostnameForDeletion = fields[1];
          }
        }
        BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
        Pair<Short, Short> idPair = BlobId.getAccountAndContainerIds(blobIdStr);
        boolean isEncrypted = BlobId.isEncrypted(blobIdStr);
        BlobId.BlobDataType blobDataType = blobId.getBlobDataType();
        String acctId = String.valueOf(idPair.getFirst());
        String contId = String.valueOf(idPair.getSecond());
        String partitionId = blobId.getPartition().toPathString();
        Byte datacenterId = blobId.getDatacenterId();
        short version = blobId.getVersion();
        boolean getBlobInfoOnly = version == (short) 5 && isEncrypted && blobDataType == BlobId.BlobDataType.DATACHUNK;
        if (processBlobIdOnly) {
          // NOTE: The processed result will be written into SimpleFile.
          //getBlobIdInfo(PARTITION_FILE);
          StringBuilder sb = new StringBuilder();
          sb.append(blobIdStr)
              .append("\tpartition=")
              .append(partitionId)
              .append("\tdc=")
              .append(idToDatacenter.get(datacenterId))
              .append("\tdataType=")
              .append(blobDataType)
              .append("\tisEncrypted=")
              .append(isEncrypted)
              .append("\tacctId=")
              .append(acctId)
              .append("\tcontId=")
              .append(contId);
          simpleWriter.println(sb.toString());
        } else if (toolConfig.typeOfOperation == Operation.GetDataChunkId) {
          logger.info("Partition Id is {}", partitionId);
          if (Integer.valueOf(partitionId) >= 1830 && Integer.valueOf(partitionId) <= 3943) {
            logger.info("Partition Id is in range of [1830, 3943] for phase1 nodes, hence skipping it");
            continue;
          }
          logger.trace("Account id is {}, Container id is {}", acctId, contId);
          List<? extends ReplicaId> replicas = blobId.getPartition().getReplicaIds();
          if (targetHostname != null && !targetHostname.isEmpty()) {
            replicas = replicas.stream()
                .filter(replica -> replica.getDataNodeId().getHostname().equals(targetHostname))
                .collect(Collectors.toList());
          }
          for (ReplicaId replicaId : replicas) {
            String targetHost = replicaId.getDataNodeId().getHostname();
            if (targetHost.equals("lva1-app30136.prod.linkedin.com") || targetHost.equals(
                "lsg1-app15923.prod.linkedin.com") || targetHost.equals("lsg1-app15947.prod.linkedin.com")) {
              logger.info("Skipping the unreachable node {}", targetHost);
              continue;
            }
            ((NonBlockingRouter) router).setTargetHost(targetHost);
            Future<GetBlobResult> getBlobResultFuture = null;
            if (getBlobInfoOnly) {
              getBlobResultFuture = router.getBlob(blobIdStr,
                  new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo)
                      .getOption(GetOption.None)
                      .build(), null);
            } else {
              getBlobResultFuture = router.getBlob(blobIdStr,
                  new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds)
                      .getOption(GetOption.None)
                      .build(), null);
            }
            StringBuilder stringBuilder = new StringBuilder();
            try {
              GetBlobResult getBlobResult = getBlobResultFuture.get();
              Exception exception = ((FutureResult<GetBlobResult>) getBlobResultFuture).error();
              if (getBlobResult != null) {
                long blobSize = getBlobResult.getBlobInfo().getBlobProperties().getBlobSize();
                logger.info("the server hostname: {}", targetHostname);
                logger.info("x-ambry-blob-size: {}", blobSize);
                if (getBlobResult.getBlobChunkIds() != null) {
                  // Composite blob
                  boolean versionIsRight = true;
                  int cnt = 1;
                  List<StoreKey> chunkIds = getBlobResult.getBlobChunkIds();
                  generateLogMsg(stringBuilder, blobIdStr, blobDataType, acctId, contId, partitionId, targetHost);
                  stringBuilder.append("\tblobSize=")
                      .append(blobSize)
                      .append("\tdc=")
                      .append(idToDatacenter.get(datacenterId))
                      .append("\tencryption=")
                      .append(isEncrypted)
                      .append("\t");
                  for (StoreKey id : chunkIds) {
                    versionIsRight = versionIsRight && ((BlobId) id).getVersion() == version;
                    if (version < (short) 5) {
                      versionIsRight = versionIsRight && BlobId.isCrafted(id.getID());
                    }
                    stringBuilder.append(id).append(cnt < chunkIds.size() ? "," : "");
                    ++cnt;
                  }
                  stringBuilder.append("\tversionCheck=").append(versionIsRight);
                  if (expectSize >= 0) {
                    stringBuilder.append("\tblobSizeCheck=").append(expectSize == blobSize);
                  }
                  chunkWriter.println(stringBuilder.toString());
                } else {
                  // Simple blob
                  generateLogMsg(stringBuilder, blobIdStr, blobDataType, acctId, contId, partitionId, targetHost);
                  stringBuilder.append("\tblobSize=")
                      .append(blobSize)
                      .append("\tdc=")
                      .append(idToDatacenter.get(datacenterId))
                      .append("\tencryption=")
                      .append(isEncrypted)
                      .append("\tblobSizeCheck=")
                      .append(expectSize == blobSize);
                  simpleWriter.println(stringBuilder.toString());
                }
              } else {
                RouterErrorCode errorCode = ((RouterException) exception).getErrorCode();
                stringBuilder.append(",partition=")
                    .append(partitionId)
                    .append(",dc=")
                    .append(idToDatacenter.get(datacenterId))
                    .append(",  ")
                    .append(errorCode);
                int bracketIndex = entry.indexOf("}");
                bracketIndex = bracketIndex < 0 ? entry.length() - 1 : bracketIndex;
                errorWriter.println(entry.substring(0, bracketIndex + 1) + stringBuilder.toString() + "\"");
              }
            } catch (ExecutionException e) {
              stringBuilder.append(" ,partition=")
                  .append(partitionId)
                  .append(",dc=")
                  .append(idToDatacenter.get(datacenterId))
                  .append(",  ")
                  .append(targetHost)
                  .append(",  ")
                  .append(e.getCause().getMessage());
              int bracketIndex = entry.indexOf("}");
              bracketIndex = bracketIndex < 0 ? entry.length() - 1 : bracketIndex;
              errorWriter.println(entry.substring(0, bracketIndex + 1) + stringBuilder.toString() + "\"");
            }
          }
        } else if (toolConfig.typeOfOperation == Operation.DeleteBlob) {
          if (hostnameForDeletion != null && !hostnameForDeletion.isEmpty()) {
            logger.info("Starting to delete the blob");
            deleteCountDown = new CountDownLatch(1);
            ((NonBlockingRouter) router).setTargetHost(hostnameForDeletion);
            DeleteCallback callback =
                new DeleteCallback(blobIdStr, hostnameForDeletion, simpleWriter, errorWriter, deleteCountDown);
            router.deleteBlob(blobIdStr, null, callback);
            deleteCountDown.await();
          }
        }
      }
      if (!processBlobIdOnly && router != null) {
        router.close();
      }
    }
    logger.info(
        "Composite blob and ChunkIds are written into file: {} ; Simple BlobIds are written into file: {} ; BlobIds encountered errors are kept in file: {}",
        toolConfig.chunkBlobFile, toolConfig.simpleBlobFile, toolConfig.errorFile);
  }

  private static class DeleteCallback implements Callback<Void> {
    public Exception exception;
    private String blobIdStr;
    private String targetHost;
    private PrintWriter simpleWriter;
    private PrintWriter errorWriter;
    private CountDownLatch latch;

    DeleteCallback(String blobIdStr, String targetHost, PrintWriter simpleWriter, PrintWriter errorWriter,
        CountDownLatch latch) {
      this.blobIdStr = blobIdStr;
      this.targetHost = targetHost;
      this.simpleWriter = simpleWriter;
      this.errorWriter = errorWriter;
      this.latch = latch;
    }

    @Override
    public void onCompletion(Void routerResult, Exception routerException) {
      this.exception = routerException;
      StringBuilder sb = new StringBuilder(blobIdStr);
      if (routerException != null) {
        sb.append("\t").append(targetHost).append("\t").append(routerException);
        errorWriter.println(sb.toString());
      } else {
        sb.append("\t").append(targetHost).append("\t").append("Deletion succeeded");
        simpleWriter.println(sb.toString());
      }
      latch.countDown();
    }
  }

  private static void generateLogMsg(StringBuilder strBuilder, String blobId, BlobId.BlobDataType blobDataType,
      String acctId, String contId, String partitionId, String targetHost) {
    strBuilder.append(blobId)
        .append("\t")
        .append(blobDataType)
        .append("\tacct=")
        .append(acctId)
        .append("\tcont=")
        .append(contId)
        .append("\tpartition=")
        .append(partitionId)
        .append("\t")
        .append(targetHost);
  }

  private enum Operation {
    ProcessBlobId, GetDataChunkId, DeleteBlob
  }

  private static class ToolConfig {

    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    @Config("blob.ids.file.path")
    final String blobIdsFilePath;

    @Config("chunk.blob.file")
    final String chunkBlobFile;

    @Config("simple.blob.file")
    final String simpleBlobFile;

    @Config("error.file")
    final String errorFile;

    @Config("type.of.operation")
    final Operation typeOfOperation;

    ToolConfig(VerifiableProperties verifiableProperties) {
      typeOfOperation = Operation.valueOf(verifiableProperties.getString("type.of.operation"));
      errorFile = verifiableProperties.getString("error.file");
      simpleBlobFile = verifiableProperties.getString("simple.blob.file");
      chunkBlobFile = verifiableProperties.getString("chunk.blob.file");
      blobIdsFilePath = verifiableProperties.getString("blob.ids.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    }
  }

  private static class InvocationOptions {
    public final String routerConfigFilePath;
    public final String toolConfigFilePath;
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
      ArgumentAcceptingOptionSpec<String> toolConfigFilePathOpt =
          parser.accepts("toolConfigFilePath", "Path to tool config file")
              .withRequiredArg()
              .describedAs("routerConfigFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> routerConfigFilePathOpt =
          parser.accepts("routerConfigFilePath", "Path to router config file")
              .withRequiredArg()
              .describedAs("routerConfigFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<Boolean> enableVerboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging if set to true")
              .withOptionalArg()
              .describedAs("enableVerboseLogging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
      requiredArgs.add(toolConfigFilePathOpt);
      requiredArgs.add(routerConfigFilePathOpt);

      OptionSet options = parser.parse(args);
      if (hasRequiredOptions(requiredArgs, options)) {
        this.routerConfigFilePath = options.valueOf(routerConfigFilePathOpt);
        logger.trace("Router/ClusterMap config file path: {}", this.routerConfigFilePath);
        this.toolConfigFilePath = options.valueOf(toolConfigFilePathOpt);
        logger.trace("Get chunk id tool config file path: {}", this.toolConfigFilePath);
      } else {
        parser.printHelpOn(System.err);
        throw new InstantiationException("Did not receive all required arguments for starting RestServer");
      }
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

  private static Set<String> getPartitionSet(String partitionFile) throws Exception {
    Set<String> partitions = new HashSet<>();
    try (Stream<String> stream = Files.lines(Paths.get(partitionFile))) {
      stream.forEach((s) -> {
        String[] arr = s.split("\\s{2}");
        for (int i = 0; i < arr.length - 1; ++i) {
          partitions.add(arr[i]);
        }
      });
    }
    return partitions;
  }

  private static void getBlobIdInfo(String partitionFile) throws Exception {
    //Set<String> partitions = new HashSet<>();
    try (Stream<String> stream = Files.lines(Paths.get(partitionFile))) {
      String str = stream.findFirst().get();
      int index = str.indexOf(',');
      String id = str.substring(1, index - 1);
      String property = str.substring(index + 2, str.length() - 1);
      index = property.indexOf("creationtime");
      property = property.substring(index, property.length() - 1);
      String[] properties = property.split(",");
      System.out.println(id);
      System.out.println(properties[0]);
      System.out.println(properties[1]);
      System.out.println(properties[2]);
      System.out.println(properties[properties.length - 1]);
    }
  }
}
