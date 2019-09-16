/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool that can be used to make sure an Ambry storage node is safe for shutdown.
 * </p>
 * This is not meant for cases where the node is going to be restarted without changing any data (new code deployments,
 * config changes etc). This is meant for cases where a node is going down for maintenance and it is useful to make
 * sure that all the data in it has been successfully replicated. For example, this tool can be used before a node
 * is shutdown to run {@link com.github.ambry.store.DiskReformatter} on it.
 * <p/>
 * Note: The tool does not actually shutdown the node - it simply makes sure that all its peers have caught up
 * completely.
 */
public class SafeServerShutdownTool {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerAdminTool.class);

  private final ServerAdminTool serverAdminTool;
  private final Time time;

  private static class SafeServerShutdownToolConfig {

    private static final String DEFAULT_PORT_STRING = "6667";
    private static final long DEFAULT_LOG_GROWTH_PAUSE_THRESHOLD = 20 * 1024 * 1024;
    private static final String DEFAULT_CHECK_REPEAT_DELAY_SECS_STRING = "5";

    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    /**
     * The hostname of the target server as it appears in the partition layout.
     */
    @Config("hostname")
    @Default("localhost")
    final String hostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to).
     */
    @Config("port")
    @Default(DEFAULT_PORT_STRING)
    final int port;

    /**
     * The peers' lag at which the node targeted will shut off all operations that cause log growth in order to allow
     * peers to completely catch up.
     */
    @Config("log.growth.pause.lag.threshold.bytes")
    @Default("20 * 1024 * 1024")
    final long logGrowthPauseLagThresholdBytes;

    /**
     * The number of replicas of each partition that have to be caught up during catchup status checks.
     * The min of this value or the total count of replicas -1 is considered.
     */
    @Config("num.replicas.caught.up.per.partition")
    @Default("Short.MAX_VALUE")
    final short numReplicasCaughtUpPerPartition;

    /**
     * The amount of time in (seconds) to wait before declaring safe shutdown as not possible.
     */
    @Config("timeout.secs")
    @Default("Long.MAX_VALUE")
    final long timeoutSecs;

    /**
     * The amount of time in (seconds) to wait b/w checks for catchup status.
     */
    @Config("check.repeat.delay.secs")
    @Default(DEFAULT_CHECK_REPEAT_DELAY_SECS_STRING)
    final long checkRepeatDelaySecs;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    SafeServerShutdownToolConfig(VerifiableProperties verifiableProperties) {

      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getIntInRange("port", Integer.valueOf(DEFAULT_PORT_STRING), Utils.MIN_PORT_NUM,
          Utils.MAX_PORT_NUM);
      logGrowthPauseLagThresholdBytes = verifiableProperties.getLongInRange("log.growth.pause.lag.threshold.bytes",
          DEFAULT_LOG_GROWTH_PAUSE_THRESHOLD, 0, Long.MAX_VALUE);
      numReplicasCaughtUpPerPartition =
          verifiableProperties.getShortInRange("num.replicas.caught.up.per.partition", Short.MAX_VALUE, (short) 1,
              Short.MAX_VALUE);
      timeoutSecs = verifiableProperties.getLongInRange("timeout.secs", Long.MAX_VALUE, 0, Long.MAX_VALUE);
      checkRepeatDelaySecs = verifiableProperties.getLongInRange("check.repeat.delay.secs",
          Long.valueOf(DEFAULT_CHECK_REPEAT_DELAY_SECS_STRING), 0, Long.MAX_VALUE);
    }
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    SafeServerShutdownToolConfig config = new SafeServerShutdownToolConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    try (ClusterMap clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory,
        clusterMapConfig, config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap()) {
      SSLFactory sslFactory = !clusterMapConfig.clusterMapSslEnabledDatacenters.isEmpty() ? SSLFactory.getNewInstance(
          new SSLConfig(verifiableProperties)) : null;
      try (ServerAdminTool serverAdminTool = new ServerAdminTool(clusterMap, sslFactory, verifiableProperties)) {
        DataNodeId dataNodeId = clusterMap.getDataNodeId(config.hostname, config.port);
        if (dataNodeId == null) {
          throw new IllegalArgumentException(
              "Could not find a data node corresponding to " + config.hostname + ":" + config.port);
        }
        SafeServerShutdownTool safeServerShutdownTool =
            new SafeServerShutdownTool(serverAdminTool, SystemTime.getInstance());
        int exitStatus =
            safeServerShutdownTool.prepareServerForShutdown(dataNodeId, config.logGrowthPauseLagThresholdBytes,
                config.numReplicasCaughtUpPerPartition, config.timeoutSecs, config.checkRepeatDelaySecs) ? 0 : 1;
        System.exit(exitStatus);
      }
    }
  }

  /**
   * @param serverAdminTool the {@link ServerAdminTool} to use to make requests.
   * @param time the {@link Time} instance to use for the timeout.
   */
  public SafeServerShutdownTool(ServerAdminTool serverAdminTool, Time time) {
    this.serverAdminTool = serverAdminTool;
    this.time = time;
  }

  /**
   * Prepares a server for safe shutdown by ensuring that all its peers have caught up to it. Does this by
   * 1. Ensuring that all peers are within {@code logGrowthPauseLagThresholdBytes} for all partitions on
   * {@code dataNodeId}
   * 2. Disabling PUT, DELETE and inbound replication on {@code dataNodeId}.
   * 3. Ensuring that all peers are fully caught up (no lag) for all partitions on {@code dataNodeId}.
   * <p/>
   * If the operation fails after #2, an attempt is made to undo the changes in #2. If that fails, an
   *  {@link IllegalStateException} will be thrown.
   * @param dataNodeId the {@link DataNodeId} that has to be prepared for shutdown.
   * @param logGrowthPauseLagThresholdBytes the peer lag (in bytes) at which PUT, DELETE and inbound replication will
   *                                        be shut off on {@code dataNodeId}.
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within {@code acceptableLagInBytes}
   *                                        (per partition). The min of this value or the total count of replicas - 1 is
   *                                        considered.
   * @param timeoutSecs the number of seconds after which the operation will be considered failed.
   * @param checkRepeatDelaySecs the number of seconds after which the catchup status will be checked again if the
   *                             previous check did not succeed.
   * @return {@code true} if the server is safe for shutdown (all peers are fully caught up). {@code false} otherwise.
   * @throws IllegalStateException if the operation failed and the reset of server state failed.
   * @throws InterruptedException if there are any interruptions while waiting for catch up.
   * @throws IOException if there is any I/O error.
   * @throws TimeoutException if there are timeout errors while making network requests.
   */
  public boolean prepareServerForShutdown(DataNodeId dataNodeId, long logGrowthPauseLagThresholdBytes,
      short numReplicasCaughtUpPerPartition, long timeoutSecs, long checkRepeatDelaySecs)
      throws InterruptedException, IOException, TimeoutException {
    long startTimeSecs = time.seconds();
    boolean success = false;
    // 1. Make sure the peers of the node have caught up till the logGrowthPauseLagThresholdBytes
    LOGGER.info("Waiting until peers of {} are within {} bytes for all partitions", dataNodeId,
        logGrowthPauseLagThresholdBytes);
    if (waitTillCatchupOrTimeout(dataNodeId, logGrowthPauseLagThresholdBytes, numReplicasCaughtUpPerPartition,
        startTimeSecs + timeoutSecs, checkRepeatDelaySecs)) {
      // 2. Disable all requests that result in log growth (PUT, DELETE and inbound replication).
      LOGGER.info("Disabling PUT, DELETE and inbound replication on {} for all partitions", dataNodeId);
      if (controlLogGrowth(dataNodeId, false)) {
        // 3. Make sure the peers of the node have completely caught up
        LOGGER.info("Waiting until peers of {} have completely caught up for all partitions", dataNodeId);
        success = waitTillCatchupOrTimeout(dataNodeId, 0, numReplicasCaughtUpPerPartition, startTimeSecs + timeoutSecs,
            checkRepeatDelaySecs);
      }
    }
    LOGGER.info("Server safe for shutdown: {}", success);
    if (!success && !controlLogGrowth(dataNodeId, true)) {
      throw new IllegalStateException("Could not reset state of " + dataNodeId + ". Please reset manually");
    }
    return success;
  }

  /**
   * Waits until the lag of all peers of {@code dataNodeId} for all partitions <=
   * {@code acceptableLagInBytes} or until time moves past {@code timeoutAtSecs}.
   * @param dataNodeId the {@link DataNodeId} to ask for peer lag details.
   * @param acceptableLagInBytes that lag in bytes that is considered "caught up".
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within {@code acceptableLagInBytes}
   *                                        (per partition). The min of this value or the total count of replicas - 1 is
   *                                        considered.
   * @param timeoutAtSecs the absolute time (in secs) at which the wait is considered failed.
   * @param checkRepeatDelaySecs the number of seconds after which the catchup status will be checked again if the
   *                             previous check did not succeed.
   * @return if the server's peers are confirmed to be within {@code acceptableLagInBytes}. {@code false} otherwise.
   * @throws InterruptedException if there are any interruptions while waiting for catch up.
   * @throws IOException if there is any I/O error.
   * @throws TimeoutException if there are timeout errors while making network requests.
   */
  private boolean waitTillCatchupOrTimeout(DataNodeId dataNodeId, long acceptableLagInBytes,
      short numReplicasCaughtUpPerPartition, long timeoutAtSecs, long checkRepeatDelaySecs)
      throws InterruptedException, IOException, TimeoutException {
    boolean isCaughtUp;
    do {
      isCaughtUp = serverAdminTool.isCaughtUp(dataNodeId, null, acceptableLagInBytes, numReplicasCaughtUpPerPartition)
          .getSecond();
      if (!isCaughtUp) {
        // sleep for a configured amount of time before trying again.
        time.sleep(TimeUnit.SECONDS.toMillis(checkRepeatDelaySecs));
      }
    } while (!isCaughtUp && time.seconds() < timeoutAtSecs);
    return isCaughtUp;
  }

  /**
   * Disables/enables requests (PUT, DELETE, inbound replication) on {@code dataNodeId} that cause the logs the grow.
   * @param dataNodeId the {@link DataNodeId} where log growth has to be controlled.
   * @param enableGrowth {@code true} if the requests need to be enabled, {@code false} if they need to be disabled.
   * @return {@code true} if the requested operation succeeded. {@code false} otherwise.
   * @throws IOException if there is any I/O error.
   * @throws TimeoutException if there are timeout errors while making network requests.
   */
  private boolean controlLogGrowth(DataNodeId dataNodeId, boolean enableGrowth) throws IOException, TimeoutException {
    return serverAdminTool.controlRequest(dataNodeId, null, RequestOrResponseType.PutRequest, enableGrowth)
        == ServerErrorCode.No_Error
        && serverAdminTool.controlRequest(dataNodeId, null, RequestOrResponseType.DeleteRequest, enableGrowth)
        == ServerErrorCode.No_Error
        && serverAdminTool.controlReplication(dataNodeId, null, Collections.EMPTY_LIST, enableGrowth)
        == ServerErrorCode.No_Error;
  }
}
