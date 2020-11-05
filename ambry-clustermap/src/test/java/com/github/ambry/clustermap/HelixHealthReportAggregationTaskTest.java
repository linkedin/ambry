/**
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
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Unit test class for {@link HelixHealthReportAggregatorTask}.
 */
@RunWith(Parameterized.class)
public class HelixHealthReportAggregationTaskTest {
  private static final String CLUSTER_NAME = "Ambry-test";
  private static final String INSTANCE_NAME = "helix.ambry.com";
  private static final long RELEVANT_PERIOD_IN_MINUTES = 60;
  private static final String HEALTH_REPORT_NAME_ACCOUNT = "AccountReport";
  private static final String STATS_FIELD_NAME_ACCOUNT = "AccountStats";
  private static final String HEALTH_REPORT_NAME_PARTITION = "PartitionClassReport";
  private static final String STATS_FIELD_NAME_PARTITION = "PartitionClassStats";
  private MockHelixManager mockHelixManager;
  private HelixHealthReportAggregatorTask task;
  private MockTime mockTime;
  private MockHelixAdmin mockHelixAdmin;
  private ClusterMapConfig clusterMapConfig;
  private TestUtils.ZkInfo zkInfo;
  private final ObjectMapper mapper = new ObjectMapper();

  private final boolean enableAggregatedMonthlyAccountReport;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public HelixHealthReportAggregationTaskTest(boolean enableAggregatedMonthlyAccountReport) throws IOException {
    this.enableAggregatedMonthlyAccountReport = enableAggregatedMonthlyAccountReport;
    File tempDir = Files.createTempDirectory("HelixHealthReportAggregationTask-" + new Random().nextInt(1000)).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    int zkPort = 13188;
    int maxRetries = 1000;
    int numRetries = 0;
    while (true) {
      if (numRetries > maxRetries) {
        throw new IOException("No ports are available to start zookeeper server");
      }
      try {
        zkInfo = new TestUtils.ZkInfo(tempDirPath, "DC1", (byte) 0, zkPort, true);
        break;
      } catch (Exception e) {
        if (e.getCause() instanceof BindException) {
          System.out.println("Port " + zkPort + " already in use, try " + (zkPort + 1) + " instead");
          zkPort++;
          numRetries++;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    String zkAddr = "localhost:" + zkInfo.getPort();

    mockHelixAdmin = new MockHelixAdminFactory().getHelixAdmin(zkAddr);
    mockHelixManager =
        new MockHelixManager(INSTANCE_NAME, InstanceType.PARTICIPANT, zkAddr, CLUSTER_NAME, mockHelixAdmin, null, null);
    mockTime = new MockTime(SystemTime.getInstance().milliseconds());
    clusterMapConfig = makeClusterMapConfig();
  }

  @After
  public void after() {
    zkInfo.shutdown();
  }

  private ClusterMapConfig makeClusterMapConfig() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", INSTANCE_NAME);
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty(ClusterMapConfig.ENABLE_AGGREGATED_MONTHLY_ACCOUNT_REPORT,
        Boolean.toString(this.enableAggregatedMonthlyAccountReport));
    return new ClusterMapConfig(new VerifiableProperties(props));
  }

  /**
   * Test {@link HelixHealthReportAggregatorTask#run()} method.
   * @throws Exception
   */
  @Test
  public void testAggregationTask() throws Exception {
    int port = 10000;
    int numNode = 3;
    for (StatsReportType type : StatsReportType.values()) {
      initializeNodeReports(type, numNode, port);

      String healthReportName =
          type == StatsReportType.ACCOUNT_REPORT ? HEALTH_REPORT_NAME_ACCOUNT : HEALTH_REPORT_NAME_PARTITION;
      String statsFieldName =
          type == StatsReportType.ACCOUNT_REPORT ? STATS_FIELD_NAME_ACCOUNT : STATS_FIELD_NAME_PARTITION;
      task = new HelixHealthReportAggregatorTask(mockHelixManager, RELEVANT_PERIOD_IN_MINUTES, healthReportName,
          statsFieldName, type, null, clusterMapConfig, mockTime);
      task.run();

      // Verify the targeted znode has value, don't worry about the correctness of the value, it's verified by other tests.
      Stat stat = new Stat();
      ZNRecord record = mockHelixManager.getHelixPropertyStore()
          .get(String.format("/%s%s", HelixHealthReportAggregatorTask.AGGREGATED_REPORT_PREFIX, healthReportName), stat,
              AccessOption.PERSISTENT);
      Assert.assertNotNull(record);
      Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.VALID_SIZE_FIELD_NAME));
      Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.RAW_VALID_SIZE_FIELD_NAME));

      if (type == StatsReportType.ACCOUNT_REPORT && this.enableAggregatedMonthlyAccountReport) {
        record = mockHelixManager.getHelixPropertyStore()
            .get(String.format("/%s%s%s", HelixHealthReportAggregatorTask.AGGREGATED_REPORT_PREFIX, healthReportName,
                HelixHealthReportAggregatorTask.AGGREGATED_MONTHLY_REPORT_SUFFIX), stat, AccessOption.PERSISTENT);
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.VALID_SIZE_FIELD_NAME));
        Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.RAW_VALID_SIZE_FIELD_NAME));
        Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.MONTH_NAME));
      }
    }
  }

  /**
   * Test aggregation task on different month to verify if the new monthly account reports are generated.
   * @throws Exception
   */
  @Test
  public void testAggregationOnNewMonth() throws Exception {
    Assume.assumeTrue(enableAggregatedMonthlyAccountReport);
    initializeNodeReports(StatsReportType.ACCOUNT_REPORT, 3, 1000);

    task = new HelixHealthReportAggregatorTask(mockHelixManager, RELEVANT_PERIOD_IN_MINUTES, HEALTH_REPORT_NAME_ACCOUNT,
        STATS_FIELD_NAME_ACCOUNT, StatsReportType.ACCOUNT_REPORT, null, clusterMapConfig, mockTime);
    task.run();

    Stat stat = new Stat();
    ZNRecord record = mockHelixManager.getHelixPropertyStore()
        .get(String.format("/%s%s%s", HelixHealthReportAggregatorTask.AGGREGATED_REPORT_PREFIX,
            HEALTH_REPORT_NAME_ACCOUNT, HelixHealthReportAggregatorTask.AGGREGATED_MONTHLY_REPORT_SUFFIX), stat,
            AccessOption.PERSISTENT);
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.VALID_SIZE_FIELD_NAME));
    Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.RAW_VALID_SIZE_FIELD_NAME));
    Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.MONTH_NAME));

    String previousMonth = record.getSimpleField(HelixHealthReportAggregatorTask.MONTH_NAME);

    // Moving timestamp to next month, we should get a different ZNRecord with different month value.
    ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
    LocalDateTime dateTime = LocalDateTime.ofEpochSecond(mockTime.seconds(), 0, zoneOffset);
    long secondsNextMonth = dateTime.plusMonths(1).atOffset(zoneOffset).toEpochSecond();
    mockTime.sleep(secondsNextMonth * Time.MsPerSec - mockTime.milliseconds());
    // Now it's should be a month later

    task.run();

    record = mockHelixManager.getHelixPropertyStore()
        .get(String.format("/%s%s%s", HelixHealthReportAggregatorTask.AGGREGATED_REPORT_PREFIX,
            HEALTH_REPORT_NAME_ACCOUNT, HelixHealthReportAggregatorTask.AGGREGATED_MONTHLY_REPORT_SUFFIX), stat,
            AccessOption.PERSISTENT);
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getSimpleField(HelixHealthReportAggregatorTask.MONTH_NAME));
    String currentMonth = record.getSimpleField(HelixHealthReportAggregatorTask.MONTH_NAME);
    Assert.assertFalse(previousMonth.equals(currentMonth));
  }

  /**
   * Initialize the reports and create instances in helix if not exists.
   * @param type The type of reports to create
   * @param numNode The number of nodes to initiate.
   * @param startingPort The starting port number, which will then be incremented to represent different nodes.
   * @throws IOException
   */
  private void initializeNodeReports(StatsReportType type, int numNode, int startingPort) throws IOException {
    String healthReportName =
        type == StatsReportType.ACCOUNT_REPORT ? HEALTH_REPORT_NAME_ACCOUNT : HEALTH_REPORT_NAME_PARTITION;
    String statsFieldName =
        type == StatsReportType.ACCOUNT_REPORT ? STATS_FIELD_NAME_ACCOUNT : STATS_FIELD_NAME_PARTITION;
    List<StatsSnapshot> storeSnapshots = new ArrayList<>();
    Random random = new Random();
    for (int i = 3; i < 6; i++) {
      storeSnapshots.add(TestUtils.generateStoreStats(i, 3, random, type));
    }
    StatsWrapper nodeStats = TestUtils.generateNodeStats(storeSnapshots, 1000, type);
    String nodeStatsJSON = mapper.writeValueAsString(nodeStats);

    HelixDataAccessor dataAccessor = mockHelixManager.getHelixDataAccessor();
    for (int i = 0; i < numNode; i++) {
      String instanceName = ClusterMapUtils.getInstanceName("localhost", startingPort);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort(Integer.toString(startingPort));
      mockHelixAdmin.addInstance(CLUSTER_NAME, instanceConfig);

      PropertyKey key = dataAccessor.keyBuilder().healthReport(instanceName, healthReportName);
      ZNRecord znRecord = new ZNRecord(instanceName);
      // Set the same reports for all instances
      znRecord.setSimpleField(statsFieldName, nodeStatsJSON);
      HelixProperty helixProperty = new HelixProperty(znRecord);
      dataAccessor.setProperty(key, helixProperty);

      startingPort++;
    }
  }
}
