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
package com.github.ambry.coordinator;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.HardwareLayout;
import com.github.ambry.clustermap.PartitionLayout;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;


/**
 *
 */
public class CoordinatorTest {

  // Below three configs are for testing error cases assuming getClusterMapOneDCThreeNodeOneDiskOnePartition
  // as cluster config
  private ArrayList<Integer> exceptionHostPorts = new ArrayList(Arrays.asList(6667, 6668, 6669));
  private final int TOTAL_HOST_COUNT = 3;
  private final String host = "localhost";

  private static HashMap<ServerErrorCode, CoordinatorError> deleteErrorMappings =
      new HashMap<ServerErrorCode, CoordinatorError>();
  private static HashMap<ServerErrorCode, CoordinatorError> getErrorMappings =
      new HashMap<ServerErrorCode, CoordinatorError>();
  private static HashMap<ServerErrorCode, CoordinatorError> putErrorMappings =
      new HashMap<ServerErrorCode, CoordinatorError>();

  private Random random = new Random();

  private void induceGetFailure(int count, ServerErrorCode errorCode) {
    List<Integer> hostPorts = (ArrayList<Integer>) exceptionHostPorts.clone();
    for (int i = 0; i < count; i++) {
      int nextRandom = random.nextInt(hostPorts.size());
      MockDataNode mockDataNode = MockConnectionPool.mockCluster.getMockDataNode(host, hostPorts.get(nextRandom));
      mockDataNode.setGetException(errorCode);
      hostPorts.remove(nextRandom);
    }
  }

  private void induceDeleteFailure(int count, ServerErrorCode errorCode) {
    List<Integer> hostPorts = (ArrayList<Integer>) exceptionHostPorts.clone();
    for (int i = 0; i < count; i++) {
      int nextRandom = random.nextInt(hostPorts.size());
      MockDataNode mockDataNode = MockConnectionPool.mockCluster.getMockDataNode(host, hostPorts.get(nextRandom));
      mockDataNode.setDeleteException(errorCode);
      hostPorts.remove(nextRandom);
    }
  }

  private void inducePutFailure(int count, ServerErrorCode errorCode) {
    List<Integer> hostPorts = (ArrayList<Integer>) exceptionHostPorts.clone();
    for (int i = 0; i < count; i++) {
      int nextRandom = random.nextInt(hostPorts.size());
      MockDataNode mockDataNode = MockConnectionPool.mockCluster.getMockDataNode(host, hostPorts.get(nextRandom));
      mockDataNode.setPutException(errorCode);
      hostPorts.remove(nextRandom);
    }
  }

  static {
    deleteErrorMappings.put(ServerErrorCode.Blob_Not_Found, CoordinatorError.BlobDoesNotExist);
    deleteErrorMappings.put(ServerErrorCode.Blob_Expired, CoordinatorError.BlobExpired);
    deleteErrorMappings.put(ServerErrorCode.Disk_Unavailable, CoordinatorError.AmbryUnavailable);
    deleteErrorMappings.put(ServerErrorCode.IO_Error, CoordinatorError.UnexpectedInternalError);
    deleteErrorMappings.put(ServerErrorCode.Partition_ReadOnly, CoordinatorError.UnexpectedInternalError);

    getErrorMappings.put(ServerErrorCode.IO_Error, CoordinatorError.UnexpectedInternalError);
    getErrorMappings.put(ServerErrorCode.Data_Corrupt, CoordinatorError.UnexpectedInternalError);
    getErrorMappings.put(ServerErrorCode.Blob_Not_Found, CoordinatorError.BlobDoesNotExist);
    getErrorMappings.put(ServerErrorCode.Blob_Deleted, CoordinatorError.BlobDeleted);
    getErrorMappings.put(ServerErrorCode.Blob_Expired, CoordinatorError.BlobExpired);
    getErrorMappings.put(ServerErrorCode.Disk_Unavailable, CoordinatorError.AmbryUnavailable);
    getErrorMappings.put(ServerErrorCode.Partition_Unknown, CoordinatorError.BlobDoesNotExist);
    //unknown
    getErrorMappings.put(ServerErrorCode.Partition_ReadOnly, CoordinatorError.UnexpectedInternalError);

    putErrorMappings.put(ServerErrorCode.IO_Error, CoordinatorError.UnexpectedInternalError);
    putErrorMappings.put(ServerErrorCode.Partition_ReadOnly, CoordinatorError.UnexpectedInternalError);
    putErrorMappings.put(ServerErrorCode.Partition_Unknown, CoordinatorError.UnexpectedInternalError);
    putErrorMappings.put(ServerErrorCode.Disk_Unavailable, CoordinatorError.AmbryUnavailable);
    putErrorMappings.put(ServerErrorCode.Blob_Already_Exists, CoordinatorError.UnexpectedInternalError);
    //unknown
    putErrorMappings.put(ServerErrorCode.Data_Corrupt, CoordinatorError.UnexpectedInternalError);
  }

  ClusterMap getClusterMapOneDCOneNodeOneDiskOnePartition()
      throws JSONException {
    String HL = "  {\n" +
        "    \"clusterName\": \"OneDCOneNodeOneDiskOnePartition\",\n" +
        "    \"version\": 2,\n" +
        "          \"datacenters\": [\n" +
        "    {\n" +
        "      \"dataNodes\": [\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6667\n" +
        "      }\n" +
        "      ],\n" +
        "      \"name\": \"Datacenter\"\n" +
        "    }\n" +
        "    ]\n" +
        "  }\n";

    String PL = "{\n" +
        "    \"clusterName\": \"OneDCOneNodeOneDiskOnePartition\",\n" +
        "    \"version\": 3,\n" +
        "          \"partitions\": [\n" +
        "    {\n" +
        "      \"id\": 0,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      }\n" +
        "      ]\n" +
        "    }\n" +
        "    ]\n" +
        "  }  \n";

    HardwareLayout hl =
        new HardwareLayout(new JSONObject(HL), new ClusterMapConfig(new VerifiableProperties(new Properties())));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapOneDCThreeNodeOneDiskOnePartition()
      throws JSONException {
    String HL = "  {\n" +
        "    \"clusterName\": \"OneDCThreeNodeOneDiskOnePartition\",\n" +
        "    \"version\": 4,\n" +
        "          \"datacenters\": [\n" +
        "    {\n" +
        "      \"dataNodes\": [\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6669\n" +
        "      }\n" +
        "      ],\n" +
        "      \"name\": \"Datacenter\"\n" +
        "    }\n" +
        "    ]\n" +
        "  }\n";

    String PL = "{\n" +
        "    \"clusterName\": \"OneDCThreeNodeOneDiskOnePartition\",\n" +
        "    \"version\": 5,\n" +
        "          \"partitions\": [\n" +
        "    {\n" +
        "      \"id\": 0,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      }\n" +
        "      ]\n" +
        "    }\n" +
        "    ]\n" +
        "  }  \n";

    HardwareLayout hl =
        new HardwareLayout(new JSONObject(HL), new ClusterMapConfig(new VerifiableProperties(new Properties())));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapOneDCFourNodeOneDiskTwoPartition()
      throws JSONException {
    String HL = "  {\n" +
        "    \"clusterName\": \"OneDCFourNodeOneDiskTwoPartition\",\n" +
        "    \"version\": 6,\n" +
        "          \"datacenters\": [\n" +
        "    {\n" +
        "      \"dataNodes\": [\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6669\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6670\n" +
        "      }\n" +
        "      ],\n" +
        "      \"name\": \"Datacenter\"\n" +
        "    }\n" +
        "    ]\n" +
        "  }\n";

    String PL = "{\n" +
        "    \"clusterName\": \"OneDCFourNodeOneDiskTwoPartition\",\n" +
        "    \"version\": 7,\n" +
        "          \"partitions\": [\n" +
        "    {\n" +
        "      \"id\": 0,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      }\n" +
        "      ]\n" +
        "    },\n" +
        "    {\n" +
        "      \"id\": 1,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6670\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      }\n" +
        "      ]\n" +
        "    }\n" +
        "    ]\n" +
        "  }  \n";

    HardwareLayout hl =
        new HardwareLayout(new JSONObject(HL), new ClusterMapConfig(new VerifiableProperties(new Properties())));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapTwoDCFourNodeOneDiskFourPartition()
      throws JSONException {
    String HL = "  {\n" +
        "    \"clusterName\": \"TwoDCFourNodeOneDiskFourPartition\",\n" +
        "    \"version\": 8,\n" +
        "          \"datacenters\": [\n" +
        "    {\n" +
        "      \"dataNodes\": [\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6669\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6670\n" +
        "      }\n" +
        "      ],\n" +
        "      \"name\": \"Datacenter\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"dataNodes\": [\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6680\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6681\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6682\n" +
        "      },\n" +
        "      {\n" +
        "        \"disks\": [\n" +
        "        {\n" +
        "          \"capacityInBytes\": 21474836480,\n" +
        "                \"hardwareState\": \"AVAILABLE\",\n" +
        "                \"mountPath\": \"/mnt0\"\n" +
        "        }\n" +
        "        ],\n" +
        "        \"hardwareState\": \"AVAILABLE\",\n" +
        "              \"hostname\": \"localhost\",\n" +
        "              \"port\": 6683\n" +
        "      }\n" +
        "      ],\n" +
        "      \"name\": \"DatacenterTwo\"\n" +
        "    }\n" +
        "    ]\n" +
        "  }\n";

    String PL = "{\n" +
        "    \"clusterName\": \"TwoDCFourNodeOneDiskFourPartition\",\n" +
        "    \"version\": 9,\n" +
        "          \"partitions\": [\n" +
        "    {\n" +
        "      \"id\": 0,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6680\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6682\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6683\n" +
        "      }\n" +
        "      ]\n" +
        "    },\n" +

        "    {\n" +
        "      \"id\": 1,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6670\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6681\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6682\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6683\n" +
        "      }\n" +
        "      ]\n" +
        "    },\n" +

        "    {\n" +
        "      \"id\": 2,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6668\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6670\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6680\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6681\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6683\n" +
        "      }\n" +
        "      ]\n" +
        "    },\n" +

        "    {\n" +
        "      \"id\": 3,\n" +
        "            \"partitionState\": \"READ_WRITE\",\n" +
        "            \"replicaCapacityInBytes\": 10737418240,\n" +
        "            \"replicas\": [\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6667\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6669\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6670\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6680\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6681\n" +
        "      },\n" +
        "      {\n" +
        "        \"hostname\": \"localhost\",\n" +
        "              \"mountPath\": \"/mnt0\",\n" +
        "              \"port\": 6682\n" +
        "      }\n" +
        "      ]\n" +
        "    }\n" +

        "    ]\n" +
        "  }  \n";

    HardwareLayout hl =
        new HardwareLayout(new JSONObject(HL), new ClusterMapConfig(new VerifiableProperties(new Properties())));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  VerifiableProperties getVProps() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "Datacenter");
    properties
        .setProperty("coordinator.connection.pool.factory", "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  VerifiableProperties getVPropsTwo() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DatacenterTwo");
    properties
        .setProperty("coordinator.connection.pool.factory", "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  void PutGetDelete(AmbryCoordinator ac)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    ByteBuffer putUserMetadata = getByteBuffer(10, false);
    ByteBuffer putContent = getByteBuffer(100, true);
    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = ac.putBlob(putBlobProperties, putUserMetadata, blobData);

    BlobProperties getBlobProperties = ac.getBlobProperties(blobId);
    Assert.assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    Assert.assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = ac.getBlobUserMetadata(blobId);
    Assert.assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = ac.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int) getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    Assert.assertArrayEquals(blobDataBytes, putContent.array());

    ac.deleteBlob(blobId);

    try {
      getBlobOutput = ac.getBlob(blobId);
      Assert.fail("GetBlob for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      getBlobProperties = ac.getBlobProperties(blobId);
      Assert.fail("GetBlobProperties for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      getUserMetadata = ac.getBlobUserMetadata(blobId);
      Assert.fail("GetUserMetaData for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      ac.deleteBlob(blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.fail("Deletion of a deleted blob should not have thrown CoordinatorException " + blobId);
    }
  }

  private String putBlobVerifyGet(AmbryCoordinator ac)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    ByteBuffer putUserMetadata = getByteBuffer(10, false);
    ByteBuffer putContent = getByteBuffer(100, true);
    return putBlobVerifyGet(ac, putBlobProperties, putUserMetadata, putContent);
  }

  private String putBlobVerifyGet(AmbryCoordinator ac, BlobProperties putBlobProperties, ByteBuffer putUserMetadata,
      ByteBuffer putContent)
      throws InterruptedException, StoreException, IOException, CoordinatorException {

    InputStream blobData = new ByteBufferInputStream(putContent);
    String blobId = ac.putBlob(putBlobProperties, putUserMetadata, blobData);

    BlobProperties getBlobProperties = ac.getBlobProperties(blobId);
    Assert.assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    Assert.assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = ac.getBlobUserMetadata(blobId);
    Assert.assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = ac.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int) getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    Assert.assertArrayEquals(blobDataBytes, putContent.array());
    return blobId;
  }

  private String putBlob(AmbryCoordinator ac)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    ByteBuffer putUserMetadata = getByteBuffer(10, false);
    ByteBuffer putContent = getByteBuffer(100, true);
    return putBlob(ac, putBlobProperties, putUserMetadata, putContent);
  }

  private String putBlob(AmbryCoordinator ac, BlobProperties putBlobProperties, ByteBuffer putUserMetadata,
      ByteBuffer putContent)
      throws InterruptedException, StoreException, IOException, CoordinatorException {

    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = ac.putBlob(putBlobProperties, putUserMetadata, blobData);
    return blobId;
  }

  void getBlob(AmbryCoordinator ac, String blobId, BlobProperties putBlobProperties, ByteBuffer putUserMetadata,
      ByteBuffer putContent)
      throws InterruptedException, StoreException, IOException, CoordinatorException {

    BlobProperties getBlobProperties = ac.getBlobProperties(blobId);
    Assert.assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    Assert.assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = ac.getBlobUserMetadata(blobId);
    Assert.assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = ac.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int) getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    Assert.assertArrayEquals(blobDataBytes, putContent.array());
  }

  void deleteBlobAndGet(AmbryCoordinator ac, String blobId)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    ac.deleteBlob(blobId);
    try {
      BlobOutput getBlobOutput = ac.getBlob(blobId);
      Assert.fail("GetBlob for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      BlobProperties getBlobProperties = ac.getBlobProperties(blobId);
      Assert.fail("GetBlobProperties for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      ByteBuffer getUserMetadata = ac.getBlobUserMetadata(blobId);
      Assert.fail("GetUserMetaData for a deleted blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDeleted);
    }
    try {
      ac.deleteBlob(blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.fail("Deletion of a deleted blob should not have thrown CoordinatorException " + blobId);
    }
  }

  void deleteBlob(AmbryCoordinator ac, String blobId)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    ac.deleteBlob(blobId);
  }

  void GetNonExistantBlob(AmbryCoordinator ac)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    ByteBuffer putUserMetadata = getByteBuffer(10, false);
    ByteBuffer putContent = getByteBuffer(100, true);
    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = ac.putBlob(putBlobProperties, putUserMetadata, blobData);

    // create dummy blobid by corrupting the actual blob id. Goal is to corrupt the UUID part of id rather than break
    // the id so much that an exception is thrown during blob id construction.
    System.err.println("blob Id " + blobId);
    String nonExistantBlobId = blobId.substring(0, blobId.length() - 5) + "AAAAA";
    System.err.println("non existent blob Id " + nonExistantBlobId);

    try {
      BlobOutput getBlobOutput = ac.getBlob(nonExistantBlobId);
      Assert.fail("GetBlob for a non existing blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDoesNotExist);
    }
    try {
      BlobProperties getBlobProperties = ac.getBlobProperties(nonExistantBlobId);
      Assert.fail("GetBlobProperties for a non existing blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDoesNotExist);
    }
    try {
      ByteBuffer getUserMetadata = ac.getBlobUserMetadata(nonExistantBlobId);
      Assert.fail("GetUserMetaData for a non existing blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDoesNotExist);
    }
    try {
      ac.deleteBlob(nonExistantBlobId);
      Assert.fail("DeleteBlob of a non existing blob should have thrown CoordinatorException " + blobId);
    } catch (CoordinatorException coordinatorException) {
      Assert.assertEquals(coordinatorException.getErrorCode(), CoordinatorError.BlobDoesNotExist);
    }
  }

  void simple(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
    for (int i = 0; i < 20; ++i) {
      PutGetDelete(ac);
    }
    ac.close();
  }

  /**
   * Same as above simple method, but instead of performing all inline, this calls very spefific methods
   * to perform something like the put, get and delete. Purpose of this method is to test the sanity of these
   * smaller methods which are going to be used for execption cases after this test case.
   * @param clusterMap
   * @throws JSONException
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   * @throws CoordinatorException
   */
  void simpleDecoupled(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
    for (int i = 0; i < 20; ++i) {
      BlobProperties putBlobProperties =
          new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
      ByteBuffer putUserMetadata = getByteBuffer(10, false);
      ByteBuffer putContent = getByteBuffer(100, true);
      String blobId = putBlob(ac, putBlobProperties, putUserMetadata, putContent);
      getBlob(ac, blobId, putBlobProperties, putUserMetadata, putContent);
      deleteBlob(ac, blobId);
    }
    ac.close();
  }

  void simpleDeleteException(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {

    for (ServerErrorCode deleteErrorCode : deleteErrorMappings.keySet()) {
      if (!deleteErrorCode.equals(ServerErrorCode.Disk_Unavailable) &&
          !deleteErrorCode.equals(ServerErrorCode.Partition_ReadOnly) &&
          !deleteErrorCode.equals(ServerErrorCode.IO_Error)) {
        // Ignoring three ServerErrorCodes as PUTs are expected to fail in such cases
        MockConnectionPool.mockCluster = new MockCluster(clusterMap);
        induceDeleteFailure(TOTAL_HOST_COUNT, deleteErrorCode);
        AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
        for (int i = 0; i < 20; ++i) {
          BlobProperties putBlobProperties =
              new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
          ByteBuffer putUserMetadata = getByteBuffer(10, false);
          ByteBuffer putContent = getByteBuffer(100, true);
          String blobId = putBlob(ac, putBlobProperties, putUserMetadata, putContent);
          getBlob(ac, blobId, putBlobProperties, putUserMetadata, putContent);
          try {
            deleteBlobAndGet(ac, blobId);
            Assert.fail("Deletion should have failed for " + deleteErrorCode);
          } catch (CoordinatorException e) {
            Assert.assertEquals(e.getErrorCode(), deleteErrorMappings.get(deleteErrorCode));
          }
        }
        ac.close();
      }
    }
  }

  void simpleGetException(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {

    for (ServerErrorCode getErrorCode : getErrorMappings.keySet()) {
      MockConnectionPool.mockCluster = new MockCluster(clusterMap);
      induceGetFailure(TOTAL_HOST_COUNT, getErrorCode);
      AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
      for (int i = 0; i < 20; ++i) {
        BlobProperties putBlobProperties =
            new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
        ByteBuffer putUserMetadata = getByteBuffer(10, false);
        ByteBuffer putContent = getByteBuffer(100, true);
        String blobId = putBlob(ac, putBlobProperties, putUserMetadata, putContent);
        try {
          getBlob(ac, blobId, putBlobProperties, putUserMetadata, putContent);
          Assert.fail("Get should have failed for " + getErrorCode);
        } catch (CoordinatorException e) {
          Assert.assertEquals(e.getErrorCode(), getErrorMappings.get(getErrorCode));
        }
        deleteBlob(ac, blobId);
      }
      ac.close();
    }
  }

  void simplePutException(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {

    for (ServerErrorCode putErrorCode : putErrorMappings.keySet()) {
      System.out.println("puError code " + putErrorCode);
      if (!putErrorCode.equals(ServerErrorCode.Disk_Unavailable) &&
          !putErrorCode.equals(ServerErrorCode.Partition_ReadOnly) &&
          !putErrorCode.equals(ServerErrorCode.IO_Error)) {
        // Ignoring three ServerErrorCodes as PUTs are expected to fail in such cases
        MockConnectionPool.mockCluster = new MockCluster(clusterMap);
        inducePutFailure(TOTAL_HOST_COUNT, putErrorCode);
        AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
        for (int i = 0; i < 20; ++i) {
          BlobProperties putBlobProperties =
              new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
          ByteBuffer putUserMetadata = getByteBuffer(10, false);
          ByteBuffer putContent = getByteBuffer(100, true);
          String blobId = null;
          try {
            blobId = putBlob(ac, putBlobProperties, putUserMetadata, putContent);
            Assert.fail("Put should have failed for " + putErrorCode);
          } catch (CoordinatorException e) {
            Assert.assertEquals(e.getErrorCode(), putErrorMappings.get(putErrorCode));
          }
        }
        ac.close();
      }
    }
  }

  void simpleGetNonExistantBlob(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
    GetNonExistantBlob(ac);
    ac.close();
  }

  @Test
  public void simpleOneDCOneNodeOneDiskOnePartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simple(getClusterMapOneDCOneNodeOneDiskOnePartition());
    simpleGetNonExistantBlob(getClusterMapOneDCOneNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCThreeNodeOneDiskOnePartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simple(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCThreeNodeOneDiskOnePartitionForDeleteException()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simpleDeleteException(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCThreeNodeOneDiskOnePartitionForGetException()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simpleGetException(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCThreeNodeOneDiskOnePartitionForPutException()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simplePutException(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleDecoupleOneDCThreeNodeOneDiskOnePartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simpleDecoupled(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCFourNodeOneDiskTwoPartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simple(getClusterMapOneDCFourNodeOneDiskTwoPartition());
  }

  @Test
  public void simpleTwoDCFourNodeOneDiskFourPartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    simple(getClusterMapTwoDCFourNodeOneDiskFourPartition());
  }

  @Test
  public void multiACTwoDCFourNodeOneDiskFourPartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    ClusterMap clusterMap = getClusterMapTwoDCFourNodeOneDiskFourPartition();
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator acOne = new AmbryCoordinator(getVProps(), clusterMap);
    for (int i = 0; i < 20; ++i) {
      PutGetDelete(acOne);
    }
    acOne.close();
  }

  void PutRemoteGetDelete(AmbryCoordinator acOne, AmbryCoordinator acTwo)
      throws InterruptedException, StoreException, IOException, CoordinatorException {
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    ByteBuffer putUserMetadata = getByteBuffer(10, false);
    ByteBuffer putContent = getByteBuffer(100, true);
    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = acOne.putBlob(putBlobProperties, putUserMetadata, blobData);

    BlobProperties getBlobProperties = acTwo.getBlobProperties(blobId);
    Assert.assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    Assert.assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = acTwo.getBlobUserMetadata(blobId);
    Assert.assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = acTwo.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int) getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    Assert.assertArrayEquals(blobDataBytes, putContent.array());

    acTwo.deleteBlob(blobId);
  }

  void remoteAC(ClusterMap clusterMap)
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator acOne = new AmbryCoordinator(getVProps(), clusterMap);
    AmbryCoordinator acTwo = new AmbryCoordinator(getVPropsTwo(), clusterMap);
    for (int i = 0; i < 20; ++i) {
      PutRemoteGetDelete(acOne, acTwo);
    }

    acOne.close();
    acTwo.close();
  }

  @Test
  public void remoteACTwoDCFourNodeOneDiskFourPartition()
      throws JSONException, InterruptedException, StoreException, IOException, CoordinatorException {
    remoteAC(getClusterMapTwoDCFourNodeOneDiskFourPartition());
  }

  private ByteBuffer getByteBuffer(int count, boolean toFlip) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(count);
    for (byte b = 0; b < count; b++) {
      byteBuffer.put(b);
    }
    if (toFlip) {
      byteBuffer.flip();
    }
    return byteBuffer;
  }
}
