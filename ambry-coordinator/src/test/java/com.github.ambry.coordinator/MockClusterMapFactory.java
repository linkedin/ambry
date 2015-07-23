package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.HardwareLayout;
import com.github.ambry.clustermap.PartitionLayout;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.json.JSONException;
import org.json.JSONObject;


public class MockClusterMapFactory {

  public static ClusterMap getClusterMapOneDCOneNodeOneDiskOnePartition()
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
        "              \"port\": 6667,\n" +
        "              \"sslport\": 7667 \n" +
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

  public static ClusterMap getClusterMapOneDCThreeNodeOneDiskOnePartition()
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
        "              \"port\": 6667,\n" +
        "              \"sslport\": 7667 \n" +
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
        "              \"port\": 6668,\n" +
        "              \"sslport\": 7668 \n" +
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
        "              \"port\": 6669,\n" +
        "              \"sslport\": 7669 \n" +
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

  public static ClusterMap getClusterMapOneDCFourNodeOneDiskTwoPartition()
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
        "              \"port\": 6667,\n" +
        "              \"sslport\": 7667 \n" +
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
        "              \"port\": 6668,\n" +
        "              \"sslport\": 7668 \n" +
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
        "              \"port\": 6669,\n" +
        "              \"sslport\": 7669 \n" +
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
        "              \"port\": 6670,\n" +
        "              \"sslport\": 7670 \n" +
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

  public static ClusterMap getClusterMapTwoDCFourNodeOneDiskFourPartition()
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
        "              \"port\": 6667,\n" +
        "              \"sslport\": 7667 \n" +
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
        "              \"port\": 6668,\n" +
        "              \"sslport\": 7668 \n" +
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
        "              \"port\": 6669,\n" +
        "              \"sslport\": 7669 \n" +
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
        "              \"port\": 6670,\n" +
        "              \"sslport\": 7670 \n" +
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
        "              \"port\": 6680,\n" +
        "              \"sslport\": 7680 \n" +
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
        "              \"port\": 6681,\n" +
        "              \"sslport\": 7681 \n" +
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
        "              \"port\": 6682,\n" +
        "              \"sslport\": 7682 \n" +
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
        "              \"port\": 6683,\n" +
        "              \"sslport\": 7683 \n" +
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
}
