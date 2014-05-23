package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.HardwareLayout;
import com.github.ambry.clustermap.PartitionLayout;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 *
 */
public class CoordinatorTest {
  ClusterMap getClusterMapOneDCOneNodeOneDiskOnePartition() throws JSONException {
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

    HardwareLayout hl = new HardwareLayout(new JSONObject(HL));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapOneDCThreeNodeOneDiskOnePartition() throws JSONException {
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

    HardwareLayout hl = new HardwareLayout(new JSONObject(HL));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapOneDCFourNodeOneDiskTwoPartition() throws JSONException {
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

    HardwareLayout hl = new HardwareLayout(new JSONObject(HL));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  ClusterMap getClusterMapTwoDCFourNodeOneDiskFourPartition() throws JSONException {
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

    HardwareLayout hl = new HardwareLayout(new JSONObject(HL));
    PartitionLayout pl = new PartitionLayout(hl, new JSONObject(PL));
    return new ClusterMapManager(pl);
  }

  VerifiableProperties getVProps() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "Datacenter");
    properties.setProperty("coordinator.connection.pool.factory",
                           "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  VerifiableProperties getVPropsTwo() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DatacenterTwo");
    properties.setProperty("coordinator.connection.pool.factory",
                           "com.github.ambry.coordinator.MockConnectionPoolFactory");
    return new VerifiableProperties(properties);
  }

  void PutGetDelete(AmbryCoordinator ac) throws InterruptedException, StoreException, IOException,
          CoordinatorException {
    BlobProperties putBlobProperties = new BlobProperties(100,
                                                          "serviceId",
                                                          "memberId",
                                                          "contentType",
                                                          false,
                                                          Utils.Infinite_Time);
    ByteBuffer putUserMetadata = ByteBuffer.allocate(10);
    for (byte b = 0; b < 10; b++) {
      putUserMetadata.put(b);
    }

    ByteBuffer putContent = ByteBuffer.allocate(100);
    for (byte b = 0; b < 100; b++) {
      putContent.put(b);
    }
    putContent.flip();
    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = ac.putBlob(putBlobProperties, putUserMetadata, blobData);
    System.out.println("BlobId: " + blobId);

    BlobProperties getBlobProperties = ac.getBlobProperties(blobId);
    assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = ac.getBlobUserMetadata(blobId);
    assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = ac.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int)getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    assertArrayEquals(blobDataBytes, putContent.array());

    ac.deleteBlob(blobId);
  }

  void simple(ClusterMap clusterMap) throws JSONException, InterruptedException, StoreException, IOException,
          CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator ac = new AmbryCoordinator(getVProps(), clusterMap);
    ac.start();
    for (int i = 0; i < 20; ++i) {
      PutGetDelete(ac);
    }
    ac.shutdown();
  }

  @Test
  public void simpleOneDCOneNodeOneDiskOnePartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    simple(getClusterMapOneDCOneNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCThreeNodeOneDiskOnePartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    simple(getClusterMapOneDCThreeNodeOneDiskOnePartition());
  }

  @Test
  public void simpleOneDCFourNodeOneDiskTwoPartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    simple(getClusterMapOneDCFourNodeOneDiskTwoPartition());
  }

  @Test
  public void simpleTwoDCFourNodeOneDiskFourPartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    simple(getClusterMapTwoDCFourNodeOneDiskFourPartition());
  }


  void multiAC(ClusterMap clusterMap) throws JSONException, InterruptedException, StoreException, IOException,
          CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator acOne = new AmbryCoordinator(getVProps(), clusterMap);
    AmbryCoordinator acTwo = new AmbryCoordinator(getVPropsTwo(), clusterMap);

    acOne.start();
    acTwo.start();

    for (int i = 0; i < 20; ++i) {
      PutGetDelete(acOne);
      PutGetDelete(acTwo);
    }

    acOne.shutdown();
    acTwo.shutdown();
  }

  @Test
  public void multiACTwoDCFourNodeOneDiskFourPartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    multiAC(getClusterMapTwoDCFourNodeOneDiskFourPartition());
  }

  void PutRemoteGetDelete(AmbryCoordinator acOne,
                          AmbryCoordinator acTwo) throws InterruptedException, StoreException, IOException,
          CoordinatorException {
    BlobProperties putBlobProperties = new BlobProperties(100,
                                                          "serviceId",
                                                          "memberId",
                                                          "contentType",
                                                          false,
                                                          Utils.Infinite_Time);
    ByteBuffer putUserMetadata = ByteBuffer.allocate(10);
    for (byte b = 0; b < 10; b++) {
      putUserMetadata.put(b);
    }

    ByteBuffer putContent = ByteBuffer.allocate(100);
    for (byte b = 0; b < 100; b++) {
      putContent.put(b);
    }
    putContent.flip();
    InputStream blobData = new ByteBufferInputStream(putContent);

    String blobId = acOne.putBlob(putBlobProperties, putUserMetadata, blobData);
    System.out.println("BlobId: " + blobId);

    BlobProperties getBlobProperties = acTwo.getBlobProperties(blobId);
    assertEquals(putBlobProperties.getBlobSize(), getBlobProperties.getBlobSize());
    assertEquals(putBlobProperties.getContentType(), getBlobProperties.getContentType());

    ByteBuffer getUserMetadata = acTwo.getBlobUserMetadata(blobId);
    assertArrayEquals(putUserMetadata.array(), getUserMetadata.array());

    BlobOutput getBlobOutput = acTwo.getBlob(blobId);
    byte[] blobDataBytes = new byte[(int)getBlobOutput.getSize()];
    new DataInputStream(getBlobOutput.getStream()).readFully(blobDataBytes);
    assertArrayEquals(blobDataBytes, putContent.array());

    acTwo.deleteBlob(blobId);
  }

  void remoteAC(ClusterMap clusterMap) throws JSONException, InterruptedException, StoreException, IOException,
          CoordinatorException {
    MockConnectionPool.mockCluster = new MockCluster(clusterMap);
    AmbryCoordinator acOne = new AmbryCoordinator(getVProps(), clusterMap);
    AmbryCoordinator acTwo = new AmbryCoordinator(getVPropsTwo(), clusterMap);

    acOne.start();
    acTwo.start();

    for (int i = 0; i < 20; ++i) {
      PutRemoteGetDelete(acOne, acTwo);
    }

    acOne.shutdown();
    acTwo.shutdown();
  }

  @Test
  public void remoteACTwoDCFourNodeOneDiskFourPartition() throws JSONException, InterruptedException, StoreException,
          IOException, CoordinatorException {
    remoteAC(getClusterMapTwoDCFourNodeOneDiskFourPartition());
  }
}
