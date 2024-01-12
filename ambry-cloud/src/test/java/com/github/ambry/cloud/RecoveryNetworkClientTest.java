package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.*;


public class RecoveryNetworkClientTest {

  private static Logger logger = LoggerFactory.getLogger(RecoveryNetworkClientTest.class);
  private AzuriteUtils azuriteUtils;
  private MockClusterMap mockClusterMap;
  private RecoveryNetworkClient recoveryNetworkClient;
  private List<MockPartitionId> mockPartitionIds;
  private final short ACCOUNT_ID = 1024, CONTAINER_ID = 2048;
  private final int NUM_BLOBS = 100;
  private final long BLOB_SIZE = 1000;
  private Container testContainer;

  public RecoveryNetworkClientTest() throws IOException {
    azuriteUtils = new AzuriteUtils();
    mockPartitionIds = new ArrayList<>();
    // Azure container names have to be 3 char long at least
    LongStream.rangeClosed(123450L, 123452L).forEach(i ->
        mockPartitionIds.add(new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS)));
    mockClusterMap = new MockClusterMap(false, true, 1,
        1, 1, true, false,
        "localhost");
    testContainer =
        new Container(CONTAINER_ID, "testContainer", Container.ContainerStatus.ACTIVE,
            "Test Container", false,
            false, false,
            false, null, false,
            false, Collections.emptySet(),
            false, false, Container.NamedBlobMode.DISABLED, ACCOUNT_ID, 0,
            0, 0, "",
            null, Collections.emptySet());
    recoveryNetworkClient =
        new RecoveryNetworkClient(new VerifiableProperties(azuriteUtils.getAzuriteConnectionProperties()),
            new MetricRegistry(),    mockClusterMap, null, null);
  }

  public static void createRequestToPutBlob(MockMessageWriteSet messageWriteSet,
      short accountId, short containerId, PartitionId partitionId,
      boolean isEncrypted, boolean isDeleted, boolean isTtlUpdated, boolean isUndeleted,
      long operationTime, long expiryMs,
      long size, short lifeVersion) {
    MessageInfo info = new MessageInfo(CloudTestUtil.getUniqueId(accountId, containerId, isEncrypted, partitionId),
            size, isDeleted, isTtlUpdated, isUndeleted, expiryMs, new Random().nextLong(),
            accountId, containerId, operationTime, lifeVersion);
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
    messageWriteSet.add(info, buffer);
  }

  @Before
  public void before() throws ReflectiveOperationException, CloudStorageException, IOException {
    // Clear Azure partitions and add some blobs
    AccountService accountService = Mockito.mock(AccountService.class);
    Mockito.lenient().when(accountService.getContainersByStatus(any())).thenReturn(Collections.singleton(testContainer));
    Properties properties = azuriteUtils.getAzuriteConnectionProperties();
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(false));
    // Azurite client
    AzureCloudDestinationSync azuriteClient = azuriteUtils.getAzuriteClient(
        properties, new MetricRegistry(),    null, accountService);
    // For each partition, add NUM_BLOBS of size BLOB_SIZE
    List<MessageInfo> messageInfoList = new ArrayList<>();
    for (MockPartitionId mockPartitionId : mockPartitionIds) {
      azuriteClient.compactPartition(mockPartitionId.toPathString());
      IntStream.range(0, NUM_BLOBS).forEach(i -> messageInfoList.add(
          new MessageInfo(CloudTestUtil.getUniqueId(testContainer.getParentAccountId(), testContainer.getId(), false, mockPartitionId),
          BLOB_SIZE, false, false, false, Utils.Infinite_Time, new Random().nextLong(),
              testContainer.getParentAccountId(), testContainer.getId(), System.currentTimeMillis(), (short) 0)));
    }
    // Upload blobs
    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(
        TestUtils.getRandomBytes((int) (NUM_BLOBS * BLOB_SIZE * mockPartitionIds.size()))));
    MessageFormatWriteSet messageWriteSet = new MessageFormatWriteSet(
        new MessageSievingInputStream(inputStream, messageInfoList, Collections.emptyList(), new MetricRegistry()),
        messageInfoList, false);
    azuriteClient.uploadBlobs(messageWriteSet);
  }

  @After
  public void after() {

  }

  @Test
  public void basic() {

  }
}
