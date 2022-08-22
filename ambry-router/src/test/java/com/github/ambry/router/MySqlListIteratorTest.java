//package com.github.ambry.router;
//
//import com.github.ambry.account.InMemAccountService;
//import com.github.ambry.clustermap.ClusterMap;
//import com.github.ambry.clustermap.MockClusterMap;
//import com.github.ambry.commons.BlobId;
//import com.github.ambry.commons.BlobIdFactory;
//import com.github.ambry.commons.LoggingNotificationSystem;
//import com.github.ambry.commons.ResponseHandler;
//import com.github.ambry.config.CryptoServiceConfig;
//import com.github.ambry.config.KMSConfig;
//import com.github.ambry.config.QuotaConfig;
//import com.github.ambry.config.RouterConfig;
//import com.github.ambry.config.VerifiableProperties;
//import com.github.ambry.messageformat.BlobProperties;
//import com.github.ambry.messageformat.CompositeBlobInfo;
//import com.github.ambry.named.MySqlPartiallyReadableBlobDb;
//import com.github.ambry.named.PartiallyReadableBlobDb;
//import com.github.ambry.named.PartiallyReadableBlobRecord;
//import com.github.ambry.quota.QuotaTestUtils;
//import com.github.ambry.rest.RestServiceException;
//import com.github.ambry.utils.MockTime;
//import com.github.ambry.utils.NettyByteBufLeakHelper;
//import com.github.ambry.utils.TestUtils;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Random;
//import java.util.concurrent.atomic.AtomicReference;
//import org.junit.Assert;
//import org.junit.Test;
//
//
//public class MySqlListIteratorTest {
//
//  private static final int MAX_PORTS_PLAIN_TEXT = 3;
//  private static final int MAX_PORTS_SSL = 3;
//  private static final int CHECKOUT_TIMEOUT_MS = 1000;
//  private static final String LOCAL_DC = "DC3";
//  private final int replicasCount;
//  private final int maxChunkSize;
//  private final MockTime time = new MockTime();
//  private final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<>();
//  private final Random random = new Random();
//  private final MockClusterMap mockClusterMap;
//  private final BlobIdFactory blobIdFactory;
//  private final NonBlockingRouterMetrics routerMetrics;
//  private final MockServerLayout mockServerLayout;
//  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
//  private final ResponseHandler responseHandler;
//  private final MockNetworkClient mockNetworkClient;
//  private final RouterCallback routerCallback;
//  private final String operationTrackerType;
//  private final boolean testEncryption;
//  private MockKeyManagementService kms = null;
//  private MockCryptoService cryptoService = null;
//  private CryptoJobHandler cryptoJobHandler = null;
//  private String localDcName;
//
//  // Certain tests recreate the routerConfig with different properties.
//  private RouterConfig routerConfig;
//  private int blobSize;
//
//  // Parameters for puts which are also used to verify the gets.
//  private String blobIdStr;
//  private BlobId blobId;
//  private BlobProperties blobProperties;
//  private byte[] userMetadata;
//  private byte[] putContent;
//  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();
//
//  // Options which are passed into GetBlobOperations
//  private GetBlobOptionsInternal options;
//
//  private final RequestRegistrationCallback<GetOperation> requestRegistrationCallback =
//      new RequestRegistrationCallback<>(correlationIdToGetOperation);
//  private final QuotaTestUtils.TestQuotaChargeCallback quotaChargeCallback;
//
//  private GetBlobOperation.MySqlListItr mySqlListIterator;
//  private final List<CompositeBlobInfo.ChunkMetadata> parentList = new ArrayList<>();
//  private final String accountName = "account-test";
//  private final String containerName = "container-test";
//  private final String blobName = "blob-test";
//  private final PartiallyReadableBlobDb partiallyReadableBlobDb = new MySqlPartiallyReadableBlobDb();
//
//  public MySqlListIteratorTest() throws IOException {
//    Properties properties = new Properties();
//    properties.setProperty(QuotaConfig.BANDWIDTH_THROTTLING_FEATURE_ENABLED,
//        String.valueOf(false));
//    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
//    quotaChargeCallback = QuotaTestUtils.createTestQuotaChargeCallback(quotaConfig);
//    this.operationTrackerType = "SimpleOperationTracker";
//    this.testEncryption = false;
//    // Defaults. Tests may override these and do new puts as appropriate.
//    maxChunkSize = random.nextInt(1024 * 1024) + 1;
//    // a blob size that is greater than the maxChunkSize and is not a multiple of it. Will result in a composite blob.
//    blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
//    mockSelectorState.set(MockSelectorState.Good);
//    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(false));
//    routerConfig = new RouterConfig(vprops);
//    mockClusterMap = new MockClusterMap();
//    localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
//    blobIdFactory = new BlobIdFactory(mockClusterMap);
//    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
//    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet);
//    mockServerLayout = new MockServerLayout(mockClusterMap);
//    replicasCount =
//        mockClusterMap.getRandomWritablePartition(MockClusterMap.DEFAULT_PARTITION_CLASS, null).getReplicaIds().size();
//    responseHandler = new ResponseHandler(mockClusterMap);
//    MockNetworkClientFactory networkClientFactory =
//        new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
//            CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
//    mockNetworkClient = networkClientFactory.getMockNetworkClient();
//    routerCallback = new RouterCallback(mockNetworkClient, new ArrayList<BackgroundDeleteRequest>());
//    GetBlobOperation getBlobOperation = new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId,
//        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet),
//        null, routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false,
//        quotaChargeCallback, partiallyReadableBlobDb);
//    this.mySqlListIterator = getBlobOperation.new MySqlListItr(parentList, accountName, containerName, blobName, -1);
//  }
//
//  private Properties getDefaultNonBlockingRouterProperties(boolean excludeTimeout) {
//    Properties properties = new Properties();
//    properties.setProperty("router.hostname", "localhost");
//    properties.setProperty("router.datacenter.name", LOCAL_DC);
//    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
//    properties.setProperty("router.put.success.target", Integer.toString(2));
//    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
//    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
//    properties.setProperty("router.get.success.target", Integer.toString(1));
//    properties.setProperty("router.get.operation.tracker.type", operationTrackerType);
//    properties.setProperty("router.request.timeout.ms", Integer.toString(20));
//    properties.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(excludeTimeout));
//    properties.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
//    properties.setProperty("router.get.blob.operation.share.memory", "true");
//    return properties;
//  }
//
//  @Test
//  public void testHasNext() {
//    boolean hasNext = mySqlListIterator.hasNext();
//    Assert.assertEquals(hasNext, false);
//    parentList.add(null);
//    hasNext = mySqlListIterator.hasNext();
//    Assert.assertEquals(hasNext, true);
//    PartiallyReadableBlobRecord record = new PartiallyReadableBlobRecord(accountName, containerName, blobName,
//        "AAYQAf____8AAQAAAAAAAAAA1IaNBDJtTReTu-xTfs_QkA", 0L, 4194304L,
//        1660707189892L, "PENDING");
//    try {
//      partiallyReadableBlobDb.put(record);
//    }
//    catch (RestServiceException e) {
//
//    }
//    hasNext = mySqlListIterator.hasNext();
//    Assert.assertEquals(hasNext, true);
//    PartiallyReadableBlobRecord recordWithDifferentName = new PartiallyReadableBlobRecord(accountName, containerName,
//        "dummyName", "AAYQAf____8AAQAAAAAAAAAA1IaNBDJtTReTu-xTfs_QTT", 0L, 4194304L,
//        1660707189892L, "PENDING");
//    try {
//      partiallyReadableBlobDb.put(record);
//    }
//    catch (RestServiceException e) {
//
//    }
//    hasNext = mySqlListIterator.hasNext();
//    Assert.assertEquals(hasNext, false);
//  }
//}
