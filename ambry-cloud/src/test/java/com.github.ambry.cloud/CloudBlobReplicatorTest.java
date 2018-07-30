package com.github.ambry.cloud;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.CloudReplicationConfig;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.InputStreamReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.CryptoService;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.KeyManagementService;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.SingleKeyManagementServiceFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.crypto.spec.SecretKeySpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class CloudBlobReplicatorTest {

  private Router router;
  private AccountService accountService;
  private InMemoryBlobEventSource eventSource;
  private CloudDestinationFactory destinationFactory;
  private CloudDestination destination;
  private CloudBlobReplicator replicator;
  private short accountId = 101, containerId = 102;
  private String accountName = "ambryTeam";
  private String containerName = "ambrytest";
  private String serviceId = "TestService";

  // Only need these for intg test
  private boolean useRealCloudService = false;
  private String configSpec;
  //String blobFilePath = System.getProperty("blob.file.path");
  //String containerName = System.getProperty("azure.container.name");

  @Before
  public void setup() throws Exception {

    useRealCloudService = Boolean.parseBoolean(System.getProperty("use.real.cloud.service"));
    if (useRealCloudService) {
      configSpec = System.getProperty("cloud.config.spec");
      destinationFactory = CloudDestinationFactory.getInstance();
    } else {
      configSpec = "AccountName=ambry;AccountKey=ambry-kay";
      destinationFactory = mock(CloudDestinationFactory.class);
      CloudDestination mockDestination = mock(CloudDestination.class);
      when(mockDestination.uploadBlob(anyString(), anyString(), anyLong(), any())).thenReturn(true);
      when(mockDestination.deleteBlob(anyString(), anyString())).thenReturn(true);
      when(destinationFactory.getCloudDestination(any(), eq(configSpec))).thenReturn(mockDestination);
      destination = mockDestination;
    }

    Properties properties = new Properties();
    // hex string that decodes to 16 bytes
    String hexKey = "32ab01de67cafe6732ab01de67cafe67";
    properties.setProperty("kms.default.container.key", hexKey);
    VerifiableProperties verProps = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    router = new InMemoryRouter(verProps, clusterMap);
    accountService = new InMemAccountService(false, true);

    //
    // Build an account owning a container configured to publish to the cloud
    //

    //CryptoService cryptoService = new GCMCryptoServiceFactory(props, null).getCryptoService();
    CryptoService cryptoService = new DummyCryptoService();
    KeyManagementService kms = new SingleKeyManagementServiceFactory(verProps, null, null).getKeyManagementService();

    // Encrypt config spec
    Object keySpec = kms.getKey(accountId, containerId);
    ByteBuffer fuzzyBuf = cryptoService.encrypt(ByteBuffer.wrap(configSpec.getBytes()), keySpec);
    configSpec = new String(fuzzyBuf.array());

    CloudReplicationConfig containerCloudConfig =
        new CloudReplicationConfig(CloudDestinationType.AZURE.name(), configSpec, containerName);
    Container container = new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, null,
        accountId).setCloudConfig(containerCloudConfig).build();
    Account account =
        new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(ImmutableList.of(container))
            .build();
    accountService.updateAccounts(ImmutableList.of(account));

    eventSource = new InMemoryBlobEventSource();

    replicator = new CloudBlobReplicator().router(router)
        .accountService(accountService)
        .eventSource(eventSource)
        .destinationFactory(destinationFactory)
        .cryptoService(cryptoService)
        .keyService(kms);
    replicator.startup();
  }

  @After
  public void teardown() {
    if (replicator != null) {
      replicator.shutdown();
    }
  }

  @Test
  public void testBlobReplication() throws Exception {

    String blobFileName = "profile-photo.jpg";
    URL blobUrl = this.getClass().getClassLoader().getResource(blobFileName);
    String blobFilePath = blobUrl.getPath();
    File inputFile = new File(blobFilePath);
    if (!inputFile.canRead()) {
      throw new RuntimeException("Can't load resource: " + blobFilePath);
    }
    long blobSize = inputFile.length();
    FileInputStream inputStream = new FileInputStream(blobFilePath);

    ReadableStreamChannel channel =
        new InputStreamReadableStreamChannel(inputStream, blobSize, Executors.newSingleThreadExecutor());

    BlobProperties blobProps = new BlobProperties(blobSize, serviceId, accountId, containerId, false);
    Future<String> fut = router.putBlob(blobProps, null, channel, null);
    String blobId = fut.get();
    assertNotNull(blobId);

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    if (!useRealCloudService) {
      verify(destination).uploadBlob(eq(blobId), eq(containerName), anyLong(), any());
    }
    assertTrue(eventSource.q.isEmpty());

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.DELETE));
    if (!useRealCloudService) {
      verify(destination).deleteBlob(eq(blobId), eq(containerName));
    }
    assertTrue(eventSource.q.isEmpty());
  }

  // TODO:
  // PUT blob with TTL should not be uploaded
  // UPDATE blob with no TTL should be uploaded
  // PUT of blob second time should not be uploaded
  // DELETE of non-existing blob should fail
  // Simulate exceptions, use bad account, container, blobId, configSpec

  private static class DummyCryptoService implements CryptoService<SecretKeySpec> {

    @Override
    public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return toEncrypt;
    }

    @Override
    public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return toDecrypt;
    }

    @Override
    public ByteBuffer encryptKey(SecretKeySpec toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return null;
    }

    @Override
    public SecretKeySpec decryptKey(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return null;
    }
  }

  public static class InMemoryBlobEventSource implements BlobEventSource {
    private BlobEventConsumer consumer;
    private Queue<BlobEvent> q;

    public InMemoryBlobEventSource() {
      q = new ArrayBlockingQueue<BlobEvent>(100);
    }

    public void pumpEvent(BlobEvent blobEvent) {
      // put event in queue
      q.add(blobEvent);

      // Drain the queue
      while (!q.isEmpty()) {
        BlobEvent event = q.peek();
        if (consumer.onBlobEvent(event)) {
          q.remove();
        } else {
          break;
        }
      }
    }

    @Override
    public void subscribe(BlobEventConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void unsubscribe(BlobEventConsumer consumer) {
      // whatever
    }
  }
}
