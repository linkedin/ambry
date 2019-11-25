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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.router.CryptoTestUtils.*;
import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;


/**
 * Tests {@link CryptoJobHandler}
 */
public class CryptoJobHandlerTest {
  static final int DEFAULT_THREAD_COUNT = 2;
  static final int DEFAULT_KEY_SIZE = 64;
  private static final int MAX_DATA_SIZE_IN_BYTES = 10000;
  private static final int RANDOM_KEY_SIZE_IN_BITS = 256;
  private static final String ENCRYPT_JOB_TYPE = "encrypt";
  private static final String DECRYPT_JOB_TYPE = "decrypt";
  private static final String CLUSTER_NAME = TestUtils.getRandomString(10);
  private static final MetricRegistry REGISTRY = new MetricRegistry();
  private final CryptoService<SecretKeySpec> cryptoService;
  private final KeyManagementService<SecretKeySpec> kms;
  private final ClusterMap referenceClusterMap;
  private final String defaultKey;
  private final VerifiableProperties verifiableProperties;
  private final NonBlockingRouterMetrics routerMetrics;
  private CryptoJobHandler cryptoJobHandler;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
    nettyByteBufLeakHelper.setDisabled(false);
  }

  public CryptoJobHandlerTest() throws IOException, GeneralSecurityException {
    defaultKey = TestUtils.getRandomKey(DEFAULT_KEY_SIZE);
    Properties props = getKMSProperties(defaultKey, RANDOM_KEY_SIZE_IN_BITS);
    verifiableProperties = new VerifiableProperties((props));
    kms = new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
    cryptoService = new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
    cryptoJobHandler = new CryptoJobHandler(DEFAULT_THREAD_COUNT);
    referenceClusterMap = new MockClusterMap();
    routerMetrics = new NonBlockingRouterMetrics(referenceClusterMap, null);
  }

  @After
  public void cleanup() {
    cryptoJobHandler.close();
  }

  /**
   * Tests {@link CryptoJobHandler} for happy path. i.e. Encrypt and Decrypt jobs are executed and callback invoked
   * @throws GeneralSecurityException
   * @throws InterruptedException
   */
  @Test
  public void testCryptoJobExecutorService() throws GeneralSecurityException, InterruptedException {
    int totalDataCount = 10;
    CountDownLatch encryptCallBackCount = new CountDownLatch(totalDataCount * 3);
    CountDownLatch decryptCallBackCount = new CountDownLatch(totalDataCount * 3);
    SecretKeySpec perBlobKey = kms.getRandomKey();
    for (int i = 0; i < totalDataCount; i++) {
      testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount, Mode.Data);
      testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount, Mode.UserMetadata);
      testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount, Mode.Both);
    }
    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobHandler} for with different worker count
   * @throws GeneralSecurityException
   * @throws InterruptedException
   */
  @Test
  public void testCryptoJobExecutorServiceDiffThreadCount() throws GeneralSecurityException, InterruptedException {
    int totalDataCount = 10;
    for (int j = 0; j < 5; j++) {
      cryptoJobHandler.close();
      cryptoJobHandler = new CryptoJobHandler(j + 1);
      CountDownLatch encryptCallBackCount = new CountDownLatch(totalDataCount);
      CountDownLatch decryptCallBackCount = new CountDownLatch(totalDataCount);
      SecretKeySpec perBlobKey = kms.getRandomKey();
      for (int i = 0; i < totalDataCount; i++) {
        testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount, Mode.Both);
      }
      awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
      awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
    }
  }

  /**
   * Tests {@link CryptoJobHandler} for failures during encryption
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testEncryptionFailure() throws InterruptedException, GeneralSecurityException {
    cryptoJobHandler.close();
    MockCryptoService mockCryptoService = new MockCryptoService(new CryptoServiceConfig(verifiableProperties));
    mockCryptoService.exceptionOnEncryption.set(
        new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobHandler = new CryptoJobHandler(DEFAULT_THREAD_COUNT);
    SecretKeySpec perBlobSecretKey = kms.getRandomKey();
    testFailureOnEncryption(perBlobSecretKey, mockCryptoService, kms);
    mockCryptoService.clearStates();
    cryptoJobHandler.close();
    MockKeyManagementService mockKms = new MockKeyManagementService(new KMSConfig(verifiableProperties), defaultKey);
    mockKms.exceptionToThrow.set(new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobHandler = new CryptoJobHandler(DEFAULT_THREAD_COUNT);
    testFailureOnEncryption(perBlobSecretKey, cryptoService, mockKms);
  }

  /**
   * Tests {@link CryptoJobHandler} for failures during decryption
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testDecryptionFailure() throws InterruptedException, GeneralSecurityException {
    cryptoJobHandler.close();
    MockCryptoService mockCryptoService = new MockCryptoService(new CryptoServiceConfig(verifiableProperties));
    mockCryptoService.exceptionOnDecryption.set(
        new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobHandler = new CryptoJobHandler(DEFAULT_THREAD_COUNT);
    SecretKeySpec perBlobSecretKey = kms.getRandomKey();
    testFailureOnDecryption(perBlobSecretKey, null, false, mockCryptoService, kms);
    mockCryptoService.clearStates();
    cryptoJobHandler.close();
    MockKeyManagementService mockKms = new MockKeyManagementService(new KMSConfig(verifiableProperties), defaultKey);
    cryptoJobHandler = new CryptoJobHandler(DEFAULT_THREAD_COUNT);
    testFailureOnDecryption(perBlobSecretKey, mockKms, true, cryptoService, mockKms);
  }

  /**
   * Tests {@link CryptoJobHandler} for pending encrypt jobs callback after closing the thread
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testPendingEncryptJobs() throws InterruptedException, GeneralSecurityException {
    nettyByteBufLeakHelper.setDisabled(true);
    int testDataCount = 10;
    int closeOnCount = 4;
    CountDownLatch encryptCallBackCount = new CountDownLatch(testDataCount);
    List<EncryptJob> encryptJobs = new ArrayList<>();
    SecretKeySpec perBlobKey = kms.getRandomKey();
    for (int i = 0; i < testDataCount; i++) {
      TestBlobData testData = getRandomBlob(referenceClusterMap);
      if (i < closeOnCount) {
        cryptoJobHandler.submitJob(
            new EncryptJob(testData.blobId.getAccountId(), testData.blobId.getContainerId(), testData.blobContent,
                testData.userMetadata, perBlobKey, cryptoService, kms,
                new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
                (EncryptJob.EncryptJobResult result, Exception exception) -> {
                  encryptCallBackCount.countDown();
                  assertNotNull("Encrypted content should not be null", result.getEncryptedBlobContent());
                  assertNotNull("Encrypted user-metadata should not be null", result.getEncryptedUserMetadata());
                  assertNotNull("Encrypted key should not be null", result.getEncryptedKey());
                  result.getEncryptedBlobContent().release();
                }));
      } else {
        encryptJobs.add(
            new EncryptJob(testData.blobId.getAccountId(), testData.blobId.getContainerId(), testData.blobContent,
                testData.userMetadata, perBlobKey, cryptoService, kms,
                new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
                (EncryptJob.EncryptJobResult result, Exception exception) -> {
                  encryptCallBackCount.countDown();
                  if (exception == null) {
                    assertNotNull("Encrypted content should not be null", result.getEncryptedBlobContent());
                    assertNotNull("Encrypted user-metadata should not be null", result.getEncryptedUserMetadata());
                    assertNotNull("Encrypted key should not be null", result.getEncryptedKey());
                    result.getEncryptedBlobContent().release();
                  } else {
                    assertTrue("Exception cause should have been GeneralSecurityException",
                        exception instanceof GeneralSecurityException);
                    assertNull("Result should have been null", result);
                  }
                }));
      }
    }
    // add special job that will close the thread. Add all the encrypt jobs to the queue before closing the thread.
    TestBlobData probeData = getRandomBlob(referenceClusterMap);
    cryptoJobHandler.submitJob(
        new EncryptJob(probeData.blobId.getAccountId(), probeData.blobId.getContainerId(), probeData.blobContent,
            probeData.userMetadata, perBlobKey, cryptoService, kms,
            new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              result.getEncryptedBlobContent().release();
              for (EncryptJob encryptJob : encryptJobs) {
                cryptoJobHandler.submitJob(encryptJob);
              }
              new Thread(new CloseCryptoHandler(cryptoJobHandler)).start();
            }));

    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobHandler} for pending decrypt jobs callback after closing the thread
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testPendingDecryptJobs() throws InterruptedException, GeneralSecurityException {
    nettyByteBufLeakHelper.setDisabled(true);
    int testDataCount = 10;
    CountDownLatch encryptCallBackCount = new CountDownLatch(testDataCount);
    CountDownLatch decryptCallBackCount = new CountDownLatch(testDataCount);
    SecretKeySpec perBlobKey = kms.getRandomKey();
    List<DecryptJob> decryptJobs = new CopyOnWriteArrayList<>();
    for (int i = 0; i < testDataCount; i++) {
      TestBlobData testData = getRandomBlob(referenceClusterMap);
      cryptoJobHandler.submitJob(
          new EncryptJob(testData.blobId.getAccountId(), testData.blobId.getContainerId(), testData.blobContent.retainedDuplicate(),
              testData.userMetadata, perBlobKey, cryptoService, kms,
              new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
              (EncryptJob.EncryptJobResult encryptJobResult, Exception exception) -> {
                assertNotNull("Encrypted content should not be null", encryptJobResult.getEncryptedBlobContent());
                assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());
                decryptJobs.add(new DecryptJob(testData.blobId, encryptJobResult.getEncryptedKey(),
                    encryptJobResult.getEncryptedBlobContent(), encryptJobResult.getEncryptedUserMetadata(),
                    cryptoService, kms, new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics),
                    (DecryptJob.DecryptJobResult decryptJobResult, Exception e) -> {
                      if (e == null) {
                        assertEquals("BlobId mismatch ", testData.blobId, decryptJobResult.getBlobId());
                        assertNull("Exception shouldn't have been thrown to decrypt contents for " + testData.blobId,
                            exception);
                        assertNotNull("Decrypted contents should not be null",
                            decryptJobResult.getDecryptedBlobContent());
                        byte[] blobContent = new byte[testData.blobContent.readableBytes()];
                        testData.blobContent.readBytes(blobContent);
                        assertArrayEquals("Decrypted content and plain bytes should match",
                            blobContent, decryptJobResult.getDecryptedBlobContent().array());
                        assertArrayEquals("Decrypted userMetadata and plain bytes should match",
                            testData.userMetadata.array(), decryptJobResult.getDecryptedUserMetadata().array());
                      } else {
                        assertNotNull("Exception should have been thrown to decrypt contents for " + testData.blobId,
                            e);
                        assertTrue("Exception cause should have been GeneralSecurityException",
                            e instanceof GeneralSecurityException);
                        assertNull("Result should have been null", decryptJobResult);
                      }
                      decryptCallBackCount.countDown();
                    }));
                encryptCallBackCount.countDown();
              }));
    }

    // wait for all encryption to complete
    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);

    // add special job that will close the thread. Add all the decrypt jobs to the queue before closing the thread.
    TestBlobData probeData = getRandomBlob(referenceClusterMap);
    cryptoJobHandler.submitJob(
        new EncryptJob(probeData.blobId.getAccountId(), probeData.blobId.getContainerId(), probeData.blobContent,
            probeData.userMetadata, perBlobKey, cryptoService, kms,
            new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              result.getEncryptedBlobContent().release();
              for (DecryptJob decryptJob : decryptJobs) {
                cryptoJobHandler.submitJob(decryptJob);
              }
              new Thread(new CloseCryptoHandler(cryptoJobHandler)).start();
            }));
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobHandler} for encrypt and decrypt calls after closing the thread
   * @throws GeneralSecurityException
   */
  @Test
  public void testCryptoJobHandlerClose() throws GeneralSecurityException {
    SecretKeySpec perBlobKey = kms.getRandomKey();
    TestBlobData randomData = getRandomBlob(referenceClusterMap);
    cryptoJobHandler.close();

    cryptoJobHandler.submitJob(
        new EncryptJob(randomData.blobId.getAccountId(), randomData.blobId.getContainerId(), randomData.blobContent,
            randomData.userMetadata, perBlobKey, cryptoService, kms,
            new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              fail("Callback should not have been called since CryptoWorker is closed");
            }));

    cryptoJobHandler.submitJob(
        new DecryptJob(randomData.blobId, randomData.blobContent.nioBuffer(), randomData.blobContent,
            randomData.userMetadata, cryptoService, kms, new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics),
            (DecryptJob.DecryptJobResult result, Exception exception) -> {
              fail("Callback should not have been called since CryptoWorker is closed");
            }));
    randomData.blobContent.release();
  }

  /**
   * Close the {@link CryptoJobHandler} asynchronously
   */
  private class CloseCryptoHandler implements Runnable {
    private final CryptoJobHandler cryptoJobHandler;

    private CloseCryptoHandler(CryptoJobHandler cryptoJobHandler) {
      this.cryptoJobHandler = cryptoJobHandler;
    }

    @Override
    public void run() {
      cryptoJobHandler.close();
    }
  }

  /**
   * Tests encryption and decryption flow for happy path
   * @param perBlobKey the {@link SecretKeySpec} representing the per blob key
   * @param encryptCallBackCount {@link CountDownLatch} to track encryption callbacks
   * @param decryptCallBackCount {@link CountDownLatch} to track decryption callbacks
   * @param mode State to be tested for encrypt decrypt flow. Blob content only, UserMetadata only, or both
   */
  private void testEncryptDecryptFlow(SecretKeySpec perBlobKey, CountDownLatch encryptCallBackCount,
      CountDownLatch decryptCallBackCount, Mode mode) {
    TestBlobData randomData = getRandomBlob(referenceClusterMap);
    ByteBuf content = mode != Mode.UserMetadata ? randomData.blobContent.retainedDuplicate() : null;
    ByteBuffer userMetadata = mode != Mode.Data ? randomData.userMetadata : null;
    cryptoJobHandler.submitJob(
        new EncryptJob(randomData.blobId.getAccountId(), randomData.blobId.getContainerId(), content, userMetadata,
            perBlobKey, cryptoService, kms, new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            new EncryptCallbackVerifier(randomData.blobId, false, encryptCallBackCount,
                new DecryptCallbackVerifier(randomData.blobId, content != null?content.nioBuffer():null, userMetadata, false,
                    decryptCallBackCount), mode)));
    randomData.blobContent.release();
  }

  /**
   * Test failure during encryption
   * @param perBlobKey the {@link SecretKeySpec} representing the per blob key
   * @throws InterruptedException
   */
  private void testFailureOnEncryption(SecretKeySpec perBlobKey, CryptoService cryptoService, KeyManagementService kms)
      throws InterruptedException {
    TestBlobData testData = getRandomBlob(referenceClusterMap);
    CountDownLatch encryptCallBackCount = new CountDownLatch(1);
    cryptoJobHandler.submitJob(
        new EncryptJob(testData.blobId.getAccountId(), testData.blobId.getContainerId(), testData.blobContent,
            testData.userMetadata, perBlobKey, cryptoService, kms,
            new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            new EncryptCallbackVerifier(testData.blobId, true, encryptCallBackCount, null, Mode.Both)));
    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
  }

  /**
   * Test failure during decryption
   * @param perBlobKey the {@link SecretKeySpec} representing the per blob key
   * @param mockKMS {@link MockKeyManagementService} that mocks {@link KeyManagementService}
   * @param setExceptionForKMS {@code true} if exception needs to be set using {@link MockKeyManagementService}
   *                           {@code false} otherwise
   * @throws InterruptedException
   */
  private void testFailureOnDecryption(SecretKeySpec perBlobKey, MockKeyManagementService mockKMS,
      boolean setExceptionForKMS, CryptoService cryptoService, KeyManagementService kms) throws InterruptedException {
    TestBlobData testData = getRandomBlob(referenceClusterMap);
    CountDownLatch encryptCallBackCount = new CountDownLatch(1);
    CountDownLatch decryptCallBackCount = new CountDownLatch(1);
    cryptoJobHandler.submitJob(
        new EncryptJob(testData.blobId.getAccountId(), testData.blobId.getContainerId(), testData.blobContent.retainedDuplicate(),
            testData.userMetadata, perBlobKey, cryptoService, kms,
            new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
            (EncryptJob.EncryptJobResult encryptJobResult, Exception exception) -> {
              encryptCallBackCount.countDown();
              assertNull("Exception shouldn't have been thrown to encrypt contents for " + testData.blobId, exception);
              assertNotNull("Encrypted contents should not be null", encryptJobResult.getEncryptedBlobContent());
              assertNotNull("Encrypted user-metadata should not be null", encryptJobResult.getEncryptedUserMetadata());
              assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());

              // set exception using MockKMS
              if (setExceptionForKMS) {
                mockKMS.exceptionToThrow.set(
                    new GeneralSecurityException("Exception to test", new IllegalStateException()));
              }
              cryptoJobHandler.submitJob(new DecryptJob(testData.blobId, encryptJobResult.getEncryptedKey(),
                  encryptJobResult.getEncryptedBlobContent(), encryptJobResult.getEncryptedUserMetadata(),
                  cryptoService, kms, new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics),
                  (DecryptJob.DecryptJobResult result, Exception e) -> {
                    decryptCallBackCount.countDown();
                    assertNotNull("Exception should have been thrown to decrypt contents for " + testData.blobId, e);
                    assertTrue("Exception cause should have been GeneralSecurityException",
                        e instanceof GeneralSecurityException);
                    assertNull("Result should have been null", result);
                  }));
            }));
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
    testData.blobContent.release();
  }

  /**
   * Encrypt callback verifier. Verifies non null for arguments and adds a decrypt job to the jobQueue on successful completion.
   * Else, verifies the exception is set correctly.
   */
  private class EncryptCallbackVerifier implements Callback<EncryptJob.EncryptJobResult> {
    private final BlobId blobId;
    private final boolean expectException;
    private final CountDownLatch countDownLatch;
    private final DecryptCallbackVerifier decryptCallBackVerifier;
    private final Mode mode;

    EncryptCallbackVerifier(BlobId blobId, boolean expectException, CountDownLatch encryptCountDownLatch,
        DecryptCallbackVerifier decryptCallBackVerifier, Mode mode) {
      this.blobId = blobId;
      this.expectException = expectException;
      this.countDownLatch = encryptCountDownLatch;
      this.decryptCallBackVerifier = decryptCallBackVerifier;
      this.mode = mode;
    }

    @Override
    public void onCompletion(EncryptJob.EncryptJobResult encryptJobResult, Exception exception) {
      if (countDownLatch != null) {
        countDownLatch.countDown();
      }
      if (!expectException) {
        assertNull("Exception shouldn't have been thrown to encrypt contents for " + blobId, exception);
        if (mode != Mode.UserMetadata) {
          assertNotNull("Encrypted content should not be null", encryptJobResult.getEncryptedBlobContent());
        }
        if (mode != Mode.Data) {
          assertNotNull("Encrypted userMetadata should not be null", encryptJobResult.getEncryptedUserMetadata());
        }
        assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());
        cryptoJobHandler.submitJob(
            new DecryptJob(blobId, encryptJobResult.getEncryptedKey(), encryptJobResult.getEncryptedBlobContent(),
                encryptJobResult.getEncryptedUserMetadata(), cryptoService, kms,
                new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics), (result, e) -> {
              decryptCallBackVerifier.onCompletion(result, e);
            }));
      } else {
        assertNotNull("Exception should have been thrown to encrypt contents for " + blobId, exception);
        assertTrue("Exception cause should have been GeneralSecurityException",
            exception instanceof GeneralSecurityException);
        assertNull("Result should have been null", encryptJobResult);
      }
    }
  }

  /**
   * Decrypt callback verifier. Verifies the decrypted content matches raw content on successful completion.
   * Else, verifies the exception is set correctly.
   */
  private class DecryptCallbackVerifier implements Callback<DecryptJob.DecryptJobResult> {
    private final BlobId blobId;
    private final boolean expectException;
    private final ByteBuffer unencryptedContent;
    private final ByteBuffer unencryptedUserMetadata;
    private final CountDownLatch countDownLatch;

    DecryptCallbackVerifier(BlobId blobId, ByteBuffer unencryptedContent, ByteBuffer unencryptedUserMetadata,
        boolean expectException, CountDownLatch countDownLatch) {
      this.blobId = blobId;
      this.unencryptedContent = unencryptedContent;
      this.unencryptedUserMetadata = unencryptedUserMetadata;
      this.expectException = expectException;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void onCompletion(DecryptJob.DecryptJobResult result, Exception exception) {
      if (countDownLatch != null) {
        countDownLatch.countDown();
      }
      if (!expectException) {
        assertEquals("BlobId mismatch ", blobId, result.getBlobId());
        assertNull("Exception shouldn't have been thrown to decrypt contents for " + blobId, exception);
        if (unencryptedContent != null) {
          assertNotNull("Decrypted contents should not be null", result.getDecryptedBlobContent());
          assertArrayEquals("Decrypted content and plain bytes should match", unencryptedContent.array(),
              result.getDecryptedBlobContent().array());
        }
        if (unencryptedUserMetadata != null) {
          assertNotNull("Decrypted userMetadata should not be null", result.getDecryptedUserMetadata());
          assertArrayEquals("Decrypted userMetadata and plain bytes should match", unencryptedUserMetadata.array(),
              result.getDecryptedUserMetadata().array());
        }
      } else {
        assertNotNull("Exception should have been thrown to decrypt contents for " + blobId, exception);
        assertTrue("Exception cause should have been GeneralSecurityException",
            exception instanceof GeneralSecurityException);
        assertNull("Result should have been null", result);
      }
    }
  }

  /**
   * Awaits for {@code countDownLatch} to count down to 0.
   * @param countDownLatch the {@link CountDownLatch} that needs to be awaited against
   * @param jobType the job type of the count down latch
   * @throws InterruptedException
   */
  static void awaitCountDownLatch(CountDownLatch countDownLatch, String jobType) throws InterruptedException {
    if (!countDownLatch.await(10, TimeUnit.SECONDS)) {
      fail("Not all " + jobType + " callbacks have been returned. Pending count: " + countDownLatch.getCount());
    }
  }

  /**
   * Generate and return random test data (i.e. BlobId, blob content and user-metadata)
   * @param referenceClusterMap clusterMap from which partition info to be fetched
   * @return {@link TestBlobData} which contains all data for the test blob
   */
  private TestBlobData getRandomBlob(ClusterMap referenceClusterMap) {
    BlobId blobId = getNewBlobId(referenceClusterMap);
    int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE_IN_BYTES);
    byte[] data = new byte[size];
    TestUtils.RANDOM.nextBytes(data);
    ByteBuf blob = ByteBufAllocator.DEFAULT.heapBuffer(size);
    blob.writeBytes(data);
    size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE_IN_BYTES);
    byte[] userMetadata = new byte[size];
    TestUtils.RANDOM.nextBytes(userMetadata);
    return new TestBlobData(blobId, ByteBuffer.wrap(userMetadata), blob);
  }

  /**
   * Generate new {@link BlobId}
   * @param referenceClusterMap clusterMap from which partition info to be fetched
   * @return newly generated {@link BlobId}
   */
  private BlobId getNewBlobId(ClusterMap referenceClusterMap) {
    byte dc = (byte) TestUtils.RANDOM.nextInt(3);
    BlobId.BlobIdType type = TestUtils.RANDOM.nextBoolean() ? BlobId.BlobIdType.NATIVE : BlobId.BlobIdType.CRAFTED;
    return new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), type, dc, getRandomShort(TestUtils.RANDOM),
        getRandomShort(TestUtils.RANDOM),
        referenceClusterMap.getRandomWritablePartition(MockClusterMap.DEFAULT_PARTITION_CLASS, null), false,
        BlobId.BlobDataType.DATACHUNK);
  }

  /**
   * Mode to represent the entity that needs to be tested for encryption and decryption
   */
  enum Mode {
    Data,
    UserMetadata,
    Both
  }

  /**
   * Class to hold all data for a test blob like BlobId, blob content and user-metadata
   */
  static class TestBlobData {
    BlobId blobId;
    ByteBuffer userMetadata;
    ByteBuf blobContent;

    TestBlobData(BlobId blobId, ByteBuffer userMetadata, ByteBuf blobContent) {
      this.blobId = blobId;
      this.userMetadata = userMetadata;
      this.blobContent = blobContent;
    }
  }
}
