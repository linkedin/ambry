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
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.UtilsTest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.CryptoTestUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * Tests {@link CryptoJobExecutorService}
 */
public class CryptoJobExecutorServiceTest {
  private static final int MAX_DATA_SIZE_IN_BYTES = 10000;
  private static final int DEFAULT_KEY_SIZE = 64;
  private static final int RANDOM_KEY_SIZE_IN_BITS = 256;
  private static final int DEFAULT_THREAD_COUNT = 2;
  private static final String ENCRYPT_JOB_TYPE = "encrypt";
  private static final String DECRYPT_JOB_TYPE = "decrypt";
  private static final String CLUSTER_NAME = UtilsTest.getRandomString(10);
  private static final MetricRegistry REGISTRY = new MetricRegistry();
  private final CryptoService<SecretKeySpec> cryptoService;
  private final KeyManagementService<SecretKeySpec> kms;
  private final ClusterMap referenceClusterMap;
  private final String defaultKey;
  private final VerifiableProperties verifiableProperties;
  private CryptoJobExecutorService cryptoJobExecutorService;

  public CryptoJobExecutorServiceTest() throws IOException, GeneralSecurityException {
    defaultKey = getRandomKey(DEFAULT_KEY_SIZE);
    Properties props = getKMSProperties(defaultKey, RANDOM_KEY_SIZE_IN_BITS);
    verifiableProperties = new VerifiableProperties((props));
    kms = new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
    cryptoService = new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
    cryptoJobExecutorService = new CryptoJobExecutorService(DEFAULT_THREAD_COUNT);
    referenceClusterMap = new MockClusterMap();
    cryptoJobExecutorService.start();
  }

  @After
  public void cleanup() {
    cryptoJobExecutorService.close();
  }

  /**
   * Tests {@link CryptoJobExecutorService} for happy path. i.e. Encrypt and Decrypt jobs are executed and callback invoked
   * @throws GeneralSecurityException
   * @throws InterruptedException
   */
  @Test
  public void testCryptoJobExecutorService() throws GeneralSecurityException, InterruptedException {
    int totalDataCount = 50;
    CountDownLatch encryptCallBackCount = new CountDownLatch(totalDataCount);
    CountDownLatch decryptCallBackCount = new CountDownLatch(totalDataCount);
    SecretKeySpec perBlobKey = kms.getRandomKey();
    for (int i = 0; i < totalDataCount; i++) {
      testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount);
    }
    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobExecutorService} for with different worker count
   * @throws GeneralSecurityException
   * @throws InterruptedException
   */
  @Test
  public void testCryptoJobExecutorServiceDiffThreadCount() throws GeneralSecurityException, InterruptedException {
    int totalDataCount = 10;
    for (int j = 0; j < 5; j++) {
      cryptoJobExecutorService.close();
      cryptoJobExecutorService = new CryptoJobExecutorService(j + 1);
      cryptoJobExecutorService.start();
      CountDownLatch encryptCallBackCount = new CountDownLatch(totalDataCount);
      CountDownLatch decryptCallBackCount = new CountDownLatch(totalDataCount);
      SecretKeySpec perBlobKey = kms.getRandomKey();
      for (int i = 0; i < totalDataCount; i++) {
        testEncryptDecryptFlow(perBlobKey, encryptCallBackCount, decryptCallBackCount);
      }
      awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
      awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
    }
  }

  /**
   * Tests {@link CryptoJobExecutorService} for failures during encryption
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testEncryptionFailure() throws InterruptedException, GeneralSecurityException {
    cryptoJobExecutorService.close();
    MockCryptoService mockCryptoService = new MockCryptoService(new CryptoServiceConfig(verifiableProperties));
    mockCryptoService.exceptionOnEncryption.set(
        new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobExecutorService = new CryptoJobExecutorService(DEFAULT_THREAD_COUNT);
    cryptoJobExecutorService.start();
    SecretKeySpec perBlobSecretKey = kms.getRandomKey();
    testFailureOnEncryption(perBlobSecretKey, mockCryptoService, kms);
    mockCryptoService.clearStates();
    cryptoJobExecutorService.close();
    MockKeyManagementService mockKms = new MockKeyManagementService(new KMSConfig(verifiableProperties), defaultKey);
    mockKms.exceptionToThrow.set(new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobExecutorService = new CryptoJobExecutorService(DEFAULT_THREAD_COUNT);
    cryptoJobExecutorService.start();
    testFailureOnEncryption(perBlobSecretKey, cryptoService, mockKms);
  }

  /**
   * Tests {@link CryptoJobExecutorService} for failures during decryption
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testDecryptionFailure() throws InterruptedException, GeneralSecurityException {
    cryptoJobExecutorService.close();
    MockCryptoService mockCryptoService = new MockCryptoService(new CryptoServiceConfig(verifiableProperties));
    mockCryptoService.exceptionOnDecryption.set(
        new GeneralSecurityException("Exception to test", new IllegalStateException()));
    cryptoJobExecutorService = new CryptoJobExecutorService(DEFAULT_THREAD_COUNT);
    cryptoJobExecutorService.start();
    SecretKeySpec perBlobSecretKey = kms.getRandomKey();
    testFailureOnDecryption(perBlobSecretKey, null, false, mockCryptoService, kms);
    mockCryptoService.clearStates();
    cryptoJobExecutorService.close();
    MockKeyManagementService mockKms = new MockKeyManagementService(new KMSConfig(verifiableProperties), defaultKey);
    cryptoJobExecutorService = new CryptoJobExecutorService(DEFAULT_THREAD_COUNT);
    cryptoJobExecutorService.start();
    testFailureOnDecryption(perBlobSecretKey, mockKms, true, cryptoService, mockKms);
  }

  /**
   * Tests {@link CryptoJobExecutorService} for pending encrypt jobs callback after closing the thread
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testPendingEncryptJobs() throws InterruptedException, GeneralSecurityException {
    int testDataCount = 10;
    int closeOnCount = 4;
    CountDownLatch encryptCallBackCount = new CountDownLatch(testDataCount);
    List<EncryptJob> encryptJobs = new ArrayList<>();
    SecretKeySpec perBlobKey = kms.getRandomKey();
    for (int i = 0; i < testDataCount; i++) {
      Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
      if (i < closeOnCount) {
        cryptoJobExecutorService.submitJob(
            new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
                (EncryptJob.EncryptJobResult result, Exception exception) -> {
                  encryptCallBackCount.countDown();
                  Assert.assertEquals("BlobId mismatch ", randomData.getFirst(), result.getBlobId());
                  Assert.assertNotNull("Encrypted content should not be null", result.getEncryptedContent());
                  Assert.assertNotNull("Encrypted key should not be null", result.getEncryptedKey());
                }));
      } else {
        encryptJobs.add(new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              encryptCallBackCount.countDown();
              Assert.assertEquals("BlobId mismatch ", randomData.getFirst(), result.getBlobId());
              if (exception == null) {
                Assert.assertNotNull("Encrypted content should not be null", result.getEncryptedContent());
                Assert.assertNotNull("Encrypted key should not be null", result.getEncryptedKey());
              } else {
                Assert.assertTrue("Exception cause should have been GeneralSecurityException",
                    exception instanceof GeneralSecurityException);
                Assert.assertNull("Encrypted contents should have been null", result.getEncryptedContent());
                Assert.assertNull("Encrypted key should have been null", result.getEncryptedKey());
              }
            }));
      }
    }
    // add special job that will close the thread. Add all the encrypt jobs to the queue before closing the thread.
    Pair<BlobId, ByteBuffer> probeData = getRandomBlob(referenceClusterMap);
    cryptoJobExecutorService.submitJob(
        new EncryptJob(probeData.getFirst(), probeData.getSecond(), perBlobKey, cryptoService, kms,
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              for (EncryptJob encryptJob : encryptJobs) {
                cryptoJobExecutorService.submitJob(encryptJob);
              }
              new Thread(new ThreadToCloseCryptoHandler(cryptoJobExecutorService)).start();
            }));

    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobExecutorService} for pending decrypt jobs callback after closing the thread
   * @throws InterruptedException
   * @throws GeneralSecurityException
   */
  @Test
  public void testPendingDecryptJobs() throws InterruptedException, GeneralSecurityException {
    int testDataCount = 10;
    CountDownLatch encryptCallBackCount = new CountDownLatch(testDataCount);
    CountDownLatch decryptCallBackCount = new CountDownLatch(testDataCount);
    SecretKeySpec perBlobKey = kms.getRandomKey();
    List<DecryptJob> decryptJobs = new CopyOnWriteArrayList<>();
    for (int i = 0; i < testDataCount; i++) {
      Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
      cryptoJobExecutorService.submitJob(
          new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
              (EncryptJob.EncryptJobResult encryptJobResult, Exception exception) -> {
                Assert.assertEquals("BlobId mismatch ", randomData.getFirst(), encryptJobResult.getBlobId());
                Assert.assertNotNull("Encrypted content should not be null", encryptJobResult.getEncryptedContent());
                Assert.assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());
                decryptJobs.add(new DecryptJob(randomData.getFirst(), encryptJobResult.getEncryptedKey(),
                    encryptJobResult.getEncryptedContent(), cryptoService, kms,
                    (DecryptJob.DecryptJobResult decryptJobResult, Exception e) -> {
                      Assert.assertEquals("BlobId mismatch ", randomData.getFirst(), decryptJobResult.getBlobId());
                      if (e == null) {
                        Assert.assertNull(
                            "Exception shouldn't have been thrown to decrypt contents for " + randomData.getFirst(),
                            exception);
                        Assert.assertNotNull("Decrypted contents should not be null",
                            decryptJobResult.getDecryptedContent());
                        Assert.assertArrayEquals("Decrypted bytes and plain bytes should match",
                            randomData.getSecond().array(), decryptJobResult.getDecryptedContent().array());
                      } else {
                        Assert.assertNotNull(
                            "Exception should have been thrown to decrypt contents for " + randomData.getFirst(), e);
                        Assert.assertTrue("Exception cause should have been GeneralSecurityException",
                            e instanceof GeneralSecurityException);
                        Assert.assertNull("Decrypted contents should have been null",
                            decryptJobResult.getDecryptedContent());
                      }
                      decryptCallBackCount.countDown();
                    }));
                encryptCallBackCount.countDown();
              }));
    }

    // wait for all encryption to complete
    awaitCountDownLatch(encryptCallBackCount, ENCRYPT_JOB_TYPE);

    // add special job that will close the thread. Add all the decrypt jobs to the queue before closing the thread.
    Pair<BlobId, ByteBuffer> probeData = getRandomBlob(referenceClusterMap);
    cryptoJobExecutorService.submitJob(
        new EncryptJob(probeData.getFirst(), probeData.getSecond(), perBlobKey, cryptoService, kms,
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              for (DecryptJob decryptJob : decryptJobs) {
                cryptoJobExecutorService.submitJob(decryptJob);
              }
              new Thread(new ThreadToCloseCryptoHandler(cryptoJobExecutorService)).start();
            }));
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
  }

  /**
   * Tests {@link CryptoJobExecutorService} for encrypt and decrypt calls after closing the thread
   * @throws GeneralSecurityException
   */
  @Test
  public void testCryptoJobHandlerClose() throws GeneralSecurityException {
    SecretKeySpec perBlobKey = kms.getRandomKey();
    Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
    cryptoJobExecutorService.close();

    cryptoJobExecutorService.submitJob(
        new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
            (EncryptJob.EncryptJobResult result, Exception exception) -> {
              Assert.fail("Callback should not have been called since CryptoWorker is closed");
            }));

    cryptoJobExecutorService.submitJob(
        new DecryptJob(randomData.getFirst(), randomData.getSecond(), randomData.getSecond(), cryptoService, kms,
            (DecryptJob.DecryptJobResult result, Exception exception) -> {
              Assert.fail("Callback should not have been called since CryptoWorker is closed");
            }));
  }

  /**
   * Thread to close the {@link CryptoJobExecutorService} asynchronously
   */
  private class ThreadToCloseCryptoHandler implements Runnable {
    private final CryptoJobExecutorService cryptoJobHandler;

    private ThreadToCloseCryptoHandler(CryptoJobExecutorService cryptoJobHandler) {
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
   */
  private void testEncryptDecryptFlow(SecretKeySpec perBlobKey, CountDownLatch encryptCallBackCount,
      CountDownLatch decryptCallBackCount) {
    Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
    cryptoJobExecutorService.submitJob(
        new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
            new EncryptCallbackVerifier(randomData.getFirst(), false, encryptCallBackCount,
                new DecryptCallbackVerifier(randomData.getFirst(), randomData.getSecond(), false,
                    decryptCallBackCount))));
  }

  /**
   * Test failure during encryption
   * @param perBlobKey the {@link SecretKeySpec} representing the per blob key
   * @throws InterruptedException
   */
  private void testFailureOnEncryption(SecretKeySpec perBlobKey, CryptoService cryptoService, KeyManagementService kms)
      throws InterruptedException {
    Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
    CountDownLatch encryptCallBackCount = new CountDownLatch(1);
    cryptoJobExecutorService.submitJob(
        new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
            new EncryptCallbackVerifier(randomData.getFirst(), true, encryptCallBackCount, null)));
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
    Pair<BlobId, ByteBuffer> randomData = getRandomBlob(referenceClusterMap);
    CountDownLatch encryptCallBackCount = new CountDownLatch(1);
    CountDownLatch decryptCallBackCount = new CountDownLatch(1);
    cryptoJobExecutorService.submitJob(
        new EncryptJob(randomData.getFirst(), randomData.getSecond(), perBlobKey, cryptoService, kms,
            (EncryptJob.EncryptJobResult encryptJobResult, Exception exception) -> {
              encryptCallBackCount.countDown();
              Assert.assertNull("Exception shouldn't have been thrown to encrypt contents for " + randomData.getFirst(),
                  exception);
              Assert.assertNotNull("Encrypted contents should not be null", encryptJobResult.getEncryptedContent());
              Assert.assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());
              Assert.assertEquals("BlobId mismatch", randomData.getFirst(), encryptJobResult.getBlobId());

              // set exception using MockKMS
              if (setExceptionForKMS) {
                mockKMS.exceptionToThrow.set(
                    new GeneralSecurityException("Exception to test", new IllegalStateException()));
              }
              cryptoJobExecutorService.submitJob(
                  new DecryptJob(randomData.getFirst(), encryptJobResult.getEncryptedKey(),
                      encryptJobResult.getEncryptedContent(), cryptoService, kms,
                      (DecryptJob.DecryptJobResult result, Exception e) -> {
                        decryptCallBackCount.countDown();
                        Assert.assertNotNull(
                            "Exception should have been thrown to decrypt contents for " + randomData.getFirst(), e);
                        Assert.assertTrue("Exception cause should have been GeneralSecurityException",
                            e instanceof GeneralSecurityException);
                        Assert.assertNull("Decrypted contents should have been null", result.getDecryptedContent());
                        Assert.assertEquals("BlobId mismatch", randomData.getFirst(), result.getBlobId());
                      }));
            }));
    awaitCountDownLatch(decryptCallBackCount, DECRYPT_JOB_TYPE);
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

    EncryptCallbackVerifier(BlobId blobId, boolean expectException, CountDownLatch encryptCountDownLatch,
        DecryptCallbackVerifier decryptCallBackVerifier) {
      this.blobId = blobId;
      this.expectException = expectException;
      this.countDownLatch = encryptCountDownLatch;
      this.decryptCallBackVerifier = decryptCallBackVerifier;
    }

    @Override
    public void onCompletion(EncryptJob.EncryptJobResult encryptJobResult, Exception exception) {
      Assert.assertEquals("BlobId mismatch ", blobId, encryptJobResult.getBlobId());
      if (countDownLatch != null) {
        countDownLatch.countDown();
      }
      if (!expectException) {
        Assert.assertNull("Exception shouldn't have been thrown to encrypt contents for " + blobId, exception);
        Assert.assertNotNull("Encrypted content should not be null", encryptJobResult.getEncryptedContent());
        Assert.assertNotNull("Encrypted key should not be null", encryptJobResult.getEncryptedKey());
        cryptoJobExecutorService.submitJob(
            new DecryptJob(blobId, encryptJobResult.getEncryptedKey(), encryptJobResult.getEncryptedContent(),
                cryptoService, kms, decryptCallBackVerifier));
      } else {
        Assert.assertNotNull("Exception should have been thrown to encrypt contents for " + blobId, exception);
        Assert.assertTrue("Exception cause should have been GeneralSecurityException",
            exception instanceof GeneralSecurityException);
        Assert.assertNull("Encrypted contents should have been null", encryptJobResult.getEncryptedContent());
        Assert.assertNull("Encrypted key should have been null", encryptJobResult.getEncryptedKey());
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
    private final CountDownLatch countDownLatch;

    DecryptCallbackVerifier(BlobId blobId, ByteBuffer unencryptedContent, boolean expectException,
        CountDownLatch countDownLatch) {
      this.blobId = blobId;
      this.unencryptedContent = unencryptedContent;
      this.expectException = expectException;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void onCompletion(DecryptJob.DecryptJobResult result, Exception exception) {
      if (countDownLatch != null) {
        countDownLatch.countDown();
      }
      Assert.assertEquals("BlobId mismatch ", blobId, result.getBlobId());
      if (!expectException) {
        Assert.assertNull("Exception shouldn't have been thrown to decrypt contents for " + blobId, exception);
        Assert.assertNotNull("Decrypted contents should not be null", result.getDecryptedContent());
        Assert.assertArrayEquals("Decrypted bytes and plain bytes should match", unencryptedContent.array(),
            result.getDecryptedContent().array());
      } else {
        Assert.assertNotNull("Exception should have been thrown to decrypt contents for " + blobId, exception);
        Assert.assertTrue("Exception cause should have been GeneralSecurityException",
            exception instanceof GeneralSecurityException);
        Assert.assertNull("Decrypted contents should have been null", result.getDecryptedContent());
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
      Assert.fail("Not all " + jobType + " callbacks have been returned. Pending count: " + countDownLatch.getCount());
    }
  }

  /**
   * Generate and return random data (i.e. BlobId and ByteBuffer)
   * @param referenceClusterMap clusterMap from which partition info to be fetched
   * @return a Pair of BlobId and ByteBuffer with random data
   */
  static Pair<BlobId, ByteBuffer> getRandomBlob(ClusterMap referenceClusterMap) {
    BlobId blobId = getNewBlobId(referenceClusterMap);
    int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE_IN_BYTES);
    byte[] data = new byte[size];
    TestUtils.RANDOM.nextBytes(data);
    ByteBuffer toEncrypt = ByteBuffer.wrap(data);
    return new Pair<>(blobId, toEncrypt);
  }

  /**
   * Generate new {@link BlobId}
   * @param referenceClusterMap clusterMap from which partition info to be fetched
   * @return newly generated {@link BlobId}
   */
  private static BlobId getNewBlobId(ClusterMap referenceClusterMap) {
    byte[] bytes = new byte[2];
    TestUtils.RANDOM.nextBytes(bytes);
    return new BlobId(bytes[0], bytes[1], getRandomShort(TestUtils.RANDOM), getRandomShort(TestUtils.RANDOM),
        referenceClusterMap.getWritablePartitionIds().get(0));
  }

  /**
   * MockCryptoService to assist in testing exception cases
   */
  static class MockCryptoService extends GCMCryptoService {

    AtomicReference<GeneralSecurityException> exceptionOnEncryption = new AtomicReference<>();
    AtomicReference<GeneralSecurityException> exceptionOnDecryption = new AtomicReference<>();

    MockCryptoService(CryptoServiceConfig cryptoServiceConfig) {
      super(cryptoServiceConfig);
    }

    @Override
    public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      if (exceptionOnEncryption.get() != null) {
        throw exceptionOnEncryption.get();
      }
      return super.encrypt(toEncrypt, key);
    }

    @Override
    public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      if (exceptionOnDecryption.get() != null) {
        throw exceptionOnDecryption.get();
      }
      return super.decrypt(toDecrypt, key);
    }

    void clearStates() {
      exceptionOnEncryption = new AtomicReference<>();
      exceptionOnDecryption = new AtomicReference<>();
    }
  }

  /**
   * MockKeyManagementService to assist in testing exception cases
   */
  static class MockKeyManagementService extends SingleKeyManagementService {

    AtomicReference<GeneralSecurityException> exceptionToThrow = new AtomicReference<>();

    MockKeyManagementService(KMSConfig KMSConfig, String defaultKey) throws GeneralSecurityException {
      super(KMSConfig, defaultKey);
    }

    @Override
    public SecretKeySpec getKey(short accountId, short containerId) throws GeneralSecurityException {
      if (exceptionToThrow.get() != null) {
        throw exceptionToThrow.get();
      } else {
        return super.getKey(accountId, containerId);
      }
    }
  }
}
