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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.network.SocketRequestResponseChannel;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link AmbryRequests}.
 */
public class AmbryRequestsTest {

  private final MockClusterMap clusterMap;
  private final MockStorageManager storageManager;
  private final MockRequestResponseChannel requestResponseChannel = new MockRequestResponseChannel();
  private final AmbryRequests ambryRequests;

  public AmbryRequestsTest() throws IOException, StoreException {
    clusterMap = new MockClusterMap();
    storageManager = new MockStorageManager();
    ambryRequests =
        new AmbryRequests(storageManager, requestResponseChannel, clusterMap, clusterMap.getDataNodeIds().get(0),
            clusterMap.getMetricRegistry(), null, null, null, null);
  }

  /**
   * Tests that compactions are scheduled correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId id : partitionIds) {
      doScheduleCompactionTest(id, ServerErrorCode.No_Error);
      assertEquals("Partition scheduled for compaction not as expected", id,
          storageManager.compactionScheduledPartitionId);
    }
  }

  /**
   * Tests failure scenarios for compaction - disk down, store not scheduled for compaction, exception while scheduling.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionFailureTest() throws InterruptedException, IOException {
    PartitionId id = clusterMap.getWritablePartitionIds().get(0);

    // store is not started - Disk_Unavailable
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.Disk_Unavailable);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // store cannot be scheduled for compaction - Unknown_Error
    storageManager.returnValueOfSchedulingCompaction = false;
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfSchedulingCompaction = true;

    // exception while attempting to schedule - InternalServerError
    storageManager.exceptionToThrowOnSchedulingCompaction = new IllegalStateException();
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnSchedulingCompaction = null;
  }

  // helpers
  // scheduleCompactionSuccessTest() and scheduleCompactionFailuresTest() helpers

  /**
   * Schedules a compaction for {@code id} and checks that the {@link ServerErrorCode} returned matches
   * {@code expectedServerErrorCode}.
   * @param id the {@link PartitionId} to schedule compaction for.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected when the request is processed.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doScheduleCompactionTest(PartitionId id, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.TriggerCompaction, id, correlationId, clientId);
    Request request = MockRequest.fromAdminRequest(adminRequest, true);
    ambryRequests.handleRequests(request);
    assertEquals("Request accompanying response does not match original request", request,
        requestResponseChannel.lastOriginalRequest);
    assertNotNull("Response not sent", requestResponseChannel.lastResponse);
    assertTrue("Response is not of type AdminResponse", requestResponseChannel.lastResponse instanceof AdminResponse);
    AdminResponse response = (AdminResponse) requestResponseChannel.lastResponse;
    assertEquals("Correlation id in response does match the one in the request", correlationId,
        response.getCorrelationId());
    assertEquals("Client id in response does match the one in the request", clientId, response.getClientId());
    assertEquals("Error code does not match expected", expectedServerErrorCode, response.getError());
  }

  /**
   * Implementation of {@link Request} to help with tests.
   */
  private static class MockRequest implements Request {

    private final InputStream stream;

    /**
     * Constructs a {@link MockRequest} from {@code adminRequest}.
     * @param adminRequest the {@link AdminRequest} to construct the {@link MockRequest} for.
     * @param prepareForHandling prepares for handling by {@link AmbryRequests#handleRequests(Request)} by reading
     *                           some initial field(s).
     * @return an instance of {@link MockRequest} that represents {@code adminRequest}.
     * @throws IOException
     */
    static MockRequest fromAdminRequest(AdminRequest adminRequest, boolean prepareForHandling) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate((int) adminRequest.sizeInBytes());
      adminRequest.writeTo(new ByteBufferChannel(buffer));
      buffer.flip();
      if (prepareForHandling) {
        // read length
        buffer.getLong();
      }
      return new MockRequest(new ByteBufferInputStream(buffer));
    }

    /**
     * Constructs a {@link MockRequest}.
     * @param stream the {@link InputStream} that will be returned on a call to {@link #getInputStream()}.
     */
    private MockRequest(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getInputStream() {
      return stream;
    }

    @Override
    public long getStartTimeInMs() {
      return 0;
    }
  }

  /**
   * An extension of {@link SocketRequestResponseChannel} to help with tests.
   */
  private static class MockRequestResponseChannel extends SocketRequestResponseChannel {
    /**
     * {@link Request} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Request lastOriginalRequest = null;

    /**
     * The {@link Send} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Send lastResponse = null;

    MockRequestResponseChannel() {
      super(1, 1);
    }

    @Override
    public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics) {
      lastResponse = payloadToSend;
      lastOriginalRequest = originalRequest;
    }
  }

  /**
   * An extension of {@link StorageManager} to help with tests.
   */
  private static class MockStorageManager extends StorageManager {

    /**
     * An empty {@link Store} implementation.
     */
    private static Store store = new Store() {
      @Override
      public void start() throws StoreException {

      }

      @Override
      public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions)
          throws StoreException {
        return null;
      }

      @Override
      public void put(MessageWriteSet messageSetToWrite) throws StoreException {

      }

      @Override
      public void delete(MessageWriteSet messageSetToDelete) throws StoreException {

      }

      @Override
      public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
        return null;
      }

      @Override
      public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
        return null;
      }

      @Override
      public StoreStats getStoreStats() {
        return null;
      }

      @Override
      public boolean isKeyDeleted(StoreKey key) throws StoreException {
        return false;
      }

      @Override
      public long getSizeInBytes() {
        return 0;
      }

      @Override
      public void shutdown() throws StoreException {

      }
    };

    /**
     * if {@code true}, a {@code null} {@link Store} is returned on a call to {@link #getStore(PartitionId)}. Otherwise
     * {@link #store} is returned.
     */
    boolean returnNullStore = false;
    /**
     * If non-null, the given exception is thrown when {@link #scheduleNextForCompaction(PartitionId)} is called.
     */
    RuntimeException exceptionToThrowOnSchedulingCompaction = null;
    /**
     * The return value for a call to {@link #scheduleNextForCompaction(PartitionId)}.
     */
    boolean returnValueOfSchedulingCompaction = true;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #scheduleNextForCompaction(PartitionId)}
     */
    PartitionId compactionScheduledPartitionId = null;

    MockStorageManager() throws StoreException {
      super(new StoreConfig(new VerifiableProperties(new Properties())), Utils.newScheduler(1, true),
          new MetricRegistry(), Collections.EMPTY_LIST, null, null, null, new MockTime());
    }

    @Override
    public Store getStore(PartitionId id) {
      return returnNullStore ? null : store;
    }

    @Override
    public boolean scheduleNextForCompaction(PartitionId id) {
      if (exceptionToThrowOnSchedulingCompaction != null) {
        throw exceptionToThrowOnSchedulingCompaction;
      }
      compactionScheduledPartitionId = id;
      return returnValueOfSchedulingCompaction;
    }
  }
}
