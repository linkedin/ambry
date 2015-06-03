package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.storageservice.BlobStorageService;
import com.github.ambry.storageservice.MockBlobStorageService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * TODO: write description
 */
public class MockRestRequestDelegator implements RestRequestDelegator {
  private final BlobStorageService blobStorageService;
  private final CountDownLatch mockRestMessageHandlersUp;
  private final int handlerCount;
  private final List<MockRestMessageHandler> mockRestMessageHandlers;
  private final ServerMetrics serverMetrics = new MockExternalServerMetrics(new MetricRegistry());

  private ExecutorService executor;
  private int currIndex = 0;

  public MockRestRequestDelegator() {
    this(1, new MockBlobStorageService());
  }

  public MockRestRequestDelegator(int handlerCount, BlobStorageService blobStorageService) {
    this.handlerCount = handlerCount;
    this.blobStorageService = blobStorageService;
    mockRestMessageHandlers = new ArrayList<MockRestMessageHandler>(handlerCount);
    mockRestMessageHandlersUp = new CountDownLatch(handlerCount);
  }

  public void start()
      throws InstantiationException {
    if (handlerCount > 0) {
      executor = Executors.newFixedThreadPool(handlerCount);
      for (int i = 0; i < handlerCount; i++) {
        MockRestMessageHandler messageHandler = new MockRestMessageHandler(blobStorageService, serverMetrics);
        executor.execute(messageHandler);
        mockRestMessageHandlers.add(messageHandler);
      }
    } else {
      throw new InstantiationException("Handlers to be created is <= 0 - (is " + handlerCount + ")");
    }
  }

  public synchronized RestMessageHandler getMessageHandler()
      throws RestException {
    try {
      MockRestMessageHandler messageHandler = mockRestMessageHandlers.get(currIndex);
      currIndex = (currIndex + 1) % mockRestMessageHandlers.size();
      return messageHandler;
    } catch (Exception e) {
      throw new RestException("Error while trying to pick a handler to return", RestErrorCode.HandlerSelectionError);
    }
  }

  public void shutdown()
      throws Exception {
  }
}
