package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.storageservice.BlobStorageService;
import com.github.ambry.storageservice.MockBlobStorageService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * TODO: write description
 */
public class MockRestRequestDelegator implements RestRequestDelegator, HandleMessageEventListener {
  private final BlobStorageService blobStorageService;
  private final CountDownLatch mockRestMessageHandlersUp;
  private final int handlerCount;
  private final List<MockRestMessageHandler> mockRestMessageHandlers;
  private final ServerMetrics serverMetrics = new MockExternalServerMetrics(new MetricRegistry());

  private ExecutorService executor;
  private int currIndex = 0;
  private boolean isFaulty = false;

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
    if(!isFaulty) {
      try {
        MockRestMessageHandler messageHandler = mockRestMessageHandlers.get(currIndex);
        currIndex = (currIndex + 1) % mockRestMessageHandlers.size();
        return messageHandler;
      } catch (Exception e) {
        throw new RestException("Error while trying to pick a handler to return", RestErrorCode.HandlerSelectionError);
      }
    } else {
      throw new RestException("Error while trying to pick a handler to return", RestErrorCode.HandlerSelectionError);
    }
  }

  public void shutdown()
      throws Exception {
    if (executor != null) {
      for (int i = 0; i < mockRestMessageHandlers.size(); i++) {
        mockRestMessageHandlers.get(i).shutdownGracefully(this);
      }
      executor.shutdown();
      if (!awaitTermination(60, TimeUnit.SECONDS)) {
        throw new Exception("MockRestRequestDelegator shutdown failed after waiting for 60 seconds");
      }
      executor = null;
    }
  }

  public void breakdown() {
    isFaulty = true;
  }

  public void repair() {
    isFaulty = false;
  }

  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return mockRestMessageHandlersUp.await(3 * timeout / 4, timeUnit) &&
        executor.awaitTermination(timeout / 4, timeUnit);
  }

  public void onMessageHandleSuccess(MessageInfo messageInfo) {
    mockRestMessageHandlersUp.countDown();
  }

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
    throw new IllegalStateException("Should not have come here. Original exception" + e);
  }
}
