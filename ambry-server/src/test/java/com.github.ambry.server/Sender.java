package com.github.ambry.server;

import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.messageformat.BlobProperties;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Sending thread to put request to ambry
 */
class Sender implements Runnable {

  BlockingQueue<Payload> payloadQueue;
  CountDownLatch completedLatch;
  int numberOfRequests;
  Coordinator coordinator;

  public Sender(LinkedBlockingQueue<Payload> payloadQueue, CountDownLatch completedLatch, int numberOfRequests,
      Coordinator coordinator) {
    this.payloadQueue = payloadQueue;
    this.completedLatch = completedLatch;
    this.numberOfRequests = numberOfRequests;
    this.coordinator = coordinator;
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < numberOfRequests; i++) {
        int size = new Random().nextInt(5000);
        BlobProperties properties = new BlobProperties(size, "service1", "owner id check", "image/jpeg", false);
        byte[] metadata = new byte[new Random().nextInt(1000)];
        byte[] blob = new byte[size];
        new Random().nextBytes(metadata);
        new Random().nextBytes(blob);
        String blobId = coordinator.putBlob(properties, ByteBuffer.wrap(metadata), new ByteArrayInputStream(blob));
        payloadQueue.put(new Payload(properties, metadata, blob, blobId));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      completedLatch.countDown();
    }
  }
}

