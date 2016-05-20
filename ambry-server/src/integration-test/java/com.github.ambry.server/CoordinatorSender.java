/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.messageformat.BlobProperties;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Sending thread to put request to ambry through Coordinator
 */
class CoordinatorSender implements Runnable {

  BlockingQueue<Payload> payloadQueue;
  CountDownLatch completedLatch;
  int numberOfRequests;
  Coordinator coordinator;

  public CoordinatorSender(LinkedBlockingQueue<Payload> payloadQueue, CountDownLatch completedLatch,
      int numberOfRequests, Coordinator coordinator) {
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

