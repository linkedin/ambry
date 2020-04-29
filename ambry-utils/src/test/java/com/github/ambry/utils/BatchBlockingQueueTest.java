/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link BatchBlockingQueue}.
 */
public class BatchBlockingQueueTest {
  /**
   * Test the behavior of a {@link BatchBlockingQueue} under various scenarios (items queued up, wakeup called, etc.).
   */
  @Test
  public void testBasics() throws Exception {
    int pollTimeoutMs = 10000;
    String wakeupMarker = "WAKEUP";
    BatchBlockingQueue<String> queue = new BatchBlockingQueue<>(wakeupMarker);
    assertEquals("Queue size wrong", 0, queue.size());

    // one item in queue
    queue.put("a");
    assertEquals("Queue size wrong", 1, queue.size());
    List<String> polled = queue.poll(pollTimeoutMs);
    assertEquals("Unexpected poll result", Collections.singletonList("a"), polled);
    assertEquals("Queue size wrong", 0, queue.size());

    // multiple items in queue
    queue.put("b");
    queue.put("c");
    queue.put("d");
    assertEquals("Queue size wrong", 3, queue.size());
    polled = new ArrayList<>();
    queue.poll(polled, pollTimeoutMs);
    assertEquals("Unexpected poll result", Arrays.asList("b", "c", "d"), polled);
    assertEquals("Queue size wrong", 0, queue.size());

    // no items in queue, short timeout should kick in
    polled = queue.poll(10);
    assertEquals("Unexpected poll result", Collections.emptyList(), polled);
    assertEquals("Queue size wrong", 0, queue.size());

    // no items in queue, wakeup to end poll early
    long startTimeMs = System.currentTimeMillis();
    CompletableFuture<List<String>> pollFuture = CompletableFuture.supplyAsync(() -> queue.poll(pollTimeoutMs));
    queue.wakeup();
    polled = pollFuture.join();
    long timeTaken = System.currentTimeMillis() - startTimeMs;
    assertTrue("Took too long to poll with wakeup call: " + timeTaken, timeTaken < pollTimeoutMs);
    assertEquals("Unexpected poll result", Collections.emptyList(), polled);
    assertEquals("Queue size wrong", 0, queue.size());

    // items and wakeup markers in queue, only the items should be returned.
    queue.wakeup();
    queue.put("e");
    queue.put("f");
    queue.wakeup();
    queue.wakeup();
    queue.put("g");
    assertEquals("Queue size wrong", 6, queue.size());
    polled = queue.poll(pollTimeoutMs);
    assertEquals("Unexpected poll result", Arrays.asList("e", "f", "g"), polled);
    assertEquals("Queue size wrong", 0, queue.size());

    // should not be able to put a wakeup marker in the queue
    TestUtils.assertException(IllegalArgumentException.class, () -> queue.put(wakeupMarker), null);
  }
}