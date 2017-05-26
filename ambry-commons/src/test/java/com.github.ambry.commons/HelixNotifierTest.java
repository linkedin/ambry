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
package com.github.ambry.commons;

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.ZNRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.commons.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link HelixNotifier}.
 */
public class HelixNotifierTest {
  private static int zkClientConnectTimeoutMs = 20000;
  private static int zkClientSessionTimeoutMs = 20000;
  private static String zkClientConnectString = "localhost:2182";
  private static String storeRootPath = "/ambry/testCluster/helixPropertyStore";

  private static final HelixPropertyStoreFactory<ZNRecord> storeFactory = new MockHelixPropertyStoreFactory<>();
  private static final List<String> refTopics = Arrays.asList(new String[]{"Topic1", "Topic2"});
  private static final List<String> refMessages = Arrays.asList(new String[]{"Message1", "Message2"});
  private final List<String> receivedTopicsByListener0 = new ArrayList<>();
  private final List<String> receivedTopicsByListener1 = new ArrayList<>();
  private final List<String> receivedMessagesByListener0 = new ArrayList<>();
  private final List<String> receivedMessagesByListener1 = new ArrayList<>();
  private final AtomicReference<CountDownLatch> latch0 = new AtomicReference<>();
  private final AtomicReference<CountDownLatch> latch1 = new AtomicReference<>();
  private static HelixPropertyStoreConfig storeConfig;
  private static long latchTimeoutMs = 1000;
  private HelixNotifier notifier;
  private List<TopicListener<String>> listeners;

  @BeforeClass
  public static void initialize() throws Exception {
    storeConfig = HelixPropertyStoreUtils.getHelixStoreConfig(zkClientConnectString, zkClientSessionTimeoutMs,
        zkClientConnectTimeoutMs, storeRootPath);
  }

  @Before
  public void setup() throws Exception {
    resetTopicListeners();
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
    listeners = new ArrayList<>();
    notifier = new HelixNotifier(storeConfig, storeFactory);
    listeners.add(new ListenerForTest(latch0, receivedTopicsByListener0, receivedMessagesByListener0));
    listeners.add(new ListenerForTest(latch1, receivedTopicsByListener1, receivedMessagesByListener1));
  }

  @AfterClass
  public static void cleanStore() throws Exception {
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
  }

  /**
   * Tests normal operations using a single {@link HelixNotifier} and two {@link TopicListener}s.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testHelixNotifier() throws Exception {
    // Subscribe a topic and send a message for the topic
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));

    // Send a different message for the topic
    resetTopicListeners();
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(1), receivedMessagesByListener0.get(0));

    // Subscribe to a different topic and send message for that topic
    resetTopicListeners();
    notifier.subscribe(refTopics.get(1), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(1), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(1), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));
  }

  /**
   * Tests non-alphabetNumeric characters.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testNonAlphabetNumericMessage() throws Exception {
    String nonAlphabetNumericMessage = "!@#$%^&*(){}|?><~";
    latch0.set(new CountDownLatch(1));
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    notifier.sendMessage(refTopics.get(0), nonAlphabetNumericMessage);
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Wrong number of Listeners for the topic", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", nonAlphabetNumericMessage,
        receivedMessagesByListener0.get(0));
  }

  /**
   * Tests two {@link TopicListener}s subscribe to the same topic through the same {@link HelixNotifier}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testTwoListenersForTheSameTopic() throws Exception {
    notifier.subscribe(refTopics.get(1), listeners.get(0));
    notifier.subscribe(refTopics.get(1), listeners.get(1));
    latch0.set(new CountDownLatch(1));
    latch1.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    awaitLatchOrTimeout(latch1.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(1), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(1), receivedMessagesByListener0.get(0));
    assertEquals("Received topic is different from expected", refTopics.get(1), receivedTopicsByListener1.get(0));
    assertEquals("Received message is different from expected", refMessages.get(1), receivedMessagesByListener1.get(0));
  }

  /**
   * Tests two {@link TopicListener}s subscribe to two different topics through the same {@link HelixNotifier}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testTwoListenersForDifferentTopics() throws Exception {
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    notifier.subscribe(refTopics.get(1), listeners.get(1));
    latch0.set(new CountDownLatch(1));
    latch1.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    notifier.sendMessage(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    awaitLatchOrTimeout(latch1.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));
    assertEquals("Received topic is different from expected", refTopics.get(1), receivedTopicsByListener1.get(0));
    assertEquals("Received message is different from expected", refMessages.get(1), receivedMessagesByListener1.get(0));
  }

  /**
   * Tests one {@link TopicListener} simultaneously listens to two different topics through the same {@link HelixNotifier}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testOneListenerForTwoDifferentTopics() throws Exception {
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    notifier.subscribe(refTopics.get(1), listeners.get(0));
    latch0.set(new CountDownLatch(2));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    notifier.sendMessage(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Received topics are different from expected", new HashSet<>(refTopics),
        new HashSet<>(receivedTopicsByListener0));
    assertEquals("Received messages are different from expected", new HashSet<>(refMessages),
        new HashSet<>(receivedMessagesByListener0));
  }

  /**
   * Tests unsubscribing a topic. This test is meaningful when using {@link MockHelixPropertyStoreFactory}, where
   * a single thread guarantees to know if {@link TopicListener#onMessageReceive(String, Object)} has been
   * called or not.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUnsubscribeTopic() throws Exception {
    // Subscribe a topic and send a message for the topic
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));

    // unsubscribe the listener
    notifier.unsubscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    try {
      awaitLatchOrTimeout(latch0.get(), 1);
      fail("should have thrown");
    } catch (TimeoutException e) {
      // expected, since the TopicListener would not be called, and the latch would not be counted down.
    }

    // unsubscribe the listener again should be silent
    notifier.unsubscribe(refTopics.get(0), listeners.get(0));
  }

  /**
   * Tests unsubscribing a non-existent {@link TopicListener}. This should not throw any exception.
   */
  @Test
  public void testUnsubscribeNonExistentListeners() {
    // unsubscribe a non-existent listener should be silent
    notifier.unsubscribe(refTopics.get(0), listeners.get(0));
  }

  /**
   * Tests sending a message to a topic without any {@link TopicListener}. This test is meaningful
   * when using {@link MockHelixPropertyStoreFactory}, where a single thread guarantees to know if
   * {@link TopicListener#onMessageReceive(String, Object)} has been called or not.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void sendMessageToTopicWithNoListeners() throws Exception {
    latch0.set(new CountDownLatch(1));
    notifier.sendMessage(refTopics.get(0), refMessages.get(0));
    try {
      awaitLatchOrTimeout(latch0.get(), 1);
      fail("should have thrown");
    } catch (TimeoutException e) {
      // expected, since the TopicListener would not be called, and the latch would not be counted down.
    }
  }

  /**
   * Tests a {@link TopicListener} subscribes a topic from one {@link HelixNotifier}, and receives a message sent
   * through another {@link HelixNotifier} for the same topic.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testOneListenerTwoNotifiers() throws Exception {
    Notifier notifier_2 = new HelixNotifier(storeConfig, storeFactory);
    notifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    notifier_2.sendMessage(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), latchTimeoutMs);
    assertEquals(1, receivedTopicsByListener0.size());
    assertEquals(refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals(1, receivedMessagesByListener0.size());
    assertEquals(refMessages.get(0), receivedMessagesByListener0.get(0));
  }

  /**
   * Tests when sending a message to a {@link TopicListener}, if the {@link TopicListener#onMessageReceive(String, Object)}
   * method throws exception, it will not crash the {@link HelixNotifier}.
   */
  @Test
  public void testSendMessageToBadListener() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    TopicListener listener = new TopicListener() {
      @Override
      public void onMessageReceive(String topic, Object message) {
        latch.countDown();
        throw new RuntimeException("Exception thrown from TopicListener");
      }
    };
    String topic = "topic";
    notifier.subscribe(topic, listener);
    notifier.sendMessage(topic, "message");
    awaitLatchOrTimeout(latch, latchTimeoutMs);
  }

  /**
   * Tests a number of bad inputs.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testBadInputs() throws Exception {
    // subscribe to null topic
    try {
      notifier.subscribe(null, listeners.get(0));
    } catch (IllegalArgumentException e) {
      // expected
    }

    // subscribe using null listener
    try {
      notifier.subscribe(refTopics.get(0), null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    // send message to a null topic
    try {
      notifier.sendMessage(null, refMessages.get(0));
    } catch (IllegalArgumentException e) {
      // expected
    }

    // send null message to a topic
    try {
      notifier.sendMessage(refTopics.get(0), null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    // unsubscribe a null listener from a topic
    try {
      notifier.unsubscribe(refTopics.get(0), null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    // unsubscribe a listener from a null topic
    try {
      notifier.unsubscribe(null, listeners.get(0));
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * A {@link TopicListener} for test purpose. It will count down the passed in latch after it has processed the
   * message.
   */
  private class ListenerForTest implements TopicListener<String> {
    private final AtomicReference<CountDownLatch> latch;
    private final List<String> receivedTopics;
    private final List<String> receivedMessages;

    public ListenerForTest(AtomicReference<CountDownLatch> latch, List<String> receivedTopics,
        List<String> receivedMessages) {
      this.latch = latch;
      this.receivedTopics = receivedTopics;
      this.receivedMessages = receivedMessages;
    }

    @Override
    public void onMessageReceive(String topic, String message) {
      System.out.println("Topic is: " + topic + ", referenceMessage1 is: " + message);
      receivedTopics.add(topic);
      receivedMessages.add(message);
      latch.get().countDown();
    }
  }

  /**
   * Resets {@link AtomicReference}s that are used to record results for {@link TopicListener}s.
   * @throws Exception Any unexpected exceptions.
   */
  private void resetTopicListeners() throws Exception {
    receivedTopicsByListener0.clear();
    receivedTopicsByListener1.clear();
    receivedMessagesByListener0.clear();
    receivedMessagesByListener1.clear();
    latch0.set(null);
    latch1.set(null);
  }
}
