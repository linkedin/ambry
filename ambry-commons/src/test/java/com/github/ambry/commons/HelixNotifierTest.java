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

import com.github.ambry.clustermap.HelixStoreOperator;
import com.github.ambry.clustermap.MockHelixPropertyStore;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link HelixNotifier}.
 */
public class HelixNotifierTest {
  private static final int ZK_CLIENT_CONNECT_TIMEOUT_MS = 20 * 1000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20 * 1000;
  private static final String ZK_CLIENT_CONNECT_STRING = "dummyHost:dummyPort";
  private static final String STORAGE_ROOT_PATH = "/ambry/testCluster/helixPropertyStore";
  private static final long LATCH_TIMEOUT_MS = 1000;
  private static final List<String> receivedTopicsByListener0 = new ArrayList<>();
  private static final List<String> receivedTopicsByListener1 = new ArrayList<>();
  private static final List<String> receivedMessagesByListener0 = new ArrayList<>();
  private static final List<String> receivedMessagesByListener1 = new ArrayList<>();
  private static final AtomicReference<CountDownLatch> latch0 = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> latch1 = new AtomicReference<>();
  private static final List<TopicListener<String>> listeners = new ArrayList<>();
  private static final List<String> refTopics = new ArrayList<>();
  private static final List<String> refMessages = new ArrayList<>();
  private static final HelixPropertyStoreConfig storeConfig =
      getHelixStoreConfig(ZK_CLIENT_CONNECT_STRING, ZK_CLIENT_SESSION_TIMEOUT_MS, ZK_CLIENT_CONNECT_TIMEOUT_MS,
          STORAGE_ROOT_PATH);
  private final Map<String, MockHelixPropertyStore<ZNRecord>> storeKeyToMockStoreMap = new HashMap<>();
  private HelixNotifier helixNotifier;

  @Before
  public void setup() throws Exception {
    deleteStoreIfExists();
    storeKeyToMockStoreMap.clear();
    helixNotifier = new HelixNotifier(getMockHelixStore(storeConfig));
    resetReferenceTopicsAndMessages();
    resetListeners();
  }

  @After
  public void cleanStore() throws Exception {
    deleteStoreIfExists();
  }

  /**
   * Tests normal operations using a single {@link HelixNotifier} and two {@link TopicListener}s.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testHelixNotifier() throws Exception {
    // Subscribe a topic and publish a message for the topic
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));

    // publish a different message for the topic
    resetListeners();
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(1), receivedMessagesByListener0.get(0));

    // Subscribe to a different topic and publish message for that topic
    resetListeners();
    helixNotifier.subscribe(refTopics.get(1), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(1), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals("Received topic is different from expected", refTopics.get(1), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));
  }

  /**
   * Tests two {@link TopicListener}s subscribe to the same topic through the same {@link HelixNotifier}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testTwoListenersForTheSameTopic() throws Exception {
    helixNotifier.subscribe(refTopics.get(1), listeners.get(0));
    helixNotifier.subscribe(refTopics.get(1), listeners.get(1));
    latch0.set(new CountDownLatch(1));
    latch1.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    awaitLatchOrTimeout(latch1.get(), LATCH_TIMEOUT_MS);
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
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    helixNotifier.subscribe(refTopics.get(1), listeners.get(1));
    latch0.set(new CountDownLatch(1));
    latch1.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    helixNotifier.publish(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    awaitLatchOrTimeout(latch1.get(), LATCH_TIMEOUT_MS);
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
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    helixNotifier.subscribe(refTopics.get(1), listeners.get(0));
    latch0.set(new CountDownLatch(2));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    helixNotifier.publish(refTopics.get(1), refMessages.get(1));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals("Received topics are different from expected", new HashSet<>(refTopics),
        new HashSet<>(receivedTopicsByListener0));
    assertEquals("Received messages are different from expected", new HashSet<>(refMessages),
        new HashSet<>(receivedMessagesByListener0));
  }

  /**
   * Tests unsubscribing a topic. This test is meaningful when using {@link MockHelixPropertyStore}, where
   * a single thread guarantees to know if {@link TopicListener#onMessage(String, Object)} has been
   * called or not.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUnsubscribeTopic() throws Exception {
    // Subscribe a topic and publish a message for the topic
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals("Received topic is different from expected", refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals("Received message is different from expected", refMessages.get(0), receivedMessagesByListener0.get(0));

    // unsubscribe the listener
    helixNotifier.unsubscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    try {
      awaitLatchOrTimeout(latch0.get(), 1);
      fail("should have thrown");
    } catch (TimeoutException e) {
      // expected, since the TopicListener would not be called, and the latch would not be counted down.
    }

    // unsubscribe the listener again should be silent
    helixNotifier.unsubscribe(refTopics.get(0), listeners.get(0));
  }

  /**
   * Tests unsubscribing a non-existent {@link TopicListener}. This should not throw any exception.
   */
  @Test
  public void testUnsubscribeNonExistentListeners() {
    // unsubscribe a non-existent listener should be silent
    helixNotifier.unsubscribe(refTopics.get(0), listeners.get(0));
  }

  /**
   * Tests publishing a message to a topic without any {@link TopicListener}. This test is meaningful
   * when using {@link MockHelixPropertyStore}, where a single thread guarantees to know if
   * {@link TopicListener#onMessage(String, Object)} has been called or not.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void publishMessageToTopicWithNoListeners() throws Exception {
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
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
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    HelixNotifier helixNotifier2 = new HelixNotifier(getMockHelixStore(storeConfig));
    helixNotifier2.publish(refTopics.get(0), refMessages.get(0));
    awaitLatchOrTimeout(latch0.get(), LATCH_TIMEOUT_MS);
    assertEquals(1, receivedTopicsByListener0.size());
    assertEquals(refTopics.get(0), receivedTopicsByListener0.get(0));
    assertEquals(1, receivedMessagesByListener0.size());
    assertEquals(refMessages.get(0), receivedMessagesByListener0.get(0));
  }

  /**
   * Tests when publishing a message to a {@link TopicListener}, if the {@link TopicListener#onMessage(String, Object)}
   * method throws exception, it will not crash the {@link HelixNotifier}.
   */
  @Test
  public void testPublishMessageToBadListener() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    TopicListener listener = (topic, message) -> {
      latch.countDown();
      throw new RuntimeException("Exception thrown from TopicListener");
    };
    String topic = "topic";
    helixNotifier.subscribe(topic, listener);
    helixNotifier.publish(topic, "message");
    awaitLatchOrTimeout(latch, LATCH_TIMEOUT_MS);
  }

  /**
   * Tests a number of bad inputs.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testBadInputs() throws Exception {
    // subscribe to null topic
    try {
      helixNotifier.subscribe(null, listeners.get(0));
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // subscribe using null listener
    try {
      helixNotifier.subscribe(refTopics.get(0), null);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // publish message to a null topic
    try {
      helixNotifier.publish(null, refMessages.get(0));
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // publish null message to a topic
    try {
      helixNotifier.publish(refTopics.get(0), null);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // unsubscribe a null listener from a topic
    try {
      helixNotifier.unsubscribe(refTopics.get(0), null);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // unsubscribe a listener from a null topic
    try {
      helixNotifier.unsubscribe(null, listeners.get(0));
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // pass null storeConfig to construct a HelixNotifier
    try {
      new HelixNotifier(ZK_CLIENT_CONNECT_STRING, (HelixPropertyStoreConfig) null);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // pass null store to construct a HelixNotifier
    try {
      new HelixNotifier((HelixPropertyStore<ZNRecord>) null);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Tests when publishing a message through {@link HelixNotifier} fails, it will not throw exception or fail the
   * caller's thread.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testFailToPublishMessage() throws Exception {
    helixNotifier = new HelixNotifier(new MockHelixPropertyStore<ZNRecord>(true, false));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
  }

  /**
   * Tests a corner case when a {@link HelixNotifier} sends messages to local {@link TopicListener}s, there is a
   * slight chance that when the {@link HelixNotifier} tries to fetch the message, it is already deleted by someone
   * else, and may fetch {@code null}. This test ensures that in this case no exception will be thrown.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadNullRecordWhenSendMessageToLocalListeners() throws Exception {
    helixNotifier = new HelixNotifier(new MockHelixPropertyStore<ZNRecord>(false, true));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    helixNotifier.subscribe(refTopics.get(0), listeners.get(0));
    latch0.set(new CountDownLatch(1));
    helixNotifier.publish(refTopics.get(0), refMessages.get(0));
    assertEquals("TopicListener incorrectly called when the record is null", 1, latch0.get().getCount());
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
    public void onMessage(String topic, String message) {
      System.out.println("Topic is: " + topic + ", referenceMessage1 is: " + message);
      receivedTopics.add(topic);
      receivedMessages.add(message);
      if (latch.get().getCount() > 0) {
        latch.get().countDown();
      } else {
        // since this callback is called in the main thread, it is ok to fail the test here.
        fail("Countdown latch has already been counted down to 0");
      }
    }
  }

  /**
   * Resets {@code listeners} and {@link AtomicReference}s that are used to record results for {@link TopicListener}s.
   * @throws Exception Any unexpected exceptions.
   */
  private void resetListeners() throws Exception {
    listeners.clear();
    listeners.add(new ListenerForTest(latch0, receivedTopicsByListener0, receivedMessagesByListener0));
    listeners.add(new ListenerForTest(latch1, receivedTopicsByListener1, receivedMessagesByListener1));
    receivedTopicsByListener0.clear();
    receivedTopicsByListener1.clear();
    receivedMessagesByListener0.clear();
    receivedMessagesByListener1.clear();
    latch0.set(null);
    latch1.set(null);
  }

  /**
   * Resets reference topic list and message list.
   */
  private void resetReferenceTopicsAndMessages() {
    refTopics.clear();
    refMessages.clear();
    refTopics.add(UUID.randomUUID().toString());
    refTopics.add(UUID.randomUUID().toString());
    refMessages.add(UUID.randomUUID().toString());
    refMessages.add(UUID.randomUUID().toString());
  }

  /**
   * Delete corresponding {@code ZooKeeper} nodes of a {@link HelixPropertyStore} if exist.
   * @throws InterruptedException
   */
  private void deleteStoreIfExists() throws Exception {
    HelixStoreOperator storeOperator = new HelixStoreOperator(getMockHelixStore(storeConfig));
    // check if the store exists by checking if root path (e.g., "/") exists in the store.
    if (storeOperator.exist("/")) {
      storeOperator.delete("/");
    }
  }

  /**
   * Gets a {@link MockHelixPropertyStore} for the given {@link HelixPropertyStoreConfig}.
   * @param storeConfig A {@link HelixPropertyStoreConfig}.
   * @return A {@link MockHelixPropertyStore} defined by the {@link HelixPropertyStoreConfig}.
   */
  private MockHelixPropertyStore<ZNRecord> getMockHelixStore(HelixPropertyStoreConfig storeConfig) {
    String storeRootPath = ZK_CLIENT_CONNECT_STRING + storeConfig.rootPath;
    MockHelixPropertyStore<ZNRecord> helixStore = storeKeyToMockStoreMap.get(storeRootPath);
    if (helixStore == null) {
      helixStore = new MockHelixPropertyStore<>();
      storeKeyToMockStoreMap.put(storeRootPath, helixStore);
    }
    return helixStore;
  }

  /**
   * A util method that generates {@link HelixPropertyStoreConfig}.
   * @param zkClientConnectString The connect string to connect to a {@code ZooKeeper}.
   * @param zkClientSessionTimeoutMs Timeout for a zk session.
   * @param zkClientConnectionTimeoutMs Timeout for a zk connection.
   * @param storeRootPath The root path of a store in {@code ZooKeeper}.
   * @return {@link HelixPropertyStoreConfig} defined by the arguments.
   */
  public static HelixPropertyStoreConfig getHelixStoreConfig(String zkClientConnectString, int zkClientSessionTimeoutMs,
      int zkClientConnectionTimeoutMs, String storeRootPath) {
    Properties helixConfigProps = new Properties();
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(zkClientConnectionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(zkClientSessionTimeoutMs));
    helixConfigProps.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, zkClientConnectString);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", storeRootPath);
    VerifiableProperties vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    return new HelixPropertyStoreConfig(vHelixConfigProps);
  }
}
