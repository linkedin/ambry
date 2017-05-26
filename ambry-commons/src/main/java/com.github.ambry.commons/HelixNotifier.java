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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *    A {@link Notifier} implementation using {@link HelixPropertyStore} as the underlying communication mechanism.
 * Each topic is created as a {@link ZNRecord} under the {@code topicPath}. A message for that topic will
 * be written to the simple field of the {@link ZNRecord} in the form of {@code "message": "message content"}.
 * Each {@link ZNRecord} under the {@code topicPath} will be used for a single topic.
 * </p>
 * <p>
 *   If multiple {@link HelixNotifier} running on different node are configured with the same
 *   {@link HelixPropertyStoreConfig}, they will essentially be in the same topic domain. I.e., {@link TopicListener}s
 *   can subscribe through any of these {@link HelixNotifier}s for a topic. A message of that topic sent through
 *   {@link HelixNotifier}-1 on node-1 can be received by all the {@link TopicListener}s on other nodes.
 * </p>
 */
public class HelixNotifier implements Notifier<String> {
  private static final String TOPIC_PATH = "/topics";
  private static final String MESSAGE_KEY = "message";
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final Map<TopicListener, HelixPropertyListener> topicListenerToHelixListenerMap = new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param storeConfig The {@link HelixPropertyStoreConfig} that specifies necessary configs to start a HelixNotifier.
   * @param storeFactory The factory to start a {@link HelixPropertyStore}.
   */
  public HelixNotifier(HelixPropertyStoreConfig storeConfig, HelixPropertyStoreFactory<ZNRecord> storeFactory) {
    if (storeFactory == null) {
      throw new IllegalArgumentException("storeFactory cannot be null.");
    }
    List<String> subscribedPaths = new ArrayList<>();
    subscribedPaths.add(storeConfig.rootPath + TOPIC_PATH);
    helixStore = storeFactory.getHelixPropertyStore(storeConfig, subscribedPaths);
    logger.info("HelixNotifier started, topicPath={}", storeConfig.rootPath + TOPIC_PATH);
  }

  /**
   * {@inheritDoc}
   *
   * Return {@code true} does not guarantee all the {@link TopicListener} will receive the message. It just indicates
   * the message has been successfully sent out.
   */
  @Override
  public boolean sendMessage(String topic, String message) {
    if (topic == null) {
      throw new IllegalArgumentException("topic cannot be null");
    }
    if (message == null) {
      throw new IllegalArgumentException("message cannot be null");
    }
    String topicPath = makeTopicPath(topic);
    ZNRecord record = new ZNRecord(topicPath);
    record.setSimpleField(MESSAGE_KEY, message);
    boolean res = helixStore.set(topicPath, record, AccessOption.PERSISTENT);
    logger.trace("message={} has been sent to topic={}", message, topic);
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void subscribe(String topic, TopicListener<String> topicListener) {
    if (topic == null) {
      throw new IllegalArgumentException("path to listen cannot be null");
    }
    if (topicListener == null) {
      throw new IllegalArgumentException("listener cannot be null");
    }
    String topicPath = makeTopicPath(topic);
    HelixPropertyListener helixListener = new HelixPropertyListener() {
      @Override
      public void onDataChange(String path) {
        logger.trace("Message is changed for topic {} at path {}", topic, path);
        sendMessageToLocalTopicListener(topicListener, topic, path);
      }

      @Override
      public void onDataCreate(String path) {
        logger.trace("Message is created for topic {} at path {}", topic, path);
        sendMessageToLocalTopicListener(topicListener, topic, path);
      }

      @Override
      public void onDataDelete(String path) {
        // for now, this is a no-op when a ZNRecord for a topic is deleted, since no
        // topic deletion is currently supported.
        logger.debug("Message is deleted for topic {} at path {}", topic, path);
      }
    };
    topicListenerToHelixListenerMap.put(topicListener, helixListener);
    helixStore.subscribe(topicPath, helixListener);
    logger.trace("TopicListener={} has been subscribed to topic={}", topicListener, topic);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unsubscribe(String topic, TopicListener<String> topicListener) {
    if (topic == null) {
      throw new IllegalArgumentException("topic cannot be null");
    }
    if (topicListener == null) {
      throw new IllegalArgumentException("topicListener cannot be null");
    }
    String topicPath = makeTopicPath(topic);
    HelixPropertyListener helixListener = topicListenerToHelixListenerMap.remove(topicListener);
    helixStore.unsubscribe(topicPath, helixListener);
    logger.trace("TopicListener={} has been unsubscribed from topic={}", topicListener, topic);
  }

  /**
   * Sends a message to local {@link TopicListener} that subscribed topics through this {@code HelixNotifier}.
   * @param topicListener The {@link TopicListener} to send the message.
   * @param topic The topic which the message belongs to.
   * @param path The path to the topic.
   */
  private void sendMessageToLocalTopicListener(TopicListener topicListener, String topic, String path) {
    String message = null;
    try {
      ZNRecord zNRecord = helixStore.get(path, null, AccessOption.PERSISTENT);
      if (zNRecord != null) {
        message = zNRecord.getSimpleField(MESSAGE_KEY);
        long startTimeMs = System.currentTimeMillis();
        topicListener.onMessageReceive(topic, message);
        logger.trace("Message has been sent to TopicListener={} on topic={} with message={} using {}ms", topicListener,
            topic, message, System.currentTimeMillis() - startTimeMs);
      } else {
        logger.debug("TopicListener={} on topic={} should receive a message, but the corresponding ZNRecord is null",
            topicListener, topic);
      }
    } catch (Exception e) {
      logger.warn("Failed to send message to TopicListener={} for topic={} with message={}", topicListener, topic,
          message, e);
    }
  }

  /**
   * Makes {@link HelixPropertyStore} path for a topic.
   * @param topic The topic to make path
   * @return A path in the {@link HelixPropertyStore} for the topic.
   */
  private String makeTopicPath(String topic) {
    return TOPIC_PATH + "/" + topic;
  }
}
