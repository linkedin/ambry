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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   A {@link Notifier} implementation using {@link HelixPropertyStore<ZNRecord>} as the underlying communication
 *   mechanism. Each topic is created as a {@link ZNRecord} under the {@link #TOPIC_PATH}. A message for that topic
 *   will be written to the simple field of the {@link ZNRecord} in the form of {@code "message": "message content"}.
 *   A {@link ZNRecord} under the {@link #TOPIC_PATH} will be used for a single topic.
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
  private static final Logger logger = LoggerFactory.getLogger(HelixNotifier.class);
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final ConcurrentHashMap<TopicListener, HelixPropertyListener> topicListenerToHelixListenerMap =
      new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param helixStore A {@link HelixPropertyStore} that will be used by this {@code HelixNotifier}. Cannot be {@code null}.
   */
  HelixNotifier(HelixPropertyStore<ZNRecord> helixStore) {
    if (helixStore == null) {
      throw new IllegalArgumentException("helixStore cannot be null.");
    }
    this.helixStore = helixStore;
  }

  /**
   * A constructor that gets a {@link HelixNotifier} based on {@link HelixPropertyStoreConfig}.
   * @param zkServers the ZooKeeper server address.
   * @param storeConfig A {@link HelixPropertyStore} used to instantiate a {@link HelixNotifier}. Cannot be {@code null}.
   */
  public HelixNotifier(String zkServers, HelixPropertyStoreConfig storeConfig) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("storeConfig cannot be null");
    }
    long startTimeMs = System.currentTimeMillis();
    logger.info("Starting a HelixNotifier");
    List<String> subscribedPaths = Collections.singletonList(storeConfig.rootPath + HelixNotifier.TOPIC_PATH);
    HelixPropertyStore<ZNRecord> helixStore =
        CommonUtils.createHelixPropertyStore(zkServers, storeConfig, subscribedPaths);
    logger.info("HelixPropertyStore started with zkClientConnectString={}, zkClientSessionTimeoutMs={}, "
            + "zkClientConnectionTimeoutMs={}, rootPath={}, subscribedPaths={}", zkServers,
        storeConfig.zkClientSessionTimeoutMs, storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath,
        subscribedPaths);
    this.helixStore = helixStore;
    long startUpTimeInMs = System.currentTimeMillis() - startTimeMs;
    logger.info("HelixNotifier started, took {} ms", startUpTimeInMs);
  }

  /**
   * {@inheritDoc}
   *
   * Returns {@code true} does not guarantee all the {@link TopicListener} will receive the message. It just indicates
   * the message has been successfully sent out.
   */
  @Override
  public boolean publish(String topic, String message) {
    if (topic == null) {
      throw new IllegalArgumentException("topic cannot be null");
    }
    if (message == null) {
      throw new IllegalArgumentException("message cannot be null");
    }
    String topicPath = getTopicPath(topic);
    ZNRecord record = new ZNRecord(topicPath);
    record.setSimpleField(MESSAGE_KEY, message);
    boolean res = helixStore.set(topicPath, record, AccessOption.PERSISTENT);
    if (res) {
      logger.trace("message={} has been published for topic={}", message, topic);
    } else {
      logger.error("failed to publish message={} for topic={}", message, topic);
    }
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
    String topicPath = getTopicPath(topic);
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
        logger.warn("Message is unexpectedly deleted for topic {} at path {}", topic, path);
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
    String topicPath = getTopicPath(topic);
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
  private void sendMessageToLocalTopicListener(TopicListener<String> topicListener, String topic, String path) {
    String message = null;
    try {
      ZNRecord zNRecord = helixStore.get(path, null, AccessOption.PERSISTENT);
      if (zNRecord != null) {
        message = zNRecord.getSimpleField(MESSAGE_KEY);
        long startTimeMs = System.currentTimeMillis();
        topicListener.onMessage(topic, message);
        logger.trace("Message has been sent to TopicListener={} on topic={} with message={} using {}ms", topicListener,
            topic, message, System.currentTimeMillis() - startTimeMs);
      } else {
        logger.debug("TopicListener={} on topic={} should receive a message, but the corresponding ZNRecord is null",
            topicListener, topic);
      }
    } catch (Exception e) {
      logger.error("Failed to send message to TopicListener={} for topic={} with message={}", topicListener, topic,
          message, e);
    }
  }

  /**
   * Gets {@link HelixPropertyStore} path for a topic.
   * @param topic The topic to get its path
   * @return A path in the {@link HelixPropertyStore} for the topic.
   */
  private String getTopicPath(String topic) {
    return TOPIC_PATH + "/" + topic;
  }
}
