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
package com.github.ambry.account;

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.ArrayList;
import java.util.Collections;
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
 *   If multiple {@link HelixNotifier} configured with the same {@link HelixPropertyStoreConfig}, they will essentially
 *   share the same topic domain. I.e., {@link TopicListener}s can subscribe through any of these {@link HelixNotifier}s
 *   for a topic. A message of that topic sent through one {@link HelixNotifier} can be received by all the
 *   {@link TopicListener}s.
 * </p>
 */
public class HelixNotifier implements Notifier<String> {
  public static final String MESSAGE_KEY = "message";

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String topicPath;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final Map<TopicListener, HelixPropertyListener> topicListenerToHelixListenerMap = new ConcurrentHashMap<>();
  private final Map<String, List<TopicListener>> topicToListenerMap = new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param helixConfig The {@link HelixPropertyStoreConfig} that specifies necessary configs to start a HelixMessenger.
   * @param helixPropertyStoreFactory The factory to start a {@link HelixPropertyStore}.
   */
  public HelixNotifier(HelixPropertyStoreConfig helixConfig,
      HelixPropertyStoreFactory<ZNRecord> helixPropertyStoreFactory) {
    this(helixConfig.rootPath, helixConfig.topicPath, helixConfig.zkClientConnectString,
        helixConfig.zkClientSessionTimeoutMs, helixConfig.zkClientConnectionTimeoutMs, helixPropertyStoreFactory);
  }

  /**
   * Constructor.
   * @param rootPath The root path of the {@link HelixPropertyStore}.
   * @param topicPath The path under which topic are created as {@link ZNRecord}s.
   * @param zkClientConnectString The string to connect to a ZooKeeper server.
   * @param zkClientSessionTimeoutMs The timeout in millisecond within which reconnection to the ZooKeeper server
   *                                 will be considered as the same session.
   * @param zkClientConnectionTimeoutMs The timeout in millisecond to establish a connection to the Zookeeper server.
   * @param storeFactory The factory to start a {@link HelixPropertyStore}.
   */
  public HelixNotifier(String rootPath, String topicPath, String zkClientConnectString, int zkClientSessionTimeoutMs,
      int zkClientConnectionTimeoutMs, HelixPropertyStoreFactory<ZNRecord> storeFactory) {
    if (storeFactory == null) {
      throw new IllegalArgumentException("storeFactory cannot be null.");
    }
    this.topicPath = topicPath;
    List<String> subscribedPaths = new ArrayList<>();
    subscribedPaths.add(rootPath + topicPath);
    this.helixStore =
        storeFactory.getHelixPropertyStore(zkClientConnectString, zkClientSessionTimeoutMs, zkClientConnectionTimeoutMs,
            rootPath, subscribedPaths);
  }

  /**
   * {@inheritDoc}
   *
   * Return {@code true} does not guarantee all the {@link TopicListener} will receive the message.
   */
  @Override
  public boolean sendMessage(String topic, String message) {
    if (topic == null) {
      throw new IllegalArgumentException("path to listen cannot be null");
    }
    if (message == null) {
      throw new IllegalArgumentException("message cannot be null");
    }
    String topicPath = makeTopicPath(topic);
    ZNRecord record = new ZNRecord(topicPath);
    record.setSimpleField(MESSAGE_KEY, message);
    return helixStore.set(topicPath, record, AccessOption.PERSISTENT);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void subscribe(String topic, final TopicListener<String> topicListener) {
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
        sendMessage(topicListener, topic, path);
      }

      @Override
      public void onDataCreate(String path) {
        logger.trace("Message is created for topic {} at path {}", topic, path);
        sendMessage(topicListener, topic, path);
      }

      @Override
      public void onDataDelete(String path) {
        // for now, this is a no-op when a ZNRecord for a topic is deleted, since no
        // topic deletion is currently supported.
        logger.trace("Message is deleted for topic {} at path {}", topic, path);
      }
    };
    topicListenerToHelixListenerMap.put(topicListener, helixListener);
    List<TopicListener> topicListeners = topicToListenerMap.get(topic);
    if (topicListeners == null) {
      topicListeners = new ArrayList<>();
    }
    topicListeners.add(topicListener);
    topicToListenerMap.put(topic, topicListeners);
    helixStore.subscribe(topicPath, helixListener);
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
    List<TopicListener> topicListeners = topicToListenerMap.get(topic);
    if (topicListeners != null) {
      topicListeners.remove(topicListener);
    }
  }

  /**
   * Gets all the {@link TopicListener}s for the specified topic.
   * @param topic The topic to get its listeners.
   * @return A list of {@link TopicListener}s that have subscribed to the topic.
   */
  public List<TopicListener> getListenersByTopic(String topic) {
    if (topic == null || topicToListenerMap.get(topic) == null) {
      return new ArrayList<>();
    }
    return Collections.unmodifiableList(topicToListenerMap.get(topic));
  }

  /**
   * Sends message to {@link TopicListener}.
   * @param topicListener The {@link TopicListener} to send the message.
   * @param topic The topic which the message belongs to.
   * @param path The path to the topic.
   */
  private void sendMessage(TopicListener topicListener, String topic, String path) {
    String message = null;
    try {
      ZNRecord zNRecord = helixStore.get(path, null, AccessOption.PERSISTENT);
      if (zNRecord != null) {
        message = zNRecord.getSimpleField(MESSAGE_KEY);
      }
      topicListener.processMessage(topic, message);
    } catch (Exception e) {
      logger.error("Failed to send message to TopicListener {} for topic {} with message {}", topicListener.getClass(),
          path, message, e);
    }
  }

  /**
   * Makes {@link HelixPropertyStore} path for a topic.
   * @param topic The topic to make path
   * @return A path in the {@link HelixPropertyStore} for the topic.
   */
  private String makeTopicPath(String topic) {
    return topicPath + "/" + topic;
  }
}
