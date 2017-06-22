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

import com.github.ambry.commons.Notifier;
import com.github.ambry.commons.TopicListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A mock implementation of {@link Notifier}.
 */
public class MockNotifier implements Notifier<String> {

  private final Map<String, Set<TopicListener<String>>> topicToListenersMap = new HashMap<>();

  @Override
  public boolean publish(String topic, String message) {
    if (topic == null || message == null) {
      throw new IllegalArgumentException("topic or message cannot be null.");
    }
    Set<TopicListener<String>> listeners = topicToListenersMap.get(topic);
    if (listeners != null) {
      for (TopicListener listener : listeners) {
        listener.onMessage(topic, message);
      }
    }
    return true;
  }

  @Override
  public void subscribe(String topic, TopicListener<String> listener) {
    if (topic == null || listener == null) {
      throw new IllegalArgumentException("topic or listener cannot be null.");
    }

    Set<TopicListener<String>> listeners = topicToListenersMap.get(topic);
    if (listeners == null) {
      listeners = new HashSet<>();
    }
    listeners.add(listener);
    topicToListenersMap.put(topic, listeners);
  }

  @Override
  public void unsubscribe(String topic, TopicListener<String> listener) {
    if (topic == null || listener == null) {
      throw new IllegalArgumentException("topic or listener cannot be null.");
    }
    Set<TopicListener<String>> listeners = topicToListenersMap.get(topic);
    if (listeners != null) {
      listeners.remove(listener);
    }
  }
}
