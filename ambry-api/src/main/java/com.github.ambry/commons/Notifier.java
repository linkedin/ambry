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

/**
 * A {@code Notifier} is a hub that dispatches messages of various topics to {@link TopicListener}s.
 * After a {@link TopicListener} has subscribed to a topic through the {@code Notifier}, a message can
 * be published for that topic, and will be received by the {@link TopicListener}. A topic can be
 * subscribed by multiple {@link TopicListener}, and a {@link TopicListener} can subscribe multiple topics.
 *
 * @param <T> The type of message.
 */
public interface Notifier<T> {

  /**
   * Publishes a message for the specified topic. The {@link TopicListener}s that have subscribed to the
   * topic will receive the message.
   * @param topic The topic the message is sent for.
   * @param message The message to send for the topic.
   * @return {@code true} if the message has been sent out successfully, {@code false} otherwise.
   */
  public boolean publish(String topic, T message);

  /**
   * Let a {@link TopicListener} subscribe to a topic. After subscription, it will receive the messages
   * published for the topic.
   * @param topic The topic to subscribe.
   * @param listener The {@link TopicListener} who subscribes the topic.
   */
  public void subscribe(String topic, TopicListener<T> listener);

  /**
   * Let a {@link TopicListener} unsubscribe from a topic, so it will no longer receive the messages for
   * the topic.
   * @param topic The topic to unsubscribe.
   * @param listener The {@link TopicListener} who unsubscribes the topic.
   */
  public void unsubscribe(String topic, TopicListener<T> listener);
}
