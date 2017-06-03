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
 * A {@code TopicListener} can subscribe topics for messages through a {@link Notifier}.
 * @param <T> The type of message the listener will receive.
 */
public interface TopicListener<T> {

  /**
   * After the {@link TopicListener} has subscribed the topic, this method will be called when there is a new message
   * for the topic.
   * @param topic The topic this {@code TopicListener} subscribes.
   * @param message The message for the topic.
   */
  public void onMessage(String topic, T message);
}
