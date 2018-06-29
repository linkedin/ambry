/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import java.io.InputStream;


/**
 * Representation of a message in the store. Contains the {@link MessageInfo} and the message bytes.
 */
public class Message {
  private final MessageInfo messageInfo;
  private final InputStream msgStream;

  /**
   * @param messageInfo the {@link MessageInfo} for this message.
   * @param msgStream the {@link InputStream} contains the data of this message.
   */
  public Message(MessageInfo messageInfo, InputStream msgStream) {
    this.messageInfo = messageInfo;
    this.msgStream = msgStream;
  }

  /**
   * @return the {@link MessageInfo} for this message.
   */
  public MessageInfo getMessageInfo() {
    return messageInfo;
  }

  /**
   * @return the {@link InputStream} that contains the data of this message.
   */
  public InputStream getStream() {
    return msgStream;
  }
}

