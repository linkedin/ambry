/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import java.util.List;


/**
 * The info returned by the store on a get call
 */
public class StoreInfo {
  private final MessageReadSet readSet;
  private final List<MessageInfo> messageSetInfos;

  public StoreInfo(MessageReadSet readSet, List<MessageInfo> messageSetInfos) {
    this.readSet = readSet;
    this.messageSetInfos = messageSetInfos;
  }

  public MessageReadSet getMessageReadSet() {
    return readSet;
  }

  public List<MessageInfo> getMessageReadSetInfo() {
    return messageSetInfos;
  }
}
