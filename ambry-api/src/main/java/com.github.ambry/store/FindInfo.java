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

import com.github.ambry.replication.FindToken;
import java.util.List;


/**
 * Contains the information from the store after a find operation. It consist of message info entries and
 * new find token that can be used for subsequent searches.
 */
public class FindInfo {
  private final List<MessageInfo> messageEntries;
  private final FindToken findToken;

  public FindInfo(List<MessageInfo> messageEntries, FindToken findToken) {
    this.messageEntries = messageEntries;
    this.findToken = findToken;
  }

  public List<MessageInfo> getMessageEntries() {
    return messageEntries;
  }

  public FindToken getFindToken() {
    return findToken;
  }
}
