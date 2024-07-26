/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import java.util.List;


public class StoreBatchDeleteInfo {
  private final PartitionId partitionId;

  private final List<MessageErrorInfo> messageErrorInfos;

  public StoreBatchDeleteInfo(PartitionId partitionId, List<MessageErrorInfo> messageErrorInfos) {
    this.partitionId = partitionId;
    this.messageErrorInfos = messageErrorInfos;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<MessageErrorInfo> getMessageErrorInfos() {
    return messageErrorInfos;
  }

  public void addMessageErrorInfo(MessageErrorInfo messageErrorInfo){
    this.messageErrorInfos.add(messageErrorInfo);
  }
}
