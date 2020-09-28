/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.github.ambry.account.Container;
import java.util.Set;


/**
 * Class that compacts containers in the Azure cloud by purging blobs of deleted containers from
 * ABS and Cosmos.
 */
public class AzureContainerCompactor {
  private long lastContainerDeletionTimestamp;

  public void updateDeletedContainers(Set<Container> deletedContainers) {
    //TODO update the deleted containers in the cosmos table.
    /*
    Write code here to get the new deleted containers.
    Write code in CosmosDataAccessor to actually add the container to cosmos db table.
     */
  }


}
