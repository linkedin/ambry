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
package com.github.ambry.router;

import com.github.ambry.network.NetworkClient;
import com.github.ambry.store.StoreKey;
import java.util.List;


/**
 * An object of this class is passed by the router to the operation managers for the latter to use to notify the
 * router about events and state changes that need attention.
 */
class RouterCallback {
  private final NetworkClient networkClient;
  private final List<BackgroundDeleteRequest> backgroundDeleteRequests;

  /**
   * Construct a RouterCallback object
   * @param networkClient the {@link NetworkClient} associated with this callback.
   */
  RouterCallback(NetworkClient networkClient, List<BackgroundDeleteRequest> backgroundDeleteRequests) {
    this.networkClient = networkClient;
    this.backgroundDeleteRequests = backgroundDeleteRequests;
  }

  /**
   * Wake up the associated {@link NetworkClient}.
   *
   * Called by the operation managers when a poll-eligible event occurs for any operation. A poll-eligible event is any
   * event that occurs asynchronously to the RequestResponseHandler thread such that there is a high chance of
   * meaningful work getting done when the operation is subsequently polled. When the callback is invoked, the
   * RequestResponseHandler thread which could be sleeping in a {@link NetworkClient#sendAndPoll(List, java.util.Set, int)} is woken up
   * so that the operations can be polled without additional delays. For example, when a chunk gets filled by the
   * ChunkFillerThread within the {@link PutManager}, this callback is invoked so that the RequestResponseHandler
   * immediately polls the operation to send out the request for the chunk.
   */
  void onPollReady() {
    networkClient.wakeup();
  }

  /**
   * Schedule the deletes of ids in the given list.
   * @param idsToDelete the list of ids that need to be deleted.
   * @param serviceIdSuffix the suffix to append to the service ID when deleting these blobs.
   */
  void scheduleDeletes(List<StoreKey> idsToDelete, String serviceIdSuffix) {
    for (StoreKey storeKey : idsToDelete) {
      backgroundDeleteRequests.add(new BackgroundDeleteRequest(storeKey, serviceIdSuffix));
    }
  }
}

