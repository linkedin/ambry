/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import java.io.IOException;
import java.util.List;


/**
 * {@link ReplicaTokenPersistor} is used in {@link ReplicationEngine} to persist replication token.
 */
public interface ReplicaTokenPersistor extends Runnable {
  /**
   * Method to persist the token of partition(s) under the same mountPath.
   * @param mountPath The mouth path of the partition(s).
   * @param shuttingDown indicates whether this is being called as part of shut down.
   */
  void write(String mountPath, boolean shuttingDown) throws IOException, ReplicationException;

  /**
   * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
   * path to a file. The file is saved on the corresponding mount path.
   * @param shuttingDown indicates whether this is being called as part of shut down.
   */
  void write(boolean shuttingDown) throws IOException, ReplicationException;

  /**
   * Read the tokens under the same mountPath.
   * @param mountPath The mouth path of the partition(s).
   * @return A list of {@link ReplicaTokenInfo}.
   */
  List<ReplicaTokenInfo> read(String mountPath) throws IOException, ReplicationException;
}

