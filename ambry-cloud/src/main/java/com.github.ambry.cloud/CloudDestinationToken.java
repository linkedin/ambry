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
package com.github.ambry.cloud;

/**
 * An interface representing the replication token to track replication progress from cloud.
 */
public interface CloudDestinationToken {

  /**
   * Serialize {@link CloudDestinationToken} object to byte array.
   * @return byte array representing this token.
   */
  byte[] toBytes();

  /**
   * Return the version of this token.
   * @return version of the token.
   */
  short getVersion();

  /**
   * Return the size of the token.
   * @return size of the token.
   */
  int size();

  /**
   * Check if the token is equal to the specified {@code otherCloudDestinationToken}.
   * @param otherCloudDestinationToken {@link CloudDestination} object to compare against.
   * @return true if equal. false otherwise.
   */
  boolean equals(CloudDestinationToken otherCloudDestinationToken);

  /**
   * Return a string representation of the object.
   * @return String representation of the object.
   */
  String toString();
}
