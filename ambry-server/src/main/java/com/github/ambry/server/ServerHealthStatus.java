/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

/**
 * Server Health Status Levels
 * Each level determines the health of the server. This can be extended to include bootstrapping
 */
public enum ServerHealthStatus {
  /**
   * The server is healthy based on the replica state's and disk state
   */
  GOOD,
  /**
   * There was at least one issue with either the replica state or disk state on this server
   */
  BAD,
}

