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
package com.github.ambry.replication.continuous;

/**
 * This class tracks for a current state for StandBy groups for a continuous replication cycle.
 * Standby group has  replicas with STANDBY_TIMED_OUT state only
 */
public class StandByGroupTracker extends GroupTracker {

  public StandByGroupTracker(int groupId) {
    super(groupId);
  }
}
