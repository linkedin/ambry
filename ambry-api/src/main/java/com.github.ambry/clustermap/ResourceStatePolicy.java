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
package com.github.ambry.clustermap;

/** ResourceStatePolicy is used to determine if the state of a resource is "up" or "down". For resources like data nodes
 *  and disks, up and down mean available and unavailable, respectively.
 */
public interface ResourceStatePolicy {
  /**
   * Checks to see if the state is permanently down.
   *
   * @return true if the state is permanently down, false otherwise.
   */
  boolean isHardDown();

  /**
   * Checks to see if the state is down (soft or hard).
   *
   * @return true if the state is down, false otherwise.
   */
  boolean isDown();

  /**
   * Should be called by the caller every time an error is encountered for the corresponding resource.
   */
  void onError();

  /**
   * May be called by the caller when the resource is responsive.
   */
  void onSuccess();

  /**
   * Should be called if the caller knows outside of the policy that the resource has gone down.
   */
  void onHardDown();

  /**
   * Should be called if the caller knows outside of the policy that the resource is up.
   */
  void onHardUp();
}

