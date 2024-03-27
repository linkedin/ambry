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
package com.github.ambry.clustermap;

public class NoOpResourceStatePolicy implements ResourceStatePolicy {
  @Override
  public boolean isHardDown() {
    return false;
  }

  @Override
  public boolean isDown() {
    return false;
  }

  @Override
  public void onError() {

  }

  @Override
  public void onSuccess() {

  }

  @Override
  public void onHardDown() {

  }

  @Override
  public void onHardUp() {

  }
}
