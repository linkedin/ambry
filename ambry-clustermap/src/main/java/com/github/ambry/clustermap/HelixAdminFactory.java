/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;


/**
 * A factory class to construct and get a reference to a {@link HelixAdmin}
 */
public class HelixAdminFactory {
  /**
   * Get a reference to a {@link HelixAdmin}
   * @param zkAddr the address identifying the zk service to which this request is to be made.
   * @return the reference to the {@link HelixAdmin}.
   */
  public HelixAdmin getHelixAdmin(String zkAddr) {
    return new ZKHelixAdmin(zkAddr);
  }
}
