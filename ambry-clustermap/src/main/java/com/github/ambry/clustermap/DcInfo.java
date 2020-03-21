/*
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
 *
 */

package com.github.ambry.clustermap;

import java.io.Closeable;


/**
 * Class that stores all the information associated with a datacenter.
 */
class DcInfo implements Closeable {
  final String dcName;
  final ClusterMapUtils.DcZkInfo dcZkInfo;
  final ClusterChangeHandler clusterChangeHandler;

  /**
   * Construct a DcInfo object with the given parameters.
   * @param dcName the associated datacenter name.
   * @param dcZkInfo the {@link ClusterMapUtils.DcZkInfo} associated with the DC.
   * @param clusterChangeHandler the associated {@link ClusterChangeHandler} for this datacenter.
   */
  DcInfo(String dcName, ClusterMapUtils.DcZkInfo dcZkInfo, ClusterChangeHandler clusterChangeHandler) {
    this.dcName = dcName;
    this.dcZkInfo = dcZkInfo;
    this.clusterChangeHandler = clusterChangeHandler;
  }

  @Override
  public void close() {
  }
}
