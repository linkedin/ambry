/**
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
package com.github.ambry.frontend;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;


/**
 * Defines all the operations that are expressed as a part of the URI
 */
public class Operations {
  public static final String GET_PEERS = "peers";
  public static final String GET_SIGNED_URL = "signedUrl";
  public static final String UPDATE_TTL = "updateTtl";
  public static final String STITCH = "stitch";
  public static final String GET_CLUSTER_MAP_SNAPSHOT = "getClusterMapSnapshot";
  public static final Set<String> OperationsSet;

  static {
    OperationsSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    OperationsSet.addAll(Arrays.asList(GET_PEERS, GET_SIGNED_URL, UPDATE_TTL, STITCH, GET_CLUSTER_MAP_SNAPSHOT));
  }
}
