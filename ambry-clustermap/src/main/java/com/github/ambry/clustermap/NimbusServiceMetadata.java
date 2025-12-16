/*
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.ambry.utils.Utils;

/**
 * Represents metadata from nimbus-service.json file containing service instance information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NimbusServiceMetadata {

  @JsonProperty("appInstanceID")
  private String appInstanceID;

  @JsonProperty("nodeName")
  private String nodeName;

  @JsonProperty("maintenanceZone")
  private String maintenanceZone;

  /**
   * Read nimbus service metadata from the specified file path.
   * @param filePath the path to the nimbus-service.json file
   * @return NimbusServiceMetadata instance, or null if file cannot be read
   */
  public static NimbusServiceMetadata readFromFile(String filePath) {
    return Utils.readJsonFromFile(filePath, NimbusServiceMetadata.class);
  }

  // Getters
  public String getAppInstanceID() {
    return appInstanceID;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getMaintenanceZone() {
    return maintenanceZone;
  }

  @Override
  public String toString() {
    return "NimbusServiceMetadata{" +
        "appInstanceID='" + appInstanceID + '\'' +
        ", nodeName='" + nodeName + '\'' +
        ", maintenanceZone='" + maintenanceZone + '\'' +
        '}';
  }
}
