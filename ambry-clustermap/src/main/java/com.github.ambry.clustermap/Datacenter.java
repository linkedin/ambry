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

import com.github.ambry.config.ClusterMapConfig;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * A Datacenter in an Ambry cluster. A Datacenter must be uniquely identifiable by its name. A Datacenter is the primary
 * unit at which Ambry hardware is organized (see {@link HardwareLayout})). A Datacenter has zero or more {@link
 * DataNode}s.
 */
public class Datacenter {
  private final HardwareLayout hardwareLayout;
  private final String name;
  private final ArrayList<DataNode> dataNodes;
  private final long rawCapacityInBytes;
  private boolean rackAware = false;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Datacenter(HardwareLayout hardwareLayout, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("Datacenter " + jsonObject.toString());
    }
    this.hardwareLayout = hardwareLayout;
    this.name = jsonObject.getString("name");

    this.dataNodes = new ArrayList<DataNode>(jsonObject.getJSONArray("dataNodes").length());
    for (int i = 0; i < jsonObject.getJSONArray("dataNodes").length(); ++i) {
      this.dataNodes.add(new DataNode(this, jsonObject.getJSONArray("dataNodes").getJSONObject(i), clusterMapConfig));
    }
    this.rawCapacityInBytes = calculateRawCapacityInBytes();
    validate();
  }

  public HardwareLayout getHardwareLayout() {
    return hardwareLayout;
  }

  public String getName() {
    return name;
  }

  public long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (DataNode dataNode : dataNodes) {
      capacityInBytes += dataNode.getRawCapacityInBytes();
    }
    return capacityInBytes;
  }

  public List<DataNode> getDataNodes() {
    return dataNodes;
  }

  /**
   * Returns {@code true} if all nodes in the datacenter have rack IDs
   * @return {@code true} if all nodes in the datacenter have rack IDs, {@code false} otherwise
   */
  public boolean isRackAware() {
    return rackAware;
  }

  protected void validateHardwareLayout() {
    if (hardwareLayout == null) {
      throw new IllegalStateException("HardwareLayout cannot be null");
    }
  }

  protected void validateName() {
    if (name == null) {
      throw new IllegalStateException("Datacenter name cannot be null.");
    } else if (name.length() == 0) {
      throw new IllegalStateException("Datacenter name cannot be zero length.");
    }
  }

  /**
   * A datacenter can be marked as rack-aware if all nodes have defined rack IDs. This method throws an exception
   * if some nodes have rack IDs and some do not.  It also sets the {@code rackAware} flag to {@code true} if all
   * nodes have rack IDs.
   *
   * @throws IllegalStateException if some nodes have defined rack IDs and some do not.
   */
  private void validateRackAwareness() {
    if (dataNodes.size() > 0) {
      Iterator<DataNode> dataNodeIter = dataNodes.iterator();
      boolean hasRackId = (dataNodeIter.next().getRackId() >= 0);
      while (dataNodeIter.hasNext()) {
        if (hasRackId != (dataNodeIter.next().getRackId() >= 0)) {
          throw new IllegalStateException("dataNodes in datacenter: " + name
              + " must all have defined rack IDs or none at all");
        }
      }
      this.rackAware = hasRackId;
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateHardwareLayout();
    validateName();
    validateRackAwareness();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject()
      throws JSONException {
    JSONObject jsonObject = new JSONObject().put("name", name).put("dataNodes", new JSONArray());
    for (DataNode dataNode : dataNodes) {
      jsonObject.accumulate("dataNodes", dataNode.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "Datacenter[" + getName() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Datacenter that = (Datacenter) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
