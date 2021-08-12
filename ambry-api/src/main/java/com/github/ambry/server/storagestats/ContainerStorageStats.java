/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server.storagestats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Objects;


@JsonPropertyOrder({"containerId", "logicalStorageUsage", "physicalStorageUsage", "numberOfBlobs"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonDeserialize(builder = ContainerStorageStats.Builder.class)
public class ContainerStorageStats {
  private final short containerId;
  private final long logicalStorageUsage;
  private final long numberOfBlobs;
  private final long physicalStorageUsage;

  public ContainerStorageStats(short containerId, long logicalStorageUsage, long physicalStorageUsage,
      long numberOfBlobs) {
    this.containerId = containerId;
    this.logicalStorageUsage = logicalStorageUsage;
    this.physicalStorageUsage = physicalStorageUsage;
    this.numberOfBlobs = numberOfBlobs;
  }

  public short getContainerId() {
    return containerId;
  }

  public long getNumberOfBlobs() {
    return numberOfBlobs;
  }

  public long getPhysicalStorageUsage() {
    return physicalStorageUsage;
  }

  public long getLogicalStorageUsage() {
    return logicalStorageUsage;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerId, logicalStorageUsage, physicalStorageUsage, numberOfBlobs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    ContainerStorageStats other = (ContainerStorageStats) o;
    return containerId == other.containerId && numberOfBlobs == other.numberOfBlobs
        && physicalStorageUsage == other.physicalStorageUsage && logicalStorageUsage == other.logicalStorageUsage;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("ContainerId=")
        .append(containerId)
        .append("&numberOfBlobs=")
        .append(numberOfBlobs)
        .append("&logicalStorageUsage=")
        .append(logicalStorageUsage)
        .append("&physicalStorageUsage=")
        .append(physicalStorageUsage);
    return buffer.toString();
  }

  public ContainerStorageStats add(ContainerStorageStats other) {
    if (other.containerId != containerId) {
      throw new IllegalArgumentException(
          "ContainerId from other stats is different from current one: " + other.containerId + " != " + containerId);
    }
    return new ContainerStorageStats(other.containerId, other.logicalStorageUsage + logicalStorageUsage,
        other.physicalStorageUsage + physicalStorageUsage, other.numberOfBlobs + numberOfBlobs);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private Short containerId = null;
    private long logicalStorageUsage = 0;
    private long physicalStorageUsage = 0;
    private long numberOfBlobs = 0;

    public Builder() {
    }

    public Builder(short containerId) {
      this.containerId = containerId;
    }

    public Builder containerId(short containerId) {
      this.containerId = containerId;
      return this;
    }

    public Builder logicalStorageUsage(long logicalStorageUsage) {
      this.logicalStorageUsage = logicalStorageUsage;
      return this;
    }

    public Builder physicalStorageUsage(long physicalStorageUsage) {
      this.physicalStorageUsage = physicalStorageUsage;
      return this;
    }

    public Builder numberOfBlobs(long numberOfBlobs) {
      this.numberOfBlobs = numberOfBlobs;
      return this;
    }

    public ContainerStorageStats build() {
      if (containerId == null) {
        throw new IllegalStateException("No container is provided");
      }
      return new ContainerStorageStats(containerId, logicalStorageUsage, physicalStorageUsage, numberOfBlobs);
    }
  }
}