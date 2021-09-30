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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Objects;


/**
 * Class to represent a container's storage stats.
 */
@JsonPropertyOrder({"containerId", "logicalStorageUsage", "physicalStorageUsage", "numberOfBlobs"})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonDeserialize(builder = ContainerStorageStats.Builder.class)
public class ContainerStorageStats {
  private final short containerId;
  private final long logicalStorageUsage;
  private final long numberOfBlobs;
  private final long physicalStorageUsage;

  /**
   * Constructor to instantiate a {@link ContainerStorageStats}.
   * @param containerId The container id.
   * @param logicalStorageUsage The logical storage usage.
   * @param physicalStorageUsage The physical storage usage.
   * @param numberOfBlobs The number of blobs.
   */
  public ContainerStorageStats(short containerId, long logicalStorageUsage, long physicalStorageUsage,
      long numberOfBlobs) {
    this.containerId = containerId;
    this.logicalStorageUsage = logicalStorageUsage;
    this.physicalStorageUsage = physicalStorageUsage;
    this.numberOfBlobs = numberOfBlobs;
  }

  /**
   * Copy constructor
   * @param other The {@link ContainerStorageStats} to copy
   */
  public ContainerStorageStats(ContainerStorageStats other) {
    this(other.containerId, other.logicalStorageUsage, other.physicalStorageUsage, other.numberOfBlobs);
  }

  /**
   * @return The container id
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * @return The number of blobs
   */
  public long getNumberOfBlobs() {
    return numberOfBlobs;
  }

  /**
   * @return The physical storage usage.
   */
  public long getPhysicalStorageUsage() {
    return physicalStorageUsage;
  }

  /**
   * @return The logical storage usage.
   */
  public long getLogicalStorageUsage() {
    return logicalStorageUsage;
  }

  /**
   * True if all the storage stats values are 0.
   * @return True if all the stats values are 0.
   */
  @JsonIgnore
  public boolean isEmpty() {
    return logicalStorageUsage == 0 && physicalStorageUsage == 0 && numberOfBlobs == 0;
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

  /**
   * Add the given {@link ContainerStorageStats} to the current {@link ContainerStorageStats}.
   * @param other The {@link ContainerStorageStats} to add.
   * @return A new {@link ContainerStorageStats}.
   */
  public ContainerStorageStats add(ContainerStorageStats other) {
    Objects.requireNonNull(other, "Parameter can't be null");
    if (other.containerId != containerId) {
      throw new IllegalArgumentException(
          "ContainerId from other stats is different from current one: " + other.containerId + " != " + containerId);
    }
    return new ContainerStorageStats(other.containerId, other.logicalStorageUsage + logicalStorageUsage,
        other.physicalStorageUsage + physicalStorageUsage, other.numberOfBlobs + numberOfBlobs);
  }

  /**
   * The Builder class for {@link ContainerStorageStats}.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private Short containerId = null;
    private long logicalStorageUsage = 0;
    private long physicalStorageUsage = 0;
    private long numberOfBlobs = 0;

    /**
     * Empty constructor for json
     */
    public Builder() {
    }

    /**
     * Constructor to instantiate a new {@link Builder}. The rest of the values are 0 by default.
     * @param containerId The container id.
     */
    public Builder(short containerId) {
      this.containerId = containerId;
    }

    /**
     * Constructor to instantiate a new {@link Builder} with the given {@link ContainerStorageStats}.
     * @param origin The given {@link ContainerStorageStats}.
     */
    public Builder(ContainerStorageStats origin) {
      this.containerId = origin.containerId;
      this.logicalStorageUsage = origin.logicalStorageUsage;
      this.physicalStorageUsage = origin.physicalStorageUsage;
      this.numberOfBlobs = origin.numberOfBlobs;
    }

    /**
     * Set container id for {@link Builder}.
     * @param containerId The container id.
     * @return Current builder
     */
    public Builder containerId(short containerId) {
      this.containerId = containerId;
      return this;
    }

    /**
     * Set logical storage usage for {@link Builder}.
     * @param logicalStorageUsage The logical storage usage.
     * @return Current Builder.
     */
    public Builder logicalStorageUsage(long logicalStorageUsage) {
      this.logicalStorageUsage = logicalStorageUsage;
      return this;
    }

    /**
     * Set physical storage usage for {@link Builder}.
     * @param physicalStorageUsage The physical storage usage.
     * @return Current Builder.
     */
    public Builder physicalStorageUsage(long physicalStorageUsage) {
      this.physicalStorageUsage = physicalStorageUsage;
      return this;
    }

    /**
     * Set number of blobs for {@link Builder}.
     * @param numberOfBlobs The number of blobs.
     * @return Current Builder.
     */
    public Builder numberOfBlobs(long numberOfBlobs) {
      this.numberOfBlobs = numberOfBlobs;
      return this;
    }

    /**
     * Build a {@link ContainerStorageStats} based on the values in builder.
     * @return A new {@link ContainerStorageStats}.
     */
    public ContainerStorageStats build() {
      if (containerId == null) {
        throw new IllegalStateException("No container is provided");
      }
      return new ContainerStorageStats(containerId, logicalStorageUsage, physicalStorageUsage, numberOfBlobs);
    }
  }
}