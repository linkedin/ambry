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
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents metadata from liStatefulSet.json file containing Kubernetes StatefulSet information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LiStatefulSetMetadata {
  private static final Logger logger = LoggerFactory.getLogger(LiStatefulSetMetadata.class);

  @JsonProperty("metadata")
  private Metadata metadata;

  /**
   * Read LiStatefulSet metadata from the specified file path.
   * @param filePath the path to the liStatefulSet.json file
   * @return LiStatefulSetMetadata instance, or null if file cannot be read
   */
  public static LiStatefulSetMetadata readFromFile(String filePath) {
    return Utils.readJsonFromFile(filePath, LiStatefulSetMetadata.class);
  }

  /**
   * Get the StatefulSet name from metadata.
   * @return the name, or null if not available
   */
  public String getName() {
    return metadata != null ? metadata.name : null;
  }

  /**
   * Extract resource tags from the StatefulSet name.
   * Expected format: "v1.ambry-prod.{resourceTag}" or "v1.ambry-prod.{start}-{end}" for ranges
   * Examples:
   * - "v1.ambry-video.10032" -> ["10032"]
   * - "v1.ambry-video.10032-10033" -> ["10032", "10033"]
   * @return list of resource tags, empty if not found
   */
  public List<String> getResourceTags() {
    String name = getName();
    if (name != null && name.contains(".")) {
      String[] parts = name.split("\\.");
      if (parts.length == 3) {
        String resourcePart = parts[parts.length - 1]; // Get the last part
        return parseResourceTags(resourcePart);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Parse resource tags from the resource part, handling ranges.
   * @param resourcePart the resource part (e.g., "10032" or "10032-10033")
   * @return list of resource tags
   */
  private List<String> parseResourceTags(String resourcePart) {
    List<String> tags = new ArrayList<>();
    if (resourcePart == null || resourcePart.trim().isEmpty()) {
      return tags;
    }
    resourcePart = resourcePart.trim();

    // Check if it's a range (contains hyphen and both parts are numeric)
    if (resourcePart.contains("-")) {
      String[] rangeParts = resourcePart.split("-");
      if (rangeParts.length == 2) {
        String startStr = rangeParts[0].trim();
        String endStr = rangeParts[1].trim();
        try {
          int start = Integer.parseInt(startStr);
          int end = Integer.parseInt(endStr);
          // Validate range
          if (start <= end) {
            for (int i = start; i <= end; i++) {
              tags.add(String.valueOf(i));
            }
            return tags;
          } else {
            logger.warn("Invalid range in resource part: {} (start={}, end={})", resourcePart, start, end);
          }
        } catch (NumberFormatException e) {
          logger.warn("Non-numeric range in resource part: {}", resourcePart);
        }
      }
    }

    // If not a valid range, treat as single tag
    tags.add(resourcePart);
    return tags;
  }

  // Getter
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "LiStatefulSetMetadata{" +
        "name='" + getName() + '\'' +
        ", resourceTags=" + getResourceTags() +
        '}';
  }

  // Inner class for JSON structure
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Metadata {
    @JsonProperty("name")
    public String name;
  }
}
