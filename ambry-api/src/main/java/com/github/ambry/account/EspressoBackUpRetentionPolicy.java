/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EspressoBackUpRetentionPolicy implements DatasetRetentionPolicy {
  public static final String RETENTION_POLICY = "EspressoBackUpRetentionPolicy";
  private static final Logger logger = LoggerFactory.getLogger(EspressoBackUpRetentionPolicy.class);
  private final List<DatasetVersionRecord> allValidDatasetVersionsOrderedByVersion;
  private final int retentionCount;

  /**
   * Constructor of the {@link EspressoBackUpRetentionPolicy}
   * @param allValidDatasetVersionsOrderedByVersion a list of all valid versions ordered by version.
   * @param retentionCount the {@link Dataset} level retention count.
   * @param versionSchema the {@link Dataset} level version schema.
   */
  public EspressoBackUpRetentionPolicy(List<DatasetVersionRecord> allValidDatasetVersionsOrderedByVersion,
      int retentionCount, Dataset.VersionSchema versionSchema) {
    this.allValidDatasetVersionsOrderedByVersion = allValidDatasetVersionsOrderedByVersion;
    this.retentionCount = retentionCount;
    if (!Dataset.VersionSchema.TIMESTAMP.equals(versionSchema)) {
      throw new IllegalArgumentException(
          "Should use TIMESTAMP version schema when using " + EspressoBackUpRetentionPolicy.RETENTION_POLICY);
    }
  }

  @Override
  public List<DatasetVersionRecord> getDatasetVersionOutOfRetention() {
    long startTimeMs = System.currentTimeMillis();
    List<DatasetVersionRecord> result = new ArrayList<>();
    if (allValidDatasetVersionsOrderedByVersion.size() <= retentionCount) {
      return result;
    }
    Map<String, List<DatasetVersionRecord>> dateGroups = new TreeMap<>();
    GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    //Parse the timestamp and group by date
    for (DatasetVersionRecord datasetVersionRecord : allValidDatasetVersionsOrderedByVersion) {
      long timestampInMillis = Long.parseLong(datasetVersionRecord.getVersion());
      calendar.setTimeInMillis(timestampInMillis);
      Date date = calendar.getTime();
      SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
      String day = dateOnlyFormat.format(date);
      dateGroups.computeIfAbsent(day, k -> new ArrayList<>()).add(datasetVersionRecord);
    }
    int count = allValidDatasetVersionsOrderedByVersion.size() - retentionCount;
    for (List<DatasetVersionRecord> versionsPerDate : dateGroups.values()) {
      while (versionsPerDate.size() > 1 && count > 0) {
        result.add(versionsPerDate.get(0));
        versionsPerDate.remove(0);
        count--;
      }
      if (count == 0) {
        break;
      }
    }
    //we only have 1 version for each date after above for loop
    while (!dateGroups.isEmpty() && count > 0) {
      Iterator<String> iterator = dateGroups.keySet().iterator();
      List<DatasetVersionRecord> versionsPerDate = dateGroups.get(iterator.next());
      result.add(versionsPerDate.get(0));
      iterator.remove();
      count--;
    }
    logger.trace("getDatasetVersionOutOfRetention took {}ms", System.currentTimeMillis() - startTimeMs);
    return result;
  }
}
