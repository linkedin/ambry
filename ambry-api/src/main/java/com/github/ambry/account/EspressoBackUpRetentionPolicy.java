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
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EspressoBackUpRetentionPolicy implements DatasetRetentionPolicy {
  public static final String RETENTION_POLICY = "EspressoBackUpRetentionPolicy";
  private static final Logger logger = LoggerFactory.getLogger(EspressoBackUpRetentionPolicy.class);
  private final List<DatasetVersionRecord> allValidDatasetVersions;
  private final int retentionCount;

  public EspressoBackUpRetentionPolicy(List<DatasetVersionRecord> allValidDatasetVersions, int retentionCount,
      Dataset.VersionSchema versionSchema) {
    this.allValidDatasetVersions = allValidDatasetVersions;
    this.retentionCount = retentionCount;
    if (!Dataset.VersionSchema.TIMESTAMP.equals(versionSchema)) {
      throw new IllegalArgumentException(
          "Should use TIMESTAMP version schema when using " + EspressoBackUpRetentionPolicy.RETENTION_POLICY);
    }
  }

  @Override
  public List<DatasetVersionRecord> getDatasetVersionOutOfRetention() {
    long startTimeMs = System.currentTimeMillis();
    Map<String, TreeSet<DatasetVersionRecord>> dateGroups = new TreeMap<>();
    List<DatasetVersionRecord> result = new ArrayList<>();
    if (allValidDatasetVersions.size() <= retentionCount) {
      return result;
    }
    GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    //Parse the timestamp and group by date
    for (DatasetVersionRecord datasetVersionRecord : allValidDatasetVersions) {
      long timestampInMillis = Long.parseLong(datasetVersionRecord.getVersion());
      calendar.setTimeInMillis(timestampInMillis);
      Date date = calendar.getTime();
      SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
      String day = dateOnlyFormat.format(date);
      dateGroups.computeIfAbsent(day,
              k -> new TreeSet<>(Comparator.comparingLong(record -> Long.parseLong(record.getVersion()))))
          .add(datasetVersionRecord);
    }
    //Sorted all the record by the following rules
    // 1. When a date has more than 1 record, delete the record in the date first.
    //    This is try to keep at least one version per day which is not out of retention count.
    // 2. If first condition met, delete the smaller timestamp based version.
    List<DatasetVersionRecord> allVersionsSortedByRetentionRule = new ArrayList<>();
    for (TreeSet<DatasetVersionRecord> datasetVersionPerDate : dateGroups.values()) {
      if (datasetVersionPerDate.size() > 1) {
        allVersionsSortedByRetentionRule.addAll(
            new ArrayList<>(datasetVersionPerDate).subList(0, datasetVersionPerDate.size() - 1));
      }
    }
    for (TreeSet<DatasetVersionRecord> records : dateGroups.values()) {
      if (!records.isEmpty()) {
        allVersionsSortedByRetentionRule.add(new ArrayList<>(records).get(records.size() - 1));
      }
    }
    //Apply the retention policy by returning the first N - retentionCount record for deletion. N is the total record size.
    result.addAll(allVersionsSortedByRetentionRule.subList(0,
        Math.min(allVersionsSortedByRetentionRule.size() - retentionCount, allVersionsSortedByRetentionRule.size())));
    logger.trace("getDatasetVersionOutOfRetention took {}ms", System.currentTimeMillis() - startTimeMs);
    return result;
  }
}
