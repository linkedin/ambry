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
package com.github.ambry.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Metrics for Replication
 */
public class ReplicationMetrics {

  public final Meter interColoReplicationBytesRate;
  public final Meter intraColoReplicationBytesRate;
  public final Meter plainTextInterColoReplicationBytesRate;
  public final Meter plainTextIntraColoReplicationBytesRate;
  public final Meter sslInterColoReplicationBytesRate;
  public final Meter sslIntraColoReplicationBytesRate;
  public final Counter interColoMetadataExchangeCount;
  public final Counter intraColoMetadataExchangeCount;
  public final Counter interColoBlobsReplicatedCount;
  public final Counter intraColoBlobsReplicatedCount;
  public final Counter unknownRemoteReplicaRequestCount;
  public final Counter plainTextInterColoMetadataExchangeCount;
  public final Counter plainTextIntraColoMetadataExchangeCount;
  public final Counter plainTextInterColoBlobsReplicatedCount;
  public final Counter plainTextIntraColoBlobsReplicatedCount;
  public final Counter sslInterColoMetadataExchangeCount;
  public final Counter sslIntraColoMetadataExchangeCount;
  public final Counter sslInterColoBlobsReplicatedCount;
  public final Counter sslIntraColoBlobsReplicatedCount;
  public final Counter replicationErrors;
  public final Counter plainTextReplicationErrors;
  public final Counter sslReplicationErrors;
  public final Counter replicationTokenResetCount;
  public final Counter replicationInvalidMessageStreamErrorCount;
  public final Timer interColoReplicationLatency;
  public final Timer intraColoReplicationLatency;
  public final Timer plainTextInterColoReplicationLatency;
  public final Timer plainTextIntraColoReplicationLatency;
  public final Timer sslInterColoReplicationLatency;
  public final Timer sslIntraColoReplicationLatency;
  public final Histogram remoteReplicaTokensPersistTime;
  public final Histogram remoteReplicaTokensRestoreTime;
  public final Histogram interColoExchangeMetadataTime;
  public final Histogram intraColoExchangeMetadataTime;
  public final Histogram plainTextInterColoExchangeMetadataTime;
  public final Histogram plainTextIntraColoExchangeMetadataTime;
  public final Histogram sslInterColoExchangeMetadataTime;
  public final Histogram sslIntraColoExchangeMetadataTime;
  public final Histogram interColoFixMissingKeysTime;
  public final Histogram intraColoFixMissingKeysTime;
  public final Histogram plainTextInterColoFixMissingKeysTime;
  public final Histogram plainTextIntraColoFixMissingKeysTime;
  public final Histogram sslInterColoFixMissingKeysTime;
  public final Histogram sslIntraColoFixMissingKeysTime;
  public final Histogram interColoReplicationMetadataRequestTime;
  public final Histogram intraColoReplicationMetadataRequestTime;
  public final Histogram plainTextInterColoReplicationMetadataRequestTime;
  public final Histogram plainTextIntraColoReplicationMetadataRequestTime;
  public final Histogram sslInterColoReplicationMetadataRequestTime;
  public final Histogram sslIntraColoReplicationMetadataRequestTime;
  public final Histogram interColoReplicationWaitTime;
  public final Histogram intraColoReplicationWaitTime;
  public final Histogram interColoCheckMissingKeysTime;
  public final Histogram intraColoCheckMissingKeysTime;
  public final Histogram interColoProcessMetadataResponseTime;
  public final Histogram intraColoProcessMetadataResponseTime;
  public final Histogram interColoGetRequestTime;
  public final Histogram intraColoGetRequestTime;
  public final Histogram plainTextInterColoGetRequestTime;
  public final Histogram plainTextIntraColoGetRequestTime;
  public final Histogram sslInterColoGetRequestTime;
  public final Histogram sslIntraColoGetRequestTime;
  public final Histogram interColoBatchStoreWriteTime;
  public final Histogram intraColoBatchStoreWriteTime;
  public final Histogram plainTextInterColoBatchStoreWriteTime;
  public final Histogram plainTextIntraColoBatchStoreWriteTime;
  public final Histogram sslInterColoBatchStoreWriteTime;
  public final Histogram sslIntraColoBatchStoreWriteTime;
  public final Histogram interColoTotalReplicationTime;
  public final Histogram intraColoTotalReplicationTime;
  public final Histogram plainTextInterColoTotalReplicationTime;
  public final Histogram plainTextIntraColoTotalReplicationTime;
  public final Histogram sslInterColoTotalReplicationTime;
  public final Histogram sslIntraColoTotalReplicationTime;

  public Gauge<Integer> numberOfIntraDCReplicaThreads;
  public Gauge<Integer> numberOfInterDCReplicaThreads;
  public List<Gauge<Long>> replicaLagInBytes;
  private MetricRegistry registry;
  private Map<String, Counter> metadataRequestErrorMap;
  private Map<String, Counter> getRequestErrorMap;
  private Map<String, Counter> localStoreErrorMap;
  private Map<PartitionId, Counter> partitionIdToInvalidMessageStreamErrorCounter;

  public ReplicationMetrics(MetricRegistry registry, final List<ReplicaThread> replicaIntraDCThreads,
      final List<ReplicaThread> replicaInterDCThreads, List<ReplicaId> replicaIds) {
    metadataRequestErrorMap = new HashMap<String, Counter>();
    getRequestErrorMap = new HashMap<String, Counter>();
    localStoreErrorMap = new HashMap<String, Counter>();
    partitionIdToInvalidMessageStreamErrorCounter = new HashMap<PartitionId, Counter>();
    interColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationBytesRate"));
    intraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationBytesRate"));
    plainTextInterColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoReplicationBytesRate"));
    plainTextIntraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationBytesRate"));
    sslInterColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "SslInterColoReplicationBytesRate"));
    sslIntraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationBytesRate"));
    interColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoMetadataExchangeCount"));
    intraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoMetadataExchangeCount"));
    interColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationBlobsCount"));
    intraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoBlobsReplicatedCount"));
    unknownRemoteReplicaRequestCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "UnknownRemoteReplicaRequestCount"));
    plainTextInterColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoMetadataExchangeCount"));
    plainTextIntraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoMetadataExchangeCount"));
    plainTextInterColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoBlobsReplicatedCount"));
    plainTextIntraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoBlobsReplicatedCount"));
    sslInterColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslInterColoMetadataExchangeCount"));
    sslIntraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoMetadataExchangeCount"));
    sslInterColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslInterColoBlobsReplicatedCount"));
    sslIntraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoBlobsReplicatedCount"));
    replicationErrors = registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationErrors"));
    plainTextReplicationErrors =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextReplicationErrors"));
    sslReplicationErrors = registry.counter(MetricRegistry.name(ReplicaThread.class, "SslReplicationErrors"));
    replicationTokenResetCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationTokenResetCount"));
    replicationInvalidMessageStreamErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationInvalidMessageStreamErrorCount"));
    interColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationLatency"));
    intraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationLatency"));
    plainTextInterColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoReplicationLatency"));
    plainTextIntraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationLatency"));
    sslInterColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "SslInterColoReplicationLatency"));
    sslIntraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationLatency"));
    remoteReplicaTokensPersistTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensPersistTime"));
    remoteReplicaTokensRestoreTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensRestoreTime"));
    interColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoExchangeMetadataTime"));
    intraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoExchangeMetadataTime"));
    plainTextInterColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoExchangeMetadataTime"));
    plainTextIntraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoExchangeMetadataTime"));
    sslInterColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoExchangeMetadataTime"));
    sslIntraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoExchangeMetadataTime"));
    interColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoFixMissingKeysTime"));
    intraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoFixMissingKeysTime"));
    plainTextInterColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoFixMissingKeysTime"));
    plainTextIntraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoFixMissingKeysTime"));
    sslInterColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoFixMissingKeysTime"));
    sslIntraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoFixMissingKeysTime"));
    interColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationMetadataRequestTime"));
    intraColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationMetadataRequestTime"));
    plainTextInterColoReplicationMetadataRequestTime = registry
        .histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoReplicationMetadataRequestTime"));
    plainTextIntraColoReplicationMetadataRequestTime = registry
        .histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationMetadataRequestTime"));
    sslInterColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoReplicationMetadataRequestTime"));
    sslIntraColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationMetadataRequestTime"));
    interColoReplicationWaitTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationWaitTime"));
    intraColoReplicationWaitTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationWaitTime"));
    interColoCheckMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoCheckMissingKeysTime"));
    intraColoCheckMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoCheckMissingKeysTime"));
    interColoProcessMetadataResponseTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoProcessMetadataResponseTime"));
    intraColoProcessMetadataResponseTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoProcessMetadataResponseTime"));
    interColoGetRequestTime = registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoGetRequestTime"));
    intraColoGetRequestTime = registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoGetRequestTime"));
    plainTextInterColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoGetRequestTime"));
    plainTextIntraColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoGetRequestTime"));
    sslInterColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoGetRequestTime"));
    sslIntraColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoGetRequestTime"));
    interColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoBatchStoreWriteTime"));
    intraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoBatchStoreWriteTime"));
    plainTextInterColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoBatchStoreWriteTime"));
    plainTextIntraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoBatchStoreWriteTime"));
    sslInterColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoBatchStoreWriteTime"));
    sslIntraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoBatchStoreWriteTime"));
    interColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoTotalReplicationTime"));
    intraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoTotalReplicationTime"));
    plainTextInterColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextInterColoTotalReplicationTime"));
    plainTextIntraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoTotalReplicationTime"));
    sslInterColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInterColoTotalReplicationTime"));
    sslIntraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoTotalReplicationTime"));

    this.registry = registry;
    numberOfIntraDCReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads(replicaIntraDCThreads);
      }
    };
    numberOfInterDCReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads(replicaInterDCThreads);
      }
    };

    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfIntraDCReplicaThreads"),
        numberOfIntraDCReplicaThreads);
    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfInterDCReplicaThreads"),
        numberOfInterDCReplicaThreads);
    this.replicaLagInBytes = new ArrayList<Gauge<Long>>();
    populateInvalidMessageMetricForReplicas(replicaIds);
  }

  private int getLiveThreads(List<ReplicaThread> replicaThreads) {
    int count = 0;
    for (ReplicaThread thread : replicaThreads) {
      if (thread.isThreadUp()) {
        count++;
      }
    }
    return count;
  }

  public void addRemoteReplicaToLagMetrics(final RemoteReplicaInfo remoteReplicaInfo) {
    final String metricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-replicaLagInBytes";
    Gauge<Long> replicaLag = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return remoteReplicaInfo.getReplicaLagInBytes();
      }
    };
    registry.register(MetricRegistry.name(ReplicationMetrics.class, metricName), replicaLag);
    replicaLagInBytes.add(replicaLag);
  }

  public void populateInvalidMessageMetricForReplicas(List<ReplicaId> replicaIds) {
    for (ReplicaId replicaId : replicaIds) {
      PartitionId partitionId = replicaId.getPartitionId();
      if (!partitionIdToInvalidMessageStreamErrorCounter.containsKey(partitionId)) {
        Counter partitionBasedCorruptionErrorCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, partitionId + "-CorruptionErrorCount"));
        partitionIdToInvalidMessageStreamErrorCounter.put(partitionId, partitionBasedCorruptionErrorCount);
      }
    }
  }

  public void incrementInvalidMessageError(PartitionId partitionId) {
    replicationInvalidMessageStreamErrorCount.inc();
    if (partitionIdToInvalidMessageStreamErrorCounter.containsKey(partitionId)) {
      partitionIdToInvalidMessageStreamErrorCounter.get(partitionId).inc();
    }
  }

  public void createRemoteReplicaErrorMetrics(RemoteReplicaInfo remoteReplicaInfo) {
    String metadataRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-metadataRequestError";
    Counter metadataRequestError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, metadataRequestErrorMetricName));
    metadataRequestErrorMap.put(metadataRequestErrorMetricName, metadataRequestError);
    String getRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-getRequestError";
    Counter getRequestError = registry.counter(MetricRegistry.name(ReplicaThread.class, getRequestErrorMetricName));
    getRequestErrorMap.put(getRequestErrorMetricName, getRequestError);
    String localStoreErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-localStoreError";
    Counter localStoreError = registry.counter(MetricRegistry.name(ReplicaThread.class, localStoreErrorMetricName));
    localStoreErrorMap.put(localStoreErrorMetricName, localStoreError);
  }

  public void updateMetadataRequestError(RemoteReplicaInfo remoteReplicaInfo) {
    String metadataRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-metadataRequestError";
    metadataRequestErrorMap.get(metadataRequestErrorMetricName).inc();
  }

  public void updateGetRequestError(RemoteReplicaInfo remoteReplicaInfo) {
    String getRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-getRequestError";
    getRequestErrorMap.get(getRequestErrorMetricName).inc();
  }

  public void updateLocalStoreError(RemoteReplicaInfo remoteReplicaInfo) {
    String localStoreErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-localStoreError";
    localStoreErrorMap.get(localStoreErrorMetricName).inc();
  }

  public void incrementReplicationErrors(boolean sslEnabled) {
    replicationErrors.inc();
    if (sslEnabled) {
      sslReplicationErrors.inc();
    } else {
      plainTextReplicationErrors.inc();
    }
  }

  public void updateTotalReplicationTime(long totalReplicationTime, boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoTotalReplicationTime.update(totalReplicationTime);
      if (sslEnabled) {
        sslInterColoTotalReplicationTime.update(totalReplicationTime);
      } else {
        plainTextInterColoTotalReplicationTime.update(totalReplicationTime);
      }
    } else {
      intraColoTotalReplicationTime.update(totalReplicationTime);
      if (sslEnabled) {
        sslIntraColoTotalReplicationTime.update(totalReplicationTime);
      } else {
        plainTextIntraColoTotalReplicationTime.update(totalReplicationTime);
      }
    }
  }

  public void updateExchangeMetadataTime(long exchangeMetadataTime, boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoMetadataExchangeCount.inc();
      interColoExchangeMetadataTime.update(exchangeMetadataTime);
      if (sslEnabled) {
        sslInterColoMetadataExchangeCount.inc();
        sslInterColoExchangeMetadataTime.update(exchangeMetadataTime);
      } else {
        plainTextInterColoMetadataExchangeCount.inc();
        plainTextInterColoExchangeMetadataTime.update(exchangeMetadataTime);
      }
    } else {
      intraColoMetadataExchangeCount.inc();
      intraColoExchangeMetadataTime.update(exchangeMetadataTime);
      if (sslEnabled) {
        sslIntraColoMetadataExchangeCount.inc();
        sslIntraColoExchangeMetadataTime.update(exchangeMetadataTime);
      } else {
        plainTextIntraColoMetadataExchangeCount.inc();
        plainTextIntraColoExchangeMetadataTime.update(exchangeMetadataTime);
      }
    }
  }

  public void updateFixMissingStoreKeysTime(long fixMissingStoreKeysTime, boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      if (sslEnabled) {
        sslInterColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      } else {
        plainTextInterColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      }
    } else {
      intraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      if (sslEnabled) {
        sslIntraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      } else {
        plainTextIntraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      }
    }
  }

  public void updateMetadataRequestTime(long metadataRequestTime, boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoReplicationMetadataRequestTime.update(metadataRequestTime);
      if (sslEnabled) {
        sslInterColoReplicationMetadataRequestTime.update(metadataRequestTime);
      } else {
        plainTextInterColoReplicationMetadataRequestTime.update(metadataRequestTime);
      }
    } else {
      intraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      if (sslEnabled) {
        sslIntraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      } else {
        plainTextIntraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      }
    }
  }

  public void updateGetRequestTime(long getRequestTime, boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoGetRequestTime.update(getRequestTime);
      if (sslEnabled) {
        sslInterColoGetRequestTime.update(getRequestTime);
      } else {
        plainTextInterColoGetRequestTime.update(getRequestTime);
      }
    } else {
      intraColoGetRequestTime.update(getRequestTime);
      if (sslEnabled) {
        sslIntraColoGetRequestTime.update(getRequestTime);
      } else {
        plainTextIntraColoGetRequestTime.update(getRequestTime);
      }
    }
  }

  public void updateBatchStoreWriteTime(long batchStoreWriteTime, long totalBytesFixed, long totalBlobsFixed,
      boolean remoteColo, boolean sslEnabled) {
    if (remoteColo) {
      interColoReplicationBytesRate.mark(totalBytesFixed);
      interColoBlobsReplicatedCount.inc(totalBlobsFixed);
      interColoBatchStoreWriteTime.update(batchStoreWriteTime);
      if (sslEnabled) {
        sslInterColoReplicationBytesRate.mark(totalBytesFixed);
        sslInterColoBlobsReplicatedCount.inc(totalBlobsFixed);
        sslInterColoBatchStoreWriteTime.update(batchStoreWriteTime);
      } else {
        plainTextInterColoReplicationBytesRate.mark(totalBytesFixed);
        plainTextInterColoBlobsReplicatedCount.inc(totalBlobsFixed);
        plainTextInterColoBatchStoreWriteTime.update(batchStoreWriteTime);
      }
    } else {
      intraColoReplicationBytesRate.mark(totalBytesFixed);
      intraColoBlobsReplicatedCount.inc(totalBlobsFixed);
      intraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      if (sslEnabled) {
        sslIntraColoReplicationBytesRate.mark(totalBytesFixed);
        sslIntraColoBlobsReplicatedCount.inc(totalBlobsFixed);
        sslIntraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      } else {
        plainTextIntraColoReplicationBytesRate.mark(totalBytesFixed);
        plainTextIntraColoBlobsReplicatedCount.inc(totalBlobsFixed);
        plainTextIntraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      }
    }
  }
}
