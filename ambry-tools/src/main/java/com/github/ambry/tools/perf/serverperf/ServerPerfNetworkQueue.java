package com.github.ambry.tools.perf.serverperf;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;


public class ServerPerfNetworkQueue extends Thread {
  Http2NetworkClient networkClient;

  List<Http2NetworkClient> networkClients;

  int clientIndex;
  ConcurrentLinkedQueue<RequestInfo> requestInfos;

  ConcurrentLinkedQueue<ResponseInfo> responseInfos;

  DataNodeId dataNodeId;
  int pollTimeout;

  Semaphore maxParallelRequest;
  ExecutorService executorService;

  ServerPerfNetworkQueue(VerifiableProperties verifiableProperties, ClusterMap clusterMap, Time time,
      int maxParallelism, DataNodeId dataNodeId) throws Exception {
    SSLFactory sslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    Http2ClientMetrics metrics = new Http2ClientMetrics(clusterMap.getMetricRegistry());
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(verifiableProperties);
    Http2NetworkClientFactory networkClientFactory =
        new Http2NetworkClientFactory(metrics, http2ClientConfig, sslFactory, time);
    networkClient = networkClientFactory.getNetworkClient();
    networkClients = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      networkClients.add(networkClientFactory.getNetworkClient());
    }
    clientIndex = 0;
    requestInfos = new ConcurrentLinkedQueue<>();
    responseInfos = new ConcurrentLinkedQueue<>();
    pollTimeout = 0;
    maxParallelRequest = new Semaphore(maxParallelism);
    executorService = Executors.newFixedThreadPool(maxParallelism);
    this.dataNodeId = dataNodeId;
  }

  void submit(RequestInfo requestInfo) throws Exception {
    maxParallelRequest.acquire();
    requestInfos.offer(requestInfo);
  }

  void poll(ResponseInfoProcessor responseInfoProcessor) throws Exception {
    ResponseInfo responseInfo = responseInfos.poll();
    if (responseInfo == null) {
      return;
    }
    executorService.submit(new Thread(() -> {
      try {
        responseInfoProcessor.process(responseInfo);
      } catch (Exception e) {
        //do n
      } finally {
        maxParallelRequest.release();
      }
    }));
  }

  @Override
  public void run() {
    while (true) {

      List<ResponseInfo> responseInfos = new ArrayList<>();
      networkClients.forEach(networkClient -> {
        responseInfos.addAll(networkClient.sendAndPoll(Collections.emptyList(), new HashSet<>(), pollTimeout));
      });

      if (!requestInfos.isEmpty()) {
        RequestInfo requestInfo = requestInfos.poll();
        List<ResponseInfo> responses = networkClients.get(clientIndex)
            .sendAndPoll(Collections.singletonList(requestInfo), new HashSet<>(), pollTimeout);
        clientIndex++;
        clientIndex = clientIndex % networkClients.size();
        responseInfos.addAll(responses);
      }

      this.responseInfos.addAll(responseInfos);
    }
  }
}
