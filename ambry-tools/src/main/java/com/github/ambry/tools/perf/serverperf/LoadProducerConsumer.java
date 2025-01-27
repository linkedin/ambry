package com.github.ambry.tools.perf.serverperf;

public interface LoadProducerConsumer {
  void produce() throws Exception;

  void consume() throws Exception;
}
