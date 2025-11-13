/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.benchmarks;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares Apache Commons Base64 vs Java 8 Base64 across 1KB, 128KB, and 4MB blobs.
 * Tests encoding (bytes -> string) and decoding (string -> bytes) which are the primary operations.
 * Uses randomized data selection to prevent JVM constant folding and CPU cache optimizations.
 * Runtime: ~3 minutes with default config. Memory profiling enabled via GC profiler.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 2, time = 5)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Thread)
public class Base64Benchmark {

  @Param({"1024", "131072", "4194304"})
  private int blobSize;

  private static final int DATA_SAMPLES = 10;
  private byte[][] randomData;
  private String[] apacheEncodedData;
  private String[] java8EncodedData;

  private static final java.util.Base64.Encoder JAVA8_ENCODER =
      java.util.Base64.getUrlEncoder().withoutPadding();
  private static final java.util.Base64.Decoder JAVA8_DECODER =
      java.util.Base64.getUrlDecoder();

  @Setup(Level.Trial)
  public void setup() {
    randomData = new byte[DATA_SAMPLES][];
    apacheEncodedData = new String[DATA_SAMPLES];
    java8EncodedData = new String[DATA_SAMPLES];

    Random random = new Random(42);
    for (int i = 0; i < DATA_SAMPLES; i++) {
      randomData[i] = new byte[blobSize];
      random.nextBytes(randomData[i]);
      apacheEncodedData[i] = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(randomData[i]);
      java8EncodedData[i] = JAVA8_ENCODER.encodeToString(randomData[i]);
    }
  }

  @Benchmark
  public void apacheCommonsEncode(Blackhole blackhole) {
    int index = ThreadLocalRandom.current().nextInt(DATA_SAMPLES);
    blackhole.consume(org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(randomData[index]));
  }

  @Benchmark
  public void apacheCommonsDecode(Blackhole blackhole) {
    int index = ThreadLocalRandom.current().nextInt(DATA_SAMPLES);
    blackhole.consume(org.apache.commons.codec.binary.Base64.decodeBase64(apacheEncodedData[index]));
  }

  @Benchmark
  public void java8Encode(Blackhole blackhole) {
    int index = ThreadLocalRandom.current().nextInt(DATA_SAMPLES);
    blackhole.consume(JAVA8_ENCODER.encodeToString(randomData[index]));
  }

  @Benchmark
  public void java8Decode(Blackhole blackhole) {
    int index = ThreadLocalRandom.current().nextInt(DATA_SAMPLES);
    blackhole.consume(JAVA8_DECODER.decode(java8EncodedData[index]));
  }
}
