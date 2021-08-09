/*
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
package com.github.ambry.tools.perf;

import com.github.ambry.utils.Crc32;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;


public class Crc32Benchmark {
  public static void main(String[] args) {
    // Benchmark two crc32 implementation
    // 1. java.util.zip.CRC32
    // 2. com.github.ambry.util.Crc32
    // Benchmark on several ByteBuffer on different sizes and different memory
    // -----------------------
    // size  | heap | direct
    // -----------------------
    // 100
    // 1K
    // 4K
    // 16K
    // 64K
    // 256K
    // 1MB
    // 4MB
    // ------------------------

    final int[] BUFFER_SIZES =
        new int[]{100, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024};
    final Map<Integer, String> sizeLiterals = new HashMap<>();
    sizeLiterals.put(100, "100B");
    sizeLiterals.put(1024, "1K");
    sizeLiterals.put(4 * 1024, "4K");
    sizeLiterals.put(16 * 1024, "16K");
    sizeLiterals.put(64 * 1024, "64K");
    sizeLiterals.put(256 * 1024, "256K");
    sizeLiterals.put(1024 * 1024, "1M");
    sizeLiterals.put(4 * 1024 * 1024, "4M");
    // For each size,  create 10 ByteBuffers, and run crc32 500 times
    final int NUM_ITERATION = 500;
    final int NUM_BUFFERS = 10;
    final Random random = new Random();
    System.out.println("Time Unit: us");
    System.out.printf("size\t\theapJava\theapAmbry\tdirectJava\tdirectAmbry\tarrayJava\tarrayAmbry\n");
    for (int size : BUFFER_SIZES) {
      ByteBuffer[] buffers = new ByteBuffer[NUM_BUFFERS];
      long[] crcs = new long[NUM_BUFFERS];
      // first test on heap buffer
      byte[] arr = new byte[size];
      for (int i = 0; i < NUM_BUFFERS; i++) {
        random.nextBytes(arr);
        buffers[i] = ByteBuffer.allocate(size);
        buffers[i].put(arr);
        buffers[i].flip();
      }

      long start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        Crc32 test = new Crc32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double heapAmbry = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        CRC32 test = new CRC32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double heapJava = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      // then test on direct buffer
      for (int i = 0; i < NUM_BUFFERS; i++) {
        random.nextBytes(arr);
        buffers[i] = ByteBuffer.allocateDirect(size);
        buffers[i].put(arr);
        buffers[i].flip();
      }

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        Crc32 test = new Crc32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double directAmbry = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        CRC32 test = new CRC32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double directJava = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        Crc32 test = new Crc32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        buffer.get(arr);
        test.update(arr, 0, arr.length);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double arrayAmbry = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        CRC32 test = new CRC32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        buffer.get(arr);
        test.update(arr, 0, arr.length);
        crcs[iter % NUM_BUFFERS] = test.getValue();
        buffer.position(0);
      }
      double arrayJava = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;
      printArray(crcs);

      System.out.printf("%s\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f\n", sizeLiterals.get(size), heapJava,
          heapAmbry, directJava, directAmbry, arrayJava, arrayAmbry);
    }
  }

  private static void printArray(long[] array) {
    //System.out.println(Arrays.stream(array).boxed().map(String::valueOf).collect(Collectors.joining(",")));
  }
}
