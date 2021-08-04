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
    System.out.printf("size\t\theapJava\theapAmbry\tdirectJava\tdirectAmbry\n");
    for (int size : BUFFER_SIZES) {
      ByteBuffer[] buffers = new ByteBuffer[NUM_BUFFERS];
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
        test.getValue();
        buffer.position(0);
      }
      double heapAmbry = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        CRC32 test = new CRC32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        test.getValue();
        buffer.position(0);
      }
      double heapJava = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;


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
        test.getValue();
        buffer.position(0);
      }
      double directAmbry = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;

      start = System.nanoTime();
      for (int iter = 0; iter < NUM_ITERATION; iter++) {
        CRC32 test = new CRC32();
        ByteBuffer buffer = buffers[iter % NUM_BUFFERS];
        assert buffer.remaining() == size;
        test.update(buffer);
        test.getValue();
        buffer.position(0);
      }
      double directJava = ((double) (System.nanoTime() - start)) / NUM_ITERATION / 1000;

      System.out.printf("%s\t\t%.2f\t%.2f\t%.2f\t%.2f\n", sizeLiterals.get(size), heapJava, heapAmbry, directJava,
          directAmbry);
    }
  }
}
