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
package com.github.ambry.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Random;


public class KeyGenerator {
  private static ByteBuffer randomKey(Random r) {
    byte[] bytes = new byte[48];
    r.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  static class RandomStringGenerator implements ResetableIterator<ByteBuffer> {
    int i, n, seed;
    Random random;

    RandomStringGenerator(int seed, int n) {
      i = 0;
      this.seed = seed;
      this.n = n;
      reset();
    }

    public int size() {
      return n;
    }

    public void reset() {
      random = new Random(seed);
    }

    public boolean hasNext() {
      return i < n;
    }

    public ByteBuffer next() {
      i++;
      return randomKey(random);
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  static class IntGenerator implements ResetableIterator<ByteBuffer> {
    private int i, start, n;

    IntGenerator(int n) {
      this(0, n);
    }

    IntGenerator(int start, int n) {
      this.start = start;
      this.n = n;
      reset();
    }

    public int size() {
      return n - start;
    }

    public void reset() {
      i = start;
    }

    public boolean hasNext() {
      return i < n;
    }

    public ByteBuffer next() {
      return ByteBuffer.wrap(Integer.toString(i++).getBytes());
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  static class WordGenerator implements ResetableIterator<ByteBuffer> {
    static int WORDS;

    static {
      try {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/usr/share/dict/words")));
        while (br.ready()) {
          br.readLine();
          WORDS++;
        }
      } catch (IOException e) {
        WORDS = 0;
      }
    }

    BufferedReader reader;
    private int modulo;
    private int skip;
    byte[] next;

    WordGenerator(int skip, int modulo) {
      this.skip = skip;
      this.modulo = modulo;
      reset();
    }

    public int size() {
      return (1 + WORDS - skip) / modulo;
    }

    public void reset() {
      try {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream("/usr/share/dict/words")));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < skip; i++) {
        try {
          reader.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      next();
    }

    public boolean hasNext() {
      return next != null;
    }

    public ByteBuffer next() {
      try {
        byte[] s = next;
        for (int i = 0; i < modulo; i++) {
          String line = reader.readLine();
          next = line == null ? null : line.getBytes();
        }
        return s == null ? null : ByteBuffer.wrap(s);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
