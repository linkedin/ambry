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
package com.github.ambry.tools.perf;

/**
 * Measures the file system performance using system cache and sync writes
 */
public class FileSystemWritePerformance {
  /* TODO: Remove dependency on Java 7
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("The number of args is not equal to 1. The path is required");
    }
    try {
      byte[] dataToWrite = new byte[1073741824];
      new Random().nextBytes(dataToWrite);
      File fileToWrite = new File(args[0], "Sample");
      WritableByteChannel fileChannelToWrite = Files.newByteChannel(fileToWrite.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

      long startTimeToWriteCache = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.write(ByteBuffer.wrap(dataToWrite));
      long endTimeToWriteCache = SystemTime.getInstance().nanoseconds() - startTimeToWriteCache;
      System.out.println("Time taken to write to cache in us for 1 " + endTimeToWriteCache * .001);
      long startTimeToClose = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.close();
      long endTimeToClose = SystemTime.getInstance().nanoseconds() - startTimeToClose;
      System.out.println("Time taken to close to cache in us for 1 " + endTimeToClose * .001);

      fileChannelToWrite = Files.newByteChannel(fileToWrite.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
      startTimeToWriteCache = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.write(ByteBuffer.wrap(dataToWrite));
      endTimeToWriteCache = SystemTime.getInstance().nanoseconds() - startTimeToWriteCache;
      System.out.println("Time taken to write to cache in us for 2 " + endTimeToWriteCache * .001);
      startTimeToClose = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.close();
      endTimeToClose = SystemTime.getInstance().nanoseconds() - startTimeToClose;
      System.out.println("Time taken to close to cache in us for 2 " + endTimeToClose * .001);


      fileToWrite = new File(args[0], "Sample1");
      fileChannelToWrite = Files.newByteChannel(fileToWrite.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);

      startTimeToWriteCache = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.write(ByteBuffer.wrap(dataToWrite));
      endTimeToWriteCache = SystemTime.getInstance().nanoseconds() - startTimeToWriteCache;
      System.out.println("Time taken to write to cache in us for 1 with sync " + endTimeToWriteCache * .001);
      startTimeToClose = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.close();
      endTimeToClose = SystemTime.getInstance().nanoseconds() - startTimeToClose;
      System.out.println("Time taken to close to cache in us for 1 with sync " + endTimeToClose * .001);

      fileChannelToWrite = Files.newByteChannel(fileToWrite.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
      startTimeToWriteCache = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.write(ByteBuffer.wrap(dataToWrite));
      endTimeToWriteCache = SystemTime.getInstance().nanoseconds() - startTimeToWriteCache;
      System.out.println("Time taken to write to cache in us for 2 with sync " + endTimeToWriteCache * .001);
      startTimeToClose = SystemTime.getInstance().nanoseconds();
      fileChannelToWrite.close();
      endTimeToClose = SystemTime.getInstance().nanoseconds() - startTimeToClose;
      System.out.println("Time taken to close to cache in us for 2 " + endTimeToClose * .001);
    }
    catch (IOException e) {
      System.out.println("Exception of writing to file " + e);
    }

  }
  */
}
