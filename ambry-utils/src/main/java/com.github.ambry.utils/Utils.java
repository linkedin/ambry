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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A set of utility methods
 */
public class Utils {

  /**
   * Constant to define "infinite" time.
   * <p/>
   * Currently used in lieu of either an epoch based ms expiration time or a seconds based TTL (relative to creation
   * time).
   */
  public static final long Infinite_Time = -1;
  /**
   * The lowest possible port number.
   */
  public static final int MIN_PORT_NUM = 1;
  /**
   * The highest possible port number.
   */
  public static final int MAX_PORT_NUM = 65535;
  private static final String CLIENT_RESET_EXCEPTION_MSG = "Connection reset by peer";
  private static final String CLIENT_BROKEN_PIPE_EXCEPTION_MSG = "Broken pipe";
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  // The read*String methods assume that the underlying stream is blocking

  /**
   * Reads a String whose length is a short from the given input stream
   * @param input The input stream from which to read the String from
   * @return The String read from the stream
   * @throws IOException
   */
  public static String readShortString(DataInputStream input) throws IOException {
    Short size = input.readShort();
    if (size < 0) {
      throw new IllegalArgumentException("readShortString : the size cannot be negative");
    }
    byte[] bytes = new byte[size];
    int read = 0;
    while (read < size) {
      int readBytes = input.read(bytes, read, size - read);
      if (readBytes == -1 || readBytes == 0) {
        break;
      }
      read += readBytes;
    }
    if (read != size) {
      throw new IllegalArgumentException("readShortString : the size of the input does not match the actual data size");
    }
    return new String(bytes, "UTF-8");
  }

  /**
   * Gets the size of the string in serialized form
   * @param value the string of interest to be serialized
   * @return the size of the string in serialized form
   */
  public static int getIntStringLength(String value) {
    return value == null ? Integer.BYTES : Integer.BYTES + value.length();
  }

  /**
   * Reads a String whose length is an int from the given input stream
   * @param input The input stream from which to read the String from
   * @return The String read from the stream
   * @throws IOException
   */
  public static String readIntString(DataInputStream input) throws IOException {
    return readIntString(input, StandardCharsets.UTF_8);
  }

  /**
   * Reads a String whose length is an int from the given input stream
   * @param input The input stream from which to read the String from
   * @param charset the charset to use.
   * @return The String read from the stream
   * @throws IOException
   */
  public static String readIntString(DataInputStream input, Charset charset) throws IOException {
    int size = input.readInt();
    if (size < 0) {
      throw new IllegalArgumentException("readIntString : the size cannot be negative");
    }
    byte[] bytes = new byte[size];
    int read = 0;
    while (read < size) {
      int readBytes = input.read(bytes, read, size - read);
      if (readBytes == -1 || readBytes == 0) {
        break;
      }
      read += readBytes;
    }
    if (read != size) {
      throw new IllegalArgumentException("readIntString : the size of the input does not match the actual data size");
    }
    return new String(bytes, charset);
  }

  /**
   *
   * @param input
   * @return
   * @throws IOException
   */
  public static ByteBuffer readIntBuffer(DataInputStream input) throws IOException {
    int size = input.readInt();
    if (size < 0) {
      throw new IllegalArgumentException("readIntBuffer : the size cannot be negative");
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    int read = 0;
    while (read < size) {
      int readBytes = input.read(buffer.array());
      if (readBytes == -1 || readBytes == 0) {
        break;
      }
      read += readBytes;
    }
    if (read != size) {
      throw new IllegalArgumentException("readIntBuffer : the size of the input does not match the actual data size");
    }
    return buffer;
  }

  /**
   *
   * @param input
   * @return
   * @throws IOException
   */
  public static ByteBuffer readShortBuffer(DataInputStream input) throws IOException {
    short size = input.readShort();
    if (size < 0) {
      throw new IllegalArgumentException("readShortBuffer : the size cannot be negative");
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    int read = 0;
    while (read < size) {
      int readBytes = input.read(buffer.array());
      if (readBytes == -1 || readBytes == 0) {
        break;
      }
      read += readBytes;
    }
    if (read != size) {
      throw new IllegalArgumentException("readShortBuffer the size of the input does not match the actual data size");
    }
    return buffer;
  }

  /**
   * Create a new thread
   *
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  public static Thread newThread(Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler((t, e) -> {
      logger.error("Encountered throwable in {}", t, e);
    });
    return thread;
  }

  /**
   * Create a new thread
   *
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  public static Thread newThread(String name, Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler((t, e) -> {
      logger.error("Encountered throwable in {}", t, e);
    });
    return thread;
  }

  /**
   * Create a daemon thread
   *
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  public static Thread daemonThread(Runnable runnable) {
    return newThread(runnable, true);
  }

  /**
   * Create a daemon thread
   *
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  public static Thread daemonThread(String name, Runnable runnable) {
    return newThread(name, runnable, true);
  }

  /**
   * Create a {@link ScheduledExecutorService} with the given properties.
   * @param numThreads The number of threads in the scheduler's thread pool.
   * @param threadNamePrefix The prefix string for thread names in this thread pool.
   * @param isDaemon {@code true} if the threads in this scheduler's should be daemon threads.
   * @return A {@link ScheduledExecutorService}.
   */
  public static ScheduledExecutorService newScheduler(int numThreads, String threadNamePrefix, boolean isDaemon) {
    ScheduledThreadPoolExecutor scheduler =
        new ScheduledThreadPoolExecutor(numThreads, new SchedulerThreadFactory(threadNamePrefix, isDaemon));
    scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return scheduler;
  }

  /**
   * Create a {@link ScheduledExecutorService} with the given properties.
   * @param numThreads The number of threads in the scheduler's thread pool.
   * @param isDaemon {@code true} if the threads in this scheduler's should be daemon threads.
   * @return A {@link ScheduledExecutorService}.
   */
  public static ScheduledExecutorService newScheduler(int numThreads, boolean isDaemon) {
    return newScheduler(numThreads, "ambry-scheduler-", isDaemon);
  }

  /**
   * Open a channel for the given file
   * @param file
   * @param mutable
   * @return
   * @throws FileNotFoundException
   */
  public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException {
    if (mutable) {
      return new RandomAccessFile(file, "rw").getChannel();
    } else {
      return new FileInputStream(file).getChannel();
    }
  }

  /**
   * Instantiate a class instance from a given className.
   * @param className
   * @param <T>
   * @return
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static <T> T getObj(String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (T) Class.forName(className).newInstance();
  }

  /**
   * Instantiate a class instance from a given className with an arg
   * @param className
   * @param arg
   * @param <T>
   * @return
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   */
  public static <T> T getObj(String className, Object arg)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
             InvocationTargetException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 1 && checkAssignable(paramTypes[0], arg)) {
        return (T) ctor.newInstance(arg);
      }
    }
    return null;
  }

  /**
   * Instantiate a class instance from a given className with two args
   * @param className
   * @param arg1
   * @param arg2
   * @param <T>
   * @return
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   */
  public static <T> T getObj(String className, Object arg1, Object arg2)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
             InvocationTargetException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 2 && checkAssignable(paramTypes[0], arg1) && checkAssignable(paramTypes[1], arg2)) {
        return (T) ctor.newInstance(arg1, arg2);
      }
    }
    return null;
  }

  /**
   * Instantiate a class instance from a given className with three args
   * @param className
   * @param arg1
   * @param arg2
   * @param arg3
   * @param <T>
   * @return
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   */
  public static <T> T getObj(String className, Object arg1, Object arg2, Object arg3)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
             InvocationTargetException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 3 && checkAssignable(paramTypes[0], arg1) && checkAssignable(paramTypes[1], arg2)
          && checkAssignable(paramTypes[2], arg3)) {
        return (T) ctor.newInstance(arg1, arg2, arg3);
      }
    }
    return null;
  }

  /**
   * Instantiate a class instance from a given className with variable number of args
   * @param className
   * @param objects
   * @param <T>
   * @return
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   */
  public static <T> T getObj(String className, Object... objects)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
             InvocationTargetException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == objects.length) {
        int i = 0;
        for (; i < objects.length; i++) {
          if (!checkAssignable(paramTypes[i], objects[i])) {
            break;
          }
        }
        if (i == objects.length) {
          return (T) ctor.newInstance(objects);
        }
      }
    }
    return null;
  }

  /**
   * Check if the given constructor parameter type is assignable from the provided argument object.
   * @param parameterType the {@link Class} of the constructor parameter.
   * @param arg the argument to test.
   * @return {@code true} if it is assignable. Note: this will return true if {@code arg} is {@code null}.
   */
  private static boolean checkAssignable(Class<?> parameterType, Object arg) {
    return arg == null || parameterType.isAssignableFrom(arg.getClass());
  }

  /**
   * Compute the hash code for the given items
   * @param items
   * @return
   */
  public static int hashcode(Object[] items) {
    if (items == null) {
      return 0;
    }
    int h = 1;
    int i = 0;
    while (i < items.length) {
      if (items[i] != null) {
        h = 31 * h + items[i].hashCode();
        i += 1;
      }
    }
    return h;
  }

  /**
   * Compute the CRC32 of the byte array
   *
   * @param bytes The array to compute the checksum for
   * @return The CRC32
   */
  public static long crc32(byte[] bytes) {
    return crc32(bytes, 0, bytes.length);
  }

  /**
   * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
   *
   * @param bytes The bytes to checksum
   * @param offset the offset at which to begin checksumming
   * @param size the number of bytes to checksum
   * @return The CRC32
   */
  public static long crc32(byte[] bytes, int offset, int size) {
    Crc32 crc = new Crc32();
    crc.update(bytes, offset, size);
    return crc.getValue();
  }

  /**
   * Read a properties file from the given path
   *
   * @param filename The path of the file to read
   */
  public static Properties loadProps(String filename) throws FileNotFoundException, IOException {
    InputStream propStream = new FileInputStream(filename);
    Properties props = new Properties();
    props.load(propStream);
    return props;
  }

  /**
   * Serializes a nullable string into byte buffer
   *
   * @param outputBuffer The output buffer to serialize the value to
   * @param value The value to serialize
   */
  public static void serializeNullableString(ByteBuffer outputBuffer, String value) {
    if (value == null) {
      outputBuffer.putInt(0);
    } else {
      outputBuffer.putInt(value.length());
      outputBuffer.put(value.getBytes());
    }
  }

  /**
   * Serializes a string into byte buffer
   * @param outputBuffer The output buffer to serialize the value to
   * @param value The value to serialize
   * @param charset {@link Charset} to be used to encode
   */
  public static void serializeString(ByteBuffer outputBuffer, String value, Charset charset) {
    outputBuffer.putInt(value.length());
    outputBuffer.put(value.getBytes(charset));
  }

  /**
   * Deserializes a string from byte buffer
   * @param inputBuffer The input buffer to deserialize the value from
   * @param charset {@link Charset} to be used to decode
   * @return the deserialized string
   */
  public static String deserializeString(ByteBuffer inputBuffer, Charset charset) {
    int size = inputBuffer.getInt();
    byte[] value = new byte[size];
    inputBuffer.get(value);
    return new String(value, charset);
  }

  /**
   * Returns the length of a nullable string
   *
   * @param value The string whose length is needed
   * @return The length of the string. 0 if null.
   */
  public static int getNullableStringLength(String value) {
    return value == null ? 0 : value.length();
  }

  /**
   * Writes specified string to specified file path.
   *
   * @param string to write
   * @param path file path
   * @throws IOException
   */
  public static void writeStringToFile(String string, String path) throws IOException {
    FileWriter fileWriter = null;
    try {
      File file = new File(path);
      fileWriter = new FileWriter(file);
      fileWriter.write(string);
    } finally {
      if (fileWriter != null) {
        fileWriter.close();
      }
    }
  }

  /**
   * Pretty prints specified jsonObject to specified file path.
   *
   * @param jsonObject to pretty print
   * @param path file path
   * @throws IOException
   * @throws JSONException
   */
  public static void writeJsonObjectToFile(JSONObject jsonObject, String path) throws IOException, JSONException {
    writeStringToFile(jsonObject.toString(2), path);
  }

  /**
   * Pretty prints specified {@link JSONArray} to specified file path.
   *
   * @param jsonArray to pretty print
   * @param path file path
   * @throws IOException
   * @throws JSONException
   */
  public static void writeJsonArrayToFile(JSONArray jsonArray, String path) throws IOException, JSONException {
    writeStringToFile(jsonArray.toString(2), path);
  }

  /**
   * Reads entire contents of specified file as a string.
   *
   * @param path file path to read
   * @return string read from specified file
   * @throws IOException
   */
  public static String readStringFromFile(String path) throws IOException {
    File file = new File(path);
    byte[] encoded = new byte[(int) file.length()];
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(file));
      ds.readFully(encoded);
    } finally {
      if (ds != null) {
        ds.close();
      }
    }
    return Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
  }

  /**
   * Reads JSON object (in string format) from specified file.
   *
   * @param path file path to read
   * @return JSONObject read from specified file
   * @throws IOException
   * @throws JSONException
   */
  public static JSONObject readJsonFromFile(String path) throws IOException, JSONException {
    return new JSONObject(readStringFromFile(path));
  }

  /**
   * Ensures that a given File is present. The file is pre-allocated with a given capacity using fallocate on linux
   * @param file file path to create and allocate
   * @param capacityBytes the number of bytes to pre-allocate
   * @throws IOException
   */
  public static void preAllocateFileIfNeeded(File file, long capacityBytes) throws IOException {
    if (!file.exists()) {
      file.createNewFile();
    }
    if (System.getProperty("os.name").toLowerCase().startsWith("linux")) {
      Runtime runtime = Runtime.getRuntime();
      Process process = runtime.exec("fallocate --keep-size -l " + capacityBytes + " " + file.getAbsolutePath());
      try {
        process.waitFor();
      } catch (InterruptedException e) {
        // ignore the interruption and check the exit value to be sure
      }
      if (process.exitValue() != 0) {
        throw new IOException(
            "error while trying to preallocate file " + file.getAbsolutePath() + " exitvalue " + process.exitValue()
                + " error string " + new BufferedReader(new InputStreamReader(process.getErrorStream())).lines()
                .collect(Collectors.joining("/n")));
      }
    }
  }

  /**
   * Get a pseudo-random long uniformly between 0 and n-1. Stolen from {@link java.util.Random#nextInt()}.
   *
   * @param random random object used to generate the random number so that we generate
   *                     uniforml random between 0 and n-1
   * @param n the bound
   * @return a value select randomly from the range {@code [0..n)}.
   */
  public static long getRandomLong(Random random, long n) {
    if (n <= 0) {
      throw new IllegalArgumentException("Cannot generate random long in range [0,n) for n<=0.");
    }

    final int BITS_PER_LONG = 63;
    long bits, val;
    do {
      bits = random.nextLong() & (~(1L << BITS_PER_LONG));
      val = bits % n;
    } while (bits - val + (n - 1) < 0L);
    return val;
  }

  /**
   * Returns a random short using the {@code Random} passed as arg
   * @param random the {@link Random} object that needs to be used to generate the random short
   * @return a random short in the range [0, Short.MAX_VALUE].
   */
  public static short getRandomShort(Random random) {
    return (short) random.nextInt(Short.MAX_VALUE + 1);
  }

  /**
   * Adds some number of seconds to an epoch time in ms.
   *
   * @param epochTimeInMs Epoch time in ms to which {@code deltaTimeInSeconds} needs to be added
   * @param deltaTimeInSeconds delta time in seconds which needs to be added to {@code epochTimeInMs}
   * @return epoch time in milliseconds after adding {@code deltaTimeInSeconds} to {@code epochTimeInMs} or
   * {@link Utils#Infinite_Time} if either of them is {@link Utils#Infinite_Time}
   */
  public static long addSecondsToEpochTime(long epochTimeInMs, long deltaTimeInSeconds) {
    if (deltaTimeInSeconds == Infinite_Time || epochTimeInMs == Infinite_Time) {
      return Infinite_Time;
    }
    return epochTimeInMs + (TimeUnit.SECONDS.toMillis(deltaTimeInSeconds));
  }

  /**
   * Read "size" length of bytes from stream to a byte array. If "size" length of bytes can't be read because the end of
   * the stream has been reached, IOException is thrown. This method blocks until input data is available, the end of
   * the stream is detected, or an exception is thrown.
   * @param stream from which data to be read from
   * @param size max length of bytes to be read from the stream.
   * @return byte[] which has the data that is read from the stream
   * @throws IOException
   */
  public static byte[] readBytesFromStream(InputStream stream, int size) throws IOException {
    return readBytesFromStream(stream, new byte[size], 0, size);
  }

  /**
   * Read "size" length of bytes from stream to a byte array starting at the given offset in the byte[]. If "size"
   * length of bytes can't be read because the end of the stream has been reached, IOException is thrown. This method
   * blocks until input data is available, the end of the stream is detected, or an exception is thrown.
   * @param stream from which data to be read from
   * @param data byte[] into which the data has to be written
   * @param offset starting offset in the byte[] at which the data has to be written to
   * @param size length of bytes to be read from the stream
   * @return byte[] which has the data that is read from the stream. Same as @param data
   * @throws IOException
   */
  public static byte[] readBytesFromStream(InputStream stream, byte[] data, int offset, int size) throws IOException {
    int read = 0;
    while (read < size) {
      int sizeRead = stream.read(data, offset, size - read);
      if (sizeRead == 0 || sizeRead == -1) {
        throw new IOException("Total size read " + read + " is less than the size to be read " + size);
      }
      read += sizeRead;
      offset += sizeRead;
    }
    return data;
  }

  /**
   * Split the input string "data" using the delimiter and return as list of strings for the slices obtained
   * @param data
   * @param delimiter
   * @return
   */
  public static ArrayList<String> splitString(String data, String delimiter) {
    if (data == null) {
      throw new IllegalArgumentException("Passed in string is null ");
    }
    ArrayList<String> toReturn = new ArrayList<String>();
    String[] slices = data.split(delimiter);
    toReturn.addAll(Arrays.asList(slices));
    return toReturn;
  }

  /**
   * Merge/Concatenate the input list of strings using the delimiter and return the new string
   * @param data List of strings to be merged/concatenated
   * @param delimiter using which the list of strings need to be merged/concatenated
   * @return the obtained string after merging/concatenating
   */
  public static String concatenateString(ArrayList<String> data, String delimiter) {
    if (data == null) {
      throw new IllegalArgumentException("Passed in List is null ");
    }
    StringBuilder sb = new StringBuilder();
    if (data.size() > 1) {
      for (int i = 0; i < data.size() - 1; i++) {
        sb.append(data.get(i)).append(delimiter);
      }
      sb.append(data.get(data.size() - 1));
    }
    return sb.toString();
  }

  /**
   * Make sure that the ByteBuffer capacity is equal to or greater than the expected length.
   * If not, create a new ByteBuffer of expected length and copy contents from previous ByteBuffer to the new one
   * @param existingBuffer ByteBuffer capacity to check
   * @param newLength new length for the ByteBuffer.
   * returns ByteBuffer with a minimum capacity of new length
   */
  public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
    if (newLength > existingBuffer.capacity()) {
      ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
      existingBuffer.flip();
      newBuffer.put(existingBuffer);
      return newBuffer;
    }
    return existingBuffer;
  }

  /**
   * Gets the root cause for {@code t}.
   * @param t the {@link Throwable} whose root cause is required.
   * @return the root cause for {@code t}.
   */
  public static Throwable getRootCause(Throwable t) {
    Throwable throwable = t;
    while (throwable != null && throwable.getCause() != null) {
      throwable = throwable.getCause();
    }
    return throwable;
  }

  /**
   * Convert ms to nearest second(floor) and back to ms to get the approx value in ms if not for
   * {@link Utils#Infinite_Time}.
   * @param timeInMs the time in ms that needs to be converted
   * @return the time in ms to the nearest second(floored) for the given time in ms
   */
  public static long getTimeInMsToTheNearestSec(long timeInMs) {
    long timeInSecs = timeInMs / Time.MsPerSec;
    return timeInMs != Utils.Infinite_Time ? (timeInSecs * Time.MsPerSec) : Utils.Infinite_Time;
  }

  /**
   * Shuts down an {@link ExecutorService} gracefully in a given time. If the {@link ExecutorService} cannot be shut
   * down within the given time, it will be forced to shut down immediately. In the worst case, it will try to shut down
   * the {@link ExecutorService} within 2 * shutdownTimeout, separated by a forced shutdown.
   * @param executorService The {@link ExecutorService} to shut down.
   * @param shutdownTimeout The timeout in ms to shutdown the {@link ExecutorService}.
   * @param timeUnit TimeUnit for shutdownTimeout.
   */
  public static void shutDownExecutorService(ExecutorService executorService, long shutdownTimeout, TimeUnit timeUnit) {
    // Disable new tasks from being submitted
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(shutdownTimeout, timeUnit)) {
        logger.warn("ExecutorService is not shut down successfully within {} {}, forcing an immediate shutdown.",
            shutdownTimeout, timeUnit);
        executorService.shutdownNow();
        if (!executorService.awaitTermination(shutdownTimeout, timeUnit)) {
          logger.error("ExecutorService cannot be shut down successfully.");
        }
      }
    } catch (InterruptedException e) {
      logger.error("Exception occurred when shutting down the ExecutorService. Forcing an immediate shutdown.", e);
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error("Exception occurred when shutting down the ExecutorService.", e);
    }
  }

  /**
   * @param cause the problem cause.
   * @return {@code true} this cause indicates a possible early termination from the client. {@code false} otherwise.
   */
  public static boolean isPossibleClientTermination(Throwable cause) {
    return cause instanceof IOException && (CLIENT_RESET_EXCEPTION_MSG.equals(cause.getMessage())
        || CLIENT_BROKEN_PIPE_EXCEPTION_MSG.equals(cause.getMessage()));
  }

  /**
   * Wraps the given {@code cause} such that {@link #isPossibleClientTermination(Throwable)} recognizes it.
   * @param cause the {@link Throwable} to include.
   * @return wrapped {@code cause} such that {@link #isPossibleClientTermination(Throwable)} recognizes it.
   */
  public static IOException convertToClientTerminationException(Throwable cause) {
    return new IOException(CLIENT_RESET_EXCEPTION_MSG, cause);
  }

  /**
   * Delete a directory recursively or delete a single file.
   * @param file the file or directory to delete.
   * @throws IOException if all files could not be deleted.
   */
  public static void deleteFileOrDirectory(File file) throws IOException {
    if (file.exists()) {
      Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  /**
   * A thread factory to use for {@link ScheduledExecutorService}s instantiated using
   * {@link #newScheduler(int, String, boolean)}.
   */
  private static class SchedulerThreadFactory implements ThreadFactory {
    private final AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private final String threadNamePrefix;
    private final boolean isDaemon;

    /**
     * Create a {@link SchedulerThreadFactory}
     * @param threadNamePrefix the prefix string for threads in this scheduler's thread pool.
     * @param isDaemon {@code true} if the created threads should be daemon threads.
     */
    SchedulerThreadFactory(String threadNamePrefix, boolean isDaemon) {
      this.threadNamePrefix = threadNamePrefix;
      this.isDaemon = isDaemon;
    }

    @Override
    public Thread newThread(Runnable r) {
      return Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, isDaemon);
    }
  }
}
