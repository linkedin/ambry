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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
   * <p />
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
  /**
   * The separator used to construct account-container pair in stats report.
   */
  public static final String ACCOUNT_CONTAINER_SEPARATOR = "___";
  private static final String CLIENT_RESET_EXCEPTION_MSG = "Connection reset by peer";
  private static final String CLIENT_BROKEN_PIPE_EXCEPTION_MSG = "Broken pipe";
  // This is found in Netty's SslHandler, which does not expose the exception message as a constant. Be careful, since
  // the message may change in the future.
  private static final String SSL_ENGINE_CLOSED_EXCEPTION_MSG = "SSLEngine closed already";
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
      throw new IllegalArgumentException("readShortString : the size " + size + " cannot be negative");
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
      throw new IllegalArgumentException("readShortString : the size of the input does not match the actual data size: " + read + " != " + size);
    }
    return new String(bytes, StandardCharsets.UTF_8);
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
   * Create a {@link ByteBufferInputStream} from the {@link CrcInputStream} by sharing the underlying memory if the
   * crcStream is built upon a {@link ByteBufferDataInputStream}.
   * @param crcStream The crcStream to read {@link ByteBuffer} out.
   * @param dataSize The size of {@link ByteBuffer}.
   * @return The {@link ByteBufferInputStream}
   * @throws IOException Unexpected IO errors.
   */
  public static ByteBufferInputStream getByteBufferInputStreamFromCrcInputStream(CrcInputStream crcStream, int dataSize)
      throws IOException {
    ByteBuffer buffer = readByteBufferFromCrcInputStream(crcStream, dataSize);
    return new ByteBufferInputStream(buffer);
  }

  /**
   * A helper method to return {@link ByteBuffer} from given {@link InputStream} at the given size.
   * @param stream The {@link InputStream} to read {@link ByteBuffer} out.
   * @param dataSize The size of {@link ByteBuffer}.
   * @return The {@link ByteBuffer}
   * @throws IOException Unexpected IO errors.
   */
  public static ByteBuffer getByteBufferFromInputStream(InputStream stream, int dataSize) throws IOException {
    ByteBuffer output = ByteBuffer.allocate(dataSize);
    int read = 0;
    ReadableByteChannel readableByteChannel = Channels.newChannel(stream);
    while (read < dataSize) {
      int sizeRead = readableByteChannel.read(output);
      if (sizeRead == 0 || sizeRead == -1) {
        throw new IOException("Total size read " + read + " is less than the size to be read " + dataSize);
      }
      read += sizeRead;
    }
    output.flip();
    return output;
  }

  /**
   * Transfer {@code dataSize} bytes of data from the given crc stream to a newly create {@link ByteBuffer}. The method
   * would also update the crc value in the crc stream.
   * @param crcStream The crc stream.
   * @param dataSize The number of bytes to transfer.
   * @return the newly created {@link ByteBuffer} which contains the transferred data.
   * @throws IOException Any I/O error.
   */
  public static ByteBuffer readByteBufferFromCrcInputStream(CrcInputStream crcStream, int dataSize) throws IOException {
    ByteBuffer output;
    InputStream inputStream = crcStream.getUnderlyingInputStream();
    if (inputStream instanceof NettyByteBufDataInputStream) {
      // getBuffer() doesn't increase the reference count on this ByteBuf.
      ByteBuf nettyByteBuf = ((NettyByteBufDataInputStream) inputStream).getBuffer();
      // construct a java.nio.ByteBuffer to create a ByteBufferInputStream
      int startIndex = nettyByteBuf.readerIndex();
      output = nettyByteBuf.nioBuffer(startIndex, dataSize);
      crcStream.updateCrc(output.duplicate());
      nettyByteBuf.readerIndex(startIndex + dataSize);
    } else {
      output = getByteBufferFromInputStream(crcStream, dataSize);
    }
    return output;
  }

  /**
   * Transfer {@code dataSize} bytes of data from the given crc stream to a newly create {@link ByteBuf}. The method
   * would also update the crc value in the crc stream.
   * @param crcStream The crc stream.
   * @param dataSize The number of bytes to transfer.
   * @return the newly created {@link ByteBuf} which contains the transferred data.
   * @throws IOException Any I/O error.
   */
  public static ByteBuf readNettyByteBufFromCrcInputStream(CrcInputStream crcStream, int dataSize) throws IOException {
    ByteBuf output;
    InputStream inputStream = crcStream.getUnderlyingInputStream();
    if (inputStream instanceof NettyByteBufDataInputStream) {
      ByteBuf nettyByteBuf = ((NettyByteBufDataInputStream) inputStream).getBuffer();
      // construct a java.nio.ByteBuffer to create a ByteBufferInputStream
      int startIndex = nettyByteBuf.readerIndex();
      output = nettyByteBuf.retainedSlice(startIndex, dataSize);
      crcStream.updateCrc(output.nioBuffer());
      nettyByteBuf.readerIndex(startIndex + dataSize);
    } else {
      ByteBuffer buffer = getByteBufferFromInputStream(crcStream, dataSize);
      output = Unpooled.wrappedBuffer(buffer);
    }
    return output;
  }

  /**
   * Extracts the cause of an {@link ExecutionException}. This is used to get the relevant domain-specific exception
   * after unboxing a future.
   * @param e the {@link Exception}
   * @return if the cause is {@code null}, return {@code e} itself. If the cause is not an instance
   *         of exception, return the {@link Throwable} wrapped in an exception. If not {@link ExecutionException},
   *         return the exception itself. Otherwise, return the cause {@link Exception}.
   */
  public static Exception extractExecutionExceptionCause(Exception e) {
    Throwable cause = e.getCause();
    if (!(e instanceof ExecutionException) || cause == null) {
      return e;
    }
    return cause instanceof Exception ? (Exception) cause : new Exception(cause);
  }

  /**
   * Represent an operation that accepts a {@link ByteBuffer} and returns another {@link ByteBuffer}. Side effect should
   * be expected to the input {@link ByteBuffer} and the returned {@link ByteBuffer} should be ready for read.
   * @param <T> The exception to throw in this operation.
   */
  @FunctionalInterface
  public interface ByteBufferFunction<T extends Throwable> {
    ByteBuffer apply(ByteBuffer buffer) throws T;
  }

  /**
   * Apply a {@link ByteBufferFunction} to a {@link ByteBuf} and return a {@link ByteBuf}. All the bytes in the input
   * {@link ByteBuf} will be consumed.
   * @param buf The input {@link ByteBuf}.
   * @param fn The {@link ByteBufferFunction}.
   * @param <T> The exception to throw in the {@code fn}.
   * @return A {@link ByteBuf}.
   * @throws T Exception thrown from {@code fn}.
   */
  public static <T extends Throwable> ByteBuf applyByteBufferFunctionToByteBuf(ByteBuf buf, ByteBufferFunction<T> fn)
      throws T {
    if (buf.nioBufferCount() == 1) {
      ByteBuffer buffer = buf.nioBuffer();
      buf.skipBytes(buffer.remaining());
      return Unpooled.wrappedBuffer(fn.apply(buffer));
    } else {
      ByteBuf temp = ByteBufAllocator.DEFAULT.heapBuffer(buf.readableBytes());
      try {
        temp.writeBytes(buf);
        ByteBuffer buffer = temp.nioBuffer();
        return Unpooled.wrappedBuffer(fn.apply(buffer));
      } finally {
        temp.release();
      }
    }
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
   * Read {@link FileChannel} from a given offset to fill up a {@link ByteBuffer}. Total bytes read should be buffer's
   * remaining size(limit - position).
   * @param fileChannel from which data to be read from
   * @param offset starting offset of the fileChanel to be read from
   * @param buffer the destination of the read
   * @return actual io count of fileChannel.read()
   * @throws IOException
   */
  public static int readFileToByteBuffer(FileChannel fileChannel, long offset, ByteBuffer buffer) throws IOException {
    int ioCount = 0;
    int read = 0;
    int expectedRead = buffer.remaining();
    while (buffer.hasRemaining()) {
      int sizeRead = fileChannel.read(buffer, offset);
      if (sizeRead == 0 || sizeRead == -1) {
        throw new IOException(
            "Total size read " + read + " is less than the size to be read " + expectedRead + ". sizeRead is "
                + sizeRead);
      }
      read += sizeRead;
      offset += sizeRead;
      ioCount++;
    }
    return ioCount;
  }

  /**
   * Instantiate a class instance from a given className.
   * @param className
   * @param <T>
   * @return
   * @throws ReflectiveOperationException
   */
  public static <T> T getObj(String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (T) Class.forName(className).newInstance();
  }

  /**
   * Instantiate a class instance from a given className with an arg
   * @param <T>
   * @param className
   * @param arg
   * @return
   * @throws ReflectiveOperationException
   */
  public static <T> T getObj(String className, Object arg) throws ReflectiveOperationException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 1 && checkAssignable(paramTypes[0], arg)) {
        return (T) ctor.newInstance(arg);
      }
    }
    throw buildNoConstructorException(className, arg);
  }

  /**
   * Instantiate a class instance from a given className with two args
   * @param <T>
   * @param className
   * @param arg1
   * @param arg2
   * @return
   * @throws ReflectiveOperationException
   */
  public static <T> T getObj(String className, Object arg1, Object arg2) throws ReflectiveOperationException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 2 && checkAssignable(paramTypes[0], arg1) && checkAssignable(paramTypes[1], arg2)) {
        return (T) ctor.newInstance(arg1, arg2);
      }
    }
    throw buildNoConstructorException(className, arg1, arg2);
  }

  /**
   * Instantiate a class instance from a given className with three args
   * @param className
   * @param arg1
   * @param arg2
   * @param arg3
   * @param <T>
   * @return
   * @throws ReflectiveOperationException
   */
  public static <T> T getObj(String className, Object arg1, Object arg2, Object arg3)
      throws ReflectiveOperationException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 3 && checkAssignable(paramTypes[0], arg1) && checkAssignable(paramTypes[1], arg2)
          && checkAssignable(paramTypes[2], arg3)) {
        return (T) ctor.newInstance(arg1, arg2, arg3);
      }
    }
    throw buildNoConstructorException(className, arg1, arg2, arg3);
  }

  /**
   * Instantiate a class instance from a given className with variable number of args
   * @param className
   * @param args
   * @param <T>
   * @return
   * @throws ReflectiveOperationException
   */
  public static <T> T getObj(String className, Object... args) throws ReflectiveOperationException {
    for (Constructor<?> ctor : Class.forName(className).getDeclaredConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == args.length) {
        int i = 0;
        for (; i < args.length; i++) {
          if (!checkAssignable(paramTypes[i], args[i])) {
            break;
          }
        }
        if (i == args.length) {
          return (T) ctor.newInstance(args);
        }
      }
    }
    throw buildNoConstructorException(className, args);
  }

  /**
   * Get a stream of the accessible {@link String} values of the static fields in the provided {@link Class}.
   * @param type the {@link Class} to get static field values from.
   * @return a {@link Stream} of the static field value strings.
   */
  public static Stream<String> getStaticFieldValuesAsStrings(Class type) {
    return Arrays.stream(type.getFields()).filter(field -> Modifier.isStatic(field.getModifiers())).map(field -> {
      try {
        return field.get(null).toString();
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Could not get value of a static field, " + field + ", in " + type, e);
      }
    });
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
   * Create a {@link NoSuchMethodException} for situations where a constructor for a class that conforms to the provided
   * argument types was not found.
   * @param className the class for which a suitable constructor was not found.
   * @param args the constructor arguments.
   * @return the {@link NoSuchMethodException} to throw.
   */
  private static NoSuchMethodException buildNoConstructorException(String className, Object... args) {
    String argTypes =
        Stream.of(args).map(arg -> arg == null ? "null" : arg.getClass().getName()).collect(Collectors.joining(", "));
    return new NoSuchMethodException(
        "No constructor found for " + className + " that takes arguments of types: " + argTypes);
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
  public static Properties loadProps(String filename) throws IOException {
    InputStream propStream = new FileInputStream(filename);
    Properties props = new Properties();
    props.load(propStream);
    return props;
  }

  /**
   * Serializes a nullable string into byte buffer using the default charset.
   *
   * @param outputBuffer The output buffer to serialize the value to
   * @param value The value to serialize
   */
  public static void serializeNullableString(ByteBuffer outputBuffer, String value) {
    if (value == null) {
      outputBuffer.putInt(0);
    } else {
      serializeString(outputBuffer, value, Charset.defaultCharset());
    }
  }

  /**
   * Serializes a string into byte buffer
   * @param outputBuffer The output buffer to serialize the value to
   * @param value The value to serialize
   * @param charset {@link Charset} to be used to encode
   */
  public static void serializeString(ByteBuffer outputBuffer, String value, Charset charset) {
    byte[] valueArray = value.getBytes(charset);
    outputBuffer.putInt(valueArray.length);
    outputBuffer.put(valueArray);
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
    Files.write(Paths.get(path), string.getBytes());
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
    return new String(Files.readAllBytes(Paths.get(path)));
  }

  /**
   * @return true if current operating system is Linux.
   */
  public static boolean isLinux() {
    return System.getProperty("os.name").toLowerCase().startsWith("linux");
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
    if (isLinux()) {
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
   * Read "size" length of bytes from a netty {@link ByteBuf} to a byte array starting at the given offset in the byte[]. If "size"
   * length of bytes can't be read because the end of the buffer has been reached, IOException is thrown. This method
   * blocks until input data is available, the end of the buffer is detected, or an exception is thrown.
   * @param buffer from which data to be read from
   * @param data byte[] into which the data has to be written
   * @param offset starting offset in the byte[] at which the data has to be written to
   * @param size length of bytes to be read from the stream
   * @return byte[] which has the data that is read from the buffer. Same as @param data
   * @throws IOException
   */
  public static byte[] readBytesFromByteBuf(ByteBuf buffer, byte[] data, int offset, int size) throws IOException {
    buffer.readBytes(data, offset, size);
    return data;
  }

  /**
   * Split the input string "data" using the delimiter and return as list of strings for the slices obtained.
   * This method will ignore empty segments. That is, a call like {@code splitString(",a1,,b2,c3,", ","}} will return
   * {@code ["a1","b2","c3]}. Since this is used for reading list-style configs, this is usually the desired behavior.
   * @param data the string to split.
   * @param delimiter the delimiter for splitting.
   * @return a mutable list of items.
   */
  public static ArrayList<String> splitString(String data, String delimiter) {
    return splitString(data, delimiter, ArrayList::new);
  }

  /**
   * Split the input string "data" using the delimiter and return as collection of strings for the slices obtained.
   * This method will ignore empty segments. That is, a call like {@code splitString(",a1,,b2,c3,", ","}} will return
   * {@code ["a1","b2","c3]}. Since this is used for reading list-style configs, this is usually the desired behavior.
   * @param data the string to split.
   * @param delimiter the delimiter for splitting.
   * @param collectionFactory functional interface to get the required collection.
   * @return a mutable collection of items.
   */
  public static <C extends Collection<String>> C splitString(String data, String delimiter,
      Supplier<C> collectionFactory) {
    if (data == null) {
      throw new IllegalArgumentException("Passed in string is null ");
    }
    return Arrays.stream(data.split(delimiter))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(collectionFactory));
  }

  /**
   * Partition the input list into a List of smaller sublists, each one limited to the specified batch size.
   * Method inspired by the Guava utility Lists.partition(List<T> list, int size).
   * @param inputList the input list to partition.
   * @param batchSize the maximum size of the returned sublists.
   * @return the partitioned list of sublists.
   */
  public static <T> List<List<T>> partitionList(List<T> inputList, int batchSize) {
    Objects.requireNonNull(inputList, "Input list cannot be null");
    if (batchSize < 1) {
      throw new IllegalArgumentException("Invalid batchSize: " + batchSize);
    }
    List<List<T>> partitionedLists = new ArrayList<>();
    for (int start = 0; start < inputList.size(); start += batchSize) {
      int end = Math.min(start + batchSize, inputList.size());
      partitionedLists.add(inputList.subList(start, end));
    }
    return partitionedLists;
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
      ByteBuffer newBuffer =
          existingBuffer.isDirect() ? ByteBuffer.allocateDirect(newLength) : ByteBuffer.allocate(newLength);
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
        || CLIENT_BROKEN_PIPE_EXCEPTION_MSG.equals(cause.getMessage()) || SSL_ENGINE_CLOSED_EXCEPTION_MSG.equals(
        cause.getMessage()));
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
   * Returns a TTL (in secs) given an expiry and creation time (in ms)
   * @param expiresAtMs the expiry time (in ms)
   * @param creationTimeMs the creation time (in ms)
   * @return the time to live (in secs)
   */
  public static long getTtlInSecsFromExpiryMs(long expiresAtMs, long creationTimeMs) {
    return expiresAtMs == Utils.Infinite_Time ? Utils.Infinite_Time
        : Math.max(0, TimeUnit.MILLISECONDS.toSeconds(expiresAtMs - creationTimeMs));
  }

  /**
   * Compare two times or durations, accounting for the {@link Utils#Infinite_Time} constant.
   * @param time1 the first time.
   * @param time2 the second time.
   * @return -1 if {@code time1} is earlier than {@code time2}, 0 if the times are equal, and 1 if {@code time1} is
   *         later than {@code time2}. {@link Utils#Infinite_Time} is considered greater than any other time.
   */
  public static int compareTimes(long time1, long time2) {
    if (time1 == time2) {
      return 0;
    } else if (time1 == Utils.Infinite_Time) {
      return 1;
    } else if (time2 == Utils.Infinite_Time) {
      return -1;
    } else {
      return Long.compare(time1, time2);
    }
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
   * Set permissions for given files.
   * @param files a list of files that set specified permissions
   * @param permissions a set of {@link PosixFilePermission} associated with given files
   * @throws IOException Any I/O error.
   */
  public static void setFilesPermission(List<File> files, Set<PosixFilePermission> permissions) throws IOException {
    for (File file : files) {
      Files.setPosixFilePermissions(file.toPath(), permissions);
    }
  }

  /**
   * @param str the {@link String} to examine
   * @return {@code true} if {@code str} is {@code null} or is an empty string.
   */
  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
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
