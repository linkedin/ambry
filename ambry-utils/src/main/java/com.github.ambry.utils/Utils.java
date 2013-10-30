package com.github.ambry.utils;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/14/13
 * Time: 9:23 AM
 * To change this template use File | Settings | File Templates.
 */


public class Utils {

  public static String readShortString(DataInputStream input) throws IOException {
    Short size = input.readShort();
    if(size < 0)
      return null;
    byte[] bytes = new byte[size];
    int read = input.read(bytes);
    if (read != size) {
      throw new IllegalArgumentException("the size inputstream does not match the actual data size");
    }
    return new String(bytes, "UTF-8");
  }

  public static ByteBuffer readIntBuffer(DataInputStream input) throws IOException {
    int size = input.readInt();
    if (size < 0)
      return null;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    int read = input.read(buffer.array());
    if (read != size) {
      throw new IllegalArgumentException("the size inputstream does not match the actual data size");
    }
    return buffer;
  }

  /**
   * Create a new thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  public static Thread newThread(Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
              //error("Uncaught exception in thread '" + t.getName + "':", e)
          }
        });
    return thread;
  }

  /**
   * Create a new thread
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  public static Thread newThread(String name, Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
              //error("Uncaught exception in thread '" + t.getName + "':", e)
          }
        });
    return thread;
  }

  /**
   * Create a daemon thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  public static Thread daemonThread(Runnable runnable) {
    return newThread(runnable, true);
  }

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  public static Thread daemonThread(String name, Runnable runnable) {
    return newThread(name, runnable, true);
  }

  /**
   * Open a channel for the given file
   */
  public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException {
    if(mutable)
      return new RandomAccessFile(file, "rw").getChannel();
    else
      return new FileInputStream(file).getChannel();
  }

  /**
   * Instantiate a class instance from a given className.
   */
   public static <T> T getObj(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return (T)Class.forName(className)
                .newInstance();
   }

  /**
   * Compute the hash code for the given items
   */
   public static int hashcode(Object[] items) {
     if(items == null)
       return 0;
     int h = 1;
     int i = 0;
     while(i < items.length) {
       if(items[i] != null) {
         h = 31 * h + items[i].hashCode();
         i += 1;
       }
     }
     return h;
   }

  /**
   * Compute the CRC32 of the byte array
   * @param bytes The array to compute the checksum for
   * @return The CRC32
   */
  public static long crc32(byte[] bytes) {
    return crc32(bytes, 0, bytes.length);
  }

  /**
   * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
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
   * @param filename The path of the file to read
   */
  public static Properties loadProps(String filename) throws FileNotFoundException, IOException {
    InputStream propStream = new FileInputStream(filename);
    Properties props = new Properties();
    props.load(propStream);
    return props;
  }
}
