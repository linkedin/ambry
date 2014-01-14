package com.github.ambry.tools.admin;

import com.github.ambry.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class TestPrealloc {
  public static void main(String args[]) {
    try {
      // preallocate file
      File file = new File(args[0]);
      long capacity = Long.parseLong(args[1]);
      Utils.preAllocateFileIfNeeded(file, capacity);
      FileInputStream readOnlyStream = new FileInputStream(file);
      System.out.println("size from file input stream " + readOnlyStream.available());
      FileChannel channel = Utils.openChannel(file, true);
      System.out.println("size from channel " + channel.size());
      RandomAccessFile random = new RandomAccessFile(file, "rw");
      System.out.println("size from random access file " + random.length());
      System.out.println("size from file " + file.length());
      byte[] randombytes = new byte[4000];
      new Random().nextBytes(randombytes);
      channel.write(ByteBuffer.wrap(randombytes), 0);
      System.out.println("size from file input stream " + readOnlyStream.available());
      System.out.println("size from channel " + channel.size());
      System.out.println("size from random access file " + random.length());
      System.out.println("size from file " + file.length());
      readOnlyStream.close();
      channel.close();
      random.close();
    }
    catch (Exception e) {
      System.out.println("Error running tool " + e);
    }
  }
}
