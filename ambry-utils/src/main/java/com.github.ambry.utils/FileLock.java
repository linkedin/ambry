package com.github.ambry.utils;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

/**
 * File Lock helper
 */
public class FileLock {
  private final File file;
  private final FileChannel channel;
  private java.nio.channels.FileLock flock = null;

  FileLock(File file) throws FileNotFoundException, IOException{
    this.file = file;
    file.createNewFile();
    channel = new RandomAccessFile(file, "rw").getChannel();
  }

  /**
   * Lock the file or throw an exception if the lock is already held
   */
  public void lock() throws IOException {
    synchronized(this) {
      //trace("Acquiring lock on " + file.getAbsolutePath)
      flock = channel.lock();
    }
  }

  /**
   * Try to lock the file and return true if the locking succeeds
   */
  public boolean tryLock() throws IOException {
    synchronized(this) {
      // trace("Acquiring lock on " + file.getAbsolutePath)
      try {
        // weirdly this method will return null if the lock is held by another
        // process, but will throw an exception if the lock is held by this process
        // so we have to handle both cases
        flock = channel.tryLock();
        return flock != null;
      } catch (OverlappingFileLockException e) {
        return false;
      }
    }
  }

  /**
   * Unlock the lock if it is held
   */
   public void unlock() throws IOException {
     synchronized(this) {
       //trace("Releasing lock on " + file.getAbsolutePath)
       if(flock != null)
         flock.release();
     }
   }

   /**
    * Destroy this lock, closing the associated FileChannel
    */
   public void destroy() throws IOException {
     synchronized(this) {
       unlock();
       channel.close();
     }
   }
}

