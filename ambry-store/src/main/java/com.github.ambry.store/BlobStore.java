package com.github.ambry.store;

import com.github.ambry.utils.Scheduler;

import java.io.InputStream;
import java.util.ArrayList;
import java.io.IOException;

/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {

  private Log log;
  private BlobIndex index;

  public BlobStore(String dataDir, Scheduler scheduler) throws IOException, IndexCreationException {
    log = new Log(dataDir);
    index = new BlobIndex(dataDir, scheduler, log);
    // see the log end offset to the recovered offset from the index after initializing it
    log.setLogEndOffset(index.getCurrentEndOffset());
  }

  @Override
  public void start() throws StoreException {

  }

  @Override
  public MessageReadSet get(ArrayList<String> handles) throws StoreException {
    return null;
  }

  @Override
  public void put(String handle, InputStream value) throws StoreException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void delete(ArrayList<String> handles) throws StoreException {
    // add a new delete record
    // mark the delete flag for all the handles in the index
  }

  @Override
  public void shutdown() {
    try {
      index.close();
      log.close();
    }
    catch (Exception e) {
      // log here
    }
  }
}
