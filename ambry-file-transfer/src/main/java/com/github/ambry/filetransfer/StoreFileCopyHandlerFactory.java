package com.github.ambry.filetransfer;

public class StoreFileCopyHandlerFactory implements FileCopyHandlerFactory {

  public StoreFileCopyHandlerFactory() {

  }

  @Override
  public FileCopyHandler getFileCopyHandler() {
    return new StoreFileCopyHandler();
  }
}
